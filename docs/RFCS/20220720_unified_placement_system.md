- Feature Name: allocation unification
- Status: in-progress
- Start Date: 2022-07-20
- Authors: austen mcclernon
- RFC PR: TODO(kvoli)
- Cockroach Issue: TODO(kvoli)

# Summary

This RFC outlines a proposal to establish a unified placement system. The
purpose of this proposal is to enforce clear boundaries between the
constituents of the placement system, to enable wholesale modification of
separate components in an isolated and therefore lower risk manner. A proposal
for a new or modified algorithm is **not** includes, rather the motivation for,
and a design of loosely coupled placement system is outlined. 

The components outlined in this proposal already exist in one or several
places, both explicitly and implicitly. These components are: (1) a stateless
planner, which is responsible for returning one or more changes that should be
taken, given an input of the cluster state and a list of ranges which may have
changes applied to them; (2) a stateful controller, which consults the planner
and oversees the cadence, concurrency and application of changes; (3) stateful
agent(s), which are responsible for applying changes to the cluster state; (4)
the state, which encompasses pending changes, information about stores, nodes
and ranges within the cluster; and (4) the user configuration, which specifies
constraints, goals and locality information.

The proposal is split into semantics, which abstractly define the system
interface with some mention to the existing system and the implementation,
which details the technical design with respect to the existing system.

# Motivation

The current placement system within CockroachDB is built from two primary
components, the store rebalancer and replicate queue. These components are
unaware of one another's actions, independently and maintain a fixed
concurrency of 2. The store rebalancer operates over every range the store it
is on holds a lease for, by aggregation of load at a cadence of 1 minute.
Whilst the replicate queue considers single ranges, sequentially at a cadence
of 10 minutes.

This independence and unclear split of responsibilities and goals presents a
challenge when changing either component, as whilst their decision-making is
independent, the effects of their changes are not. This interaction limits the
velocity and scope of code changes that may be explored, as there is no one
location where the "algorithm" which decides placement lives; rather, it is
disparate and spread over multiple processes.

The implicit cadence, concurrency and scope of action consideration for both
components must also be addressed. These decisions impact the result of the
system, however are not easily configurable nor general.

These challenges motivate first principles approach to defining new
components which will constitute the placement system. Whereas previously
these roles were performed by more than one section of code, this proposal
outlines a unification which establishes disjoint responsibilities and exact
relationships between components.


## Definitions


### Existing Terms

**allocator:** is a set of methods which recommends an action and provides changes given an action for a single range.
**action:** is an abstract recommendation, which specifies a change should be made on a single range.
**change:** is a concrete modification, which alters a range.
**target:** is the store which a change specifies.
**range:** is a contiguous chunk of the keyspace.
**replica:** is a copy of the data contained within a range.
**leaseholder:** is the unique replica, which holds the lease for the range.
**transfer:** is a change which removes the lease from the existing leaseholder replica and adds it to another replica
**rebalance:** is a change which removes an existing replica and adds a new replica for a single range.
**store rebalancer:** is a continually running loop which searches for transfers and replica rebalances, consulting the allocator, for the goal of converging load distribution within the cluster.
**replicate queue:** is a priority queue which acts upon the highest priority replica sequentially, consulting the allocator for an action, changes for that action and then applying these changes to the range for this replica.

## New Terms (or redefined)

**planner:** 
**controller:**
**agent:** 
**state:**
**action:**
**change:**


## Background

The reconfiguration of data and responsibilities within cockroach at a range
level is performed by two processes; the replicate queue and the store
rebalancer. These processes act independently, however both consult the
reconfiguration algorithm, the allocator in order to determine what change to
apply for a specific range. They likewise share the same view of stores within
the cluster, using the storepool, which is used as an input for the
reconfiguration algorithm.


### Allocator

The *allocator* refers to set of methods in the `kv/kvserver/allocator`
package. These methods are stateless [REF], considering a single range and the
state of the stores in the cluster as input, returning some output without
modifying the cluster. Specifically, the output is a type of action or
immutable changes that constitutes an action, without applying the action
itself.

At a high level the allocator operates on a single range at time. It provides a
recommendation for an action that may be taken on the range when asked and
separately, given that action, provides a list of changes that would apply that
action.

The two types of calls to the allocator are distinct. (1) Compute an action for
a range [code](https://github.com/cockroachdb/cockroach/blob/1c905194203cb41b9f50b3544080369160320182/pkg/kv/kvserver/allocator/allocatorimpl/allocator.go#L151-L180),
given a specific range and state of the cluster as viewed by the allocator. (2)
Return the changes for a specified action to be taken upon a range.

For example, consider a cluster (s1,s2,s3,s4,s5), where a range r1 (s1,s2,s3) has a
replication factor of 3. We call `ComputeAction` [code](https://github.com/cockroachdb/cockroach/blob/1c905194203cb41b9f50b3544080369160320182/pkg/kv/kvserver/allocator/allocatorimpl/allocator.go#L567-L567), which returns `ConsiderRebalance`. Following this action recommendation, we call `RebalanceTarget` [code](https://github.com/cockroachdb/cockroach/blob/1c905194203cb41b9f50b3544080369160320182/pkg/kv/kvserver/allocator/allocatorimpl/allocator.go#L1187-L1187) using r1. This returns an add an remove target `add s4, remove s3`. 

There is no requirement that both (1) asking for an action to take or (2) the
specifics of the action are called in sequence (1)->(2) or that are called
together.

### State

#### Store Pool

The *store pool* contains a local store's view of other and it's own store
state. This state is populated via gossip [code](https://github.com/cockroachdb/cockroach/blob/9e44f568647bc06f2e0e33102e09df467f4b8f9a/pkg/kv/kvserver/allocator/storepool/store_pool.go#L405-L405).
Due to propogation delays and the interval at new information is gossiped, a
store could have up to a 10 second stale view of another store [code](https://github.com/cockroachdb/cockroach/blob/9e44f568647bc06f2e0e33102e09df467f4b8f9a/pkg/gossip/gossip.go#L128-L128). 

The store pool's state consists of store descriptors. These descriptors contain
the information pertaining to store usage, such as the number of ranges,
leases, disk usage and queries per second.

The store pool may be mutated locally, when an agent has applied a successful
change (e.g. transfer a lease from itself, to another store: inc other, dec
self lease count). This local mutation prevents the same action being repeated
and exists until a new store descriptor is received via gossip.

The store pool is primarily consulted for selecting a target for an action.
This includes, which store to remove a replica from, which store to add a
replica to, which store to transfer a lease to; for a single range.

The target selection is based at the allocator's discretion, where the
storepool is an input into this decision.

#### Hot Replicas

The *hot replicas* tracks the most frequently requested replicas. It maintains a
collection, ordered in most frequently accessed to lease frequently accessed
over the last 25-30 minutes. Which is then used to inform an estimate of the
impact of applying a change, to a range, upon the affected stores. Currently,
*hot replicas* tracks replicas w.r.t. QPS.


### Store Rebalancer

**goal:** converge the QPS of stores in a cluster.
**uses:** Allocator, Store Pool
**cadence:** searches for and continually applies changes, until there are none left, every 60 seconds.
**concurrency:** 1 change at a time
**input:** hot replicas, store pool
**output:** none
**effects:** lease transfer, move (add then remove) replicas

The *store rebalancer* acts as an independent agent, which considers the
hottest ranges it contains a lease for (in terms of QPS). It operates on every
range, which has a leaseholder replica on a store. Where the is one store
rebalancer per store.

Every replica is considered, however only the *hottest* ranges will be
candidates for changes. The store rebalancer operates in two phases, at a
frequency of once per minute. The first phase (1), continues until there are no
valid lease transfer targets any range it maintains a lease for or the QPS of
the current store is not considered overfull, in comparison to the other stores
in the cluster. The second phase (2), searches for a range to perform replica
changes (moves) continues until there are no valid rebalance changes, or until
the current store is not considered overfull, in comparison to other stores in
the cluster.

### Replicate Queue 

**goal:** repair ranges that require changes (up replication, decommission), converge the range and lease counts of stores within the cluster.
**uses:** Allocator, Store Pool
**cadence:** continually processes enqueued ranges, in order of priority. Sleeps when none exist. (The cadence of enqueued ranges are controlled by the replica scanner).
**concurrency:** 1 change at a time
**input:** store pool, list of ranges to check for repair/rebalance
**output:** none
**effects:** lease transfer, add replica, remove replica

### Replica Scanner

The *replica scanner* checks whether a replica should be enqueued into the
split, merge or replicate queue, if so it enqueues it. Every replica has this
check performed at a frequency of once per 10 minutes. The order of replicas to
check is purposefully random.

# Technical design

The technical design is split into semantics, the outline of each component and
their interaction; then implementation, specifying how the current system may
be iteratively adapted to fit the specification and an example of
implementation of another system, to demonstrate flexibility. 

![placement system diagram](images/unified_placement_system.png)

## Semantics

![controller interaction diagram](images/controller_interaction_diagram.png)

### Planner

The planner is responsible for deciding on the placement of replicas and leases
for ranges. It encapsulates the current allocators responsibilities, in
generating an action and changes for that action. However, rather than an
interface with many methods which may be used adhoc for different types of
action, instead the planner has only one, `Plan`.

```go
type Planner interface {
    Plan(State, PendingChanges, Configuration, []Range) Path
}
```

The state encompasses the inputs to the planning algorithm, which must then
decide given the set of ranges that may have changes applied, a collection
changes to be made, in the Path. In contrast to the allocator, which returns an
action or change for a single range, the Planner will return a one or more
ordered action-change pairs, for a subset of Ranges given. Currently, this
interaction already exists, albeit implicitly in both the replicate queue and
store rebalancer. Where the replicate queue, in two phases requests an action,
then changes for that action on a single range. The store rebalancer considers
every range it has a leaseholder replica for, however the actions possible are
constrained to lease or replica rebalancing. Therefore, the Planner could
simulate the behavior of both the rq and sr.

The output of `Plan` is a `Path`, which is a (partially) ordered set of
action-change pairs, where the action abstractly describes the change and the
change itself describes the concrete steps necessary to apply the action.

Placement is temporal, where the state is may change over time without the
effects of the placement system. In the current system, only one action ahead
is planned at any time, where that action may contain multiple changes for a
single range. The justification for returning multiple, ordered changes over
possibly many ranges rather than a single change is (1) extensibility and (2) testing.

1. Implementations which plan a single step ahead at a time are a subset of an
   implementation which plans multiple steps. Therefore, in specifying a
   contract which allows multistep, ordered changes it enables both single and
   multistep implementations. An example of where a multistep plan is
   superior over a single step is when locally optimal steps never arrive at a
   global goal i.e. you apply a less optimal change in order to reap a larger
   benefit in a future step. This is purposefully abstract, as this proposal
   does not outline any implementation.

2. Outputting multiple steps allows asserting on the behavior of any algorithm
   more easily and aids in observability. Where a policy may exist to only ever
   take the first action before re-planning, a benefit remains in that a much
   greater amount portion of the algorithm may be tested up front and in one
   function call.


#### Path

TODO(kvoli): Path specification, explicit concurrency and priority with a DAG

The path output of the planner is a directed acylic graph of action-change
pairs. An example is shown in the diagram below, with unit-less usage.

The path starts with a `begin` change-action pair and ends with a `end` pair.
The in-between vertices, constitute changes to be applied to the cluster. Each
change-action pair has a priority. The user of the plan may decide between
multiple changes to execute according to the priority, given the vertex is
reachable (a path containing only applied actions exists between it and the
begin vertex). This implies that concurrency is bounded above by the width of the dag
at each depth (if assuming changes all take equal time). The path does not
specify a minimum concurrency at which the changes should be executed, so it is
bounded below by zero.


![path diagram](images/planner_path_output.png)


### Controller

The controller is responsible for coordination the placement system. Where
previously this was done independently as part of the store rebalancer and
replicate queue. The coordination responsibility comprises: (1) Pacing; (2)
Requesting plans; (3) Scheduling the application of plans, including
concurrency; and (4) maintaining the pending state. In the current system, all
four responsibilities are conducted separately by the sr and rq, with varying
explicitness.

In the simplest form, the controller constitutes a single threaded infinite
loop that requests, then applies a plan.

```go
type Controller struct {
    Planner
    AgentPool
    State
    PendingChanges
    Configuration
}
```


1. Pacing the request and application of plans dictates the frequency at which
   new information is considered and acted upon. Within the sr this is 1 minute
   and rq 10 minutes. This pacing implicitly limits the rate of changes that
   may be applied, however in the proposed system this is not the case and
   separate.
2. Requesting plans, requires access to the state, pending changes and
   configuration. In explicitly delegating only one part of the system to act
   as a client for the planner, it simplifies the interaction between these
   components.
3. The controller itself does not apply the specific steps of the plan. Rather,
   it schedules one or more agents to apply the specific changes such as lease
   transfers and snapshots. This implies changes may have arbitrary
   concurrency, where the partially ordered plan returned could specify
   multiple actions with no dependency, that may be applied in parallel.
4. The pending state represents changes which have been scheduled however not
   yet completed. This includes rebalance and recovery snapshots as well as
   transfers. The effect of these scheduled changes must be accounted for in
   two ways. First, the estimated impact of the change should be applied as a
   delta to the local state, to avoid generating identical plans until new
   information arrives. Second, to avoid conflicting concurrent actions, it is
   necessary to be aware of pending changes in the case where their application
   is asynchronous w.r.t planning.


### Agent

The agent applies a change and is otherwise oblivious to the reason for the
change or implications. In the existing system the replicate queue and store
rebalancer act as their own single agent. 


```go
type Agent interface {
    Apply(Change)
}
```


### State

The state encapsulates the current information of the cluster. Currently, this
is the storepool, replica stats for each range and additionally the hot replica
accumulator in the store rebalancer. This proposal abstractly encompasses all of
these components in a single monolith, `State`. The state is used for deciding
on which changes to make in the planner. More concretely, an example is the
resource usage on each store and the usage of ranges that are given to the
planner; the planner could decide to change the stores holding replicas for a
range in order to balance estimated store resource usage.


```go
type State struct {
    StorePool
    ...
}
```

## Implementation

The goal is to fit the current placement system behavior into the above set of
interfaces. The most significant fit is unification of the separate components
which act as agents, controllers and planners independently. These components
are the store rebalancer, replicate queue, split queue (and load based
splitter) and merge queue.

### Retrofit


#### Phase I: Store Rebalancer and Replicate Queue

The first phase is the largest and reaps the most benefit, in unifying the
replicate queue and store rebalancer.

The control flow of the two separate components is currently:

**Store Rebalancer**

```python
def store_rebalancer():
   while True:
      start = now()
      rebalance_store(...)
      sleep(60 - now() + start)

def rebalance_store(hottest_replicas, overfull_threshold=1.10, store_avg_qps, store_local_qps):
   while store_local_qps > store_avg_qps * overfull_threshold:
      transfer = find_lease_transfer(hottest_replicas)
      if !transfer:
         break
      apply(transfer)

   while store_local_qps > store_avg_qps * overfull_threshold:
      replica_rebalance, transfer = find_replica_rebalance(hottest_replicas)
      if !replica_rebalance:
         break
      apply(replica_rebalance)
      apply(transfer)
```

**Replicate Queue**
```python
def replicate_queue():
   queue = heap()

   def scan_replicas():
      for True:
         replicas = shuffle(replica_list_snapshot())
         remaining = len(replicas)
         start = now()
         for replica in replicas:
            action, priority = compute_action(replica)
            if action:
               queue.enqueue(replica, action, priority)
            remaining -= 1
            sleep((10m/remaining) - (now() - start))

   async scan_replicas()

   for True:
      replica, _, priority = queue.pop()
      action = compute_action(replica)
      if !action:
         continue
      changes = action(replica)
      apply(changes)
```

Fitted to the proposed model, with result parity:

```python
def control():
   pending_changes = []
   for True:
      ranges = filter_leaseholders(replica_list_snapshot())
      path = plan(state(), pending_changes, ranges, config())
      if !path:
         sleep(1s)
         continue
      walk(path, apply)
   
def plan(state, pending_changes, ranges, config):
   # algorithm implementation, given the state
   under_replicated = filter_underreplicated(ranges)
   dead = filter_dead(ranges)
   decommissioning = filter_decommissioning(ranges)
   over_replicated = filter_overreplicated(ranges)

   rebalance := find_replica_rebalance(state.hottest_replicas)
   transfer := find_lease_transfer(state.hottest_replicas)

   replicate_queue_plan = plan(changes_for(under_replicated, dead, decommissioning, over_replicated))
   store_rebalancer_plan = plan(changes_for(rebalance,transfer))
   
def apply():
   switch (action):
      transfer:
         admin_transfer_lease(change)
      rebalance, add, remove:
         admin_change_replicas(change)
```

#### Phase II: Range Split

#### Phase III: Range Merge

### Example: Centralized Allocator

