- Feature Name: multi-metric_allocation
- Status: draft
- Start Date: 2023-06-15
- Authors: kvoli sumeerbhola
- RFC PR:
- Cockroach Issue:

# Summary

Multi-metric allocation (MMA) is range assignment which balances load across multiple
metric goals.

One paragraph explanation of the proposed change.

Suggested contents:
- What is being proposed
- Why (short reason)
- How (short plan)
- Impact

# Motivation


Resource saturation leads to degradation of service. The current allocation
system is not able to stop resource saturation on a fraction of nodes when
there's available capacity overall in the cluster. Whilst the current
allocation system does consider multiple metrics, it does so independently and
in a manner which decisions are made independently for each metric e.g. CPU /
QPS vs Lease Count. 

Why are we doing this? What use cases does it support? What is the expected outcome?

Is there a PM in this product area already? Does the PM know of user
stories that relate to the proposed work? Can we list these user
stories here? (Specific customer names need not be included, for
confidentiality, but it is still useful to describe their use cases.)

# Technical design

Logical Inputs:

Load

All inputs are rated (delta/duration). The timescale TBD. Tentatively, all load
inputs are rated over 30mins - although this is naive.

The primary inputs are measures of resource utilization. For inputs which
specify utlization, both use and capacity are also available.


- Node: CPU utilization 
- Store: CPU utilization, disk bandwidth utilization, disk storage utilization
- Range: CPU used, logical bandwidth

There may be arbitrary seconary inputs. The currently considered ones


- Store: lease count
- Range: leaseholder

Constraints

Allocation changes should never be suggested which violate a constraint.
Constraints are requirements specified by the user or another system's
invariant which the allocation system depends or is depended on. The constraint
input is a span config.

Cluster Topology

What stores are in the cluster, for each store, the locality of the node it is
on, store attributes and node attributes.

Membership

The membership status of stores. Stores can be ACTIVE, DECOMMISSIONING or
DECOMMISSIONED.

Liveness Information

The liveness status of stores. Stores can be LIVE, UNAVAILBLE, SUSPECT or DEAD.

LIVE stores are considered available. UNAVAILABLE stores were recently
considered unavailable. SUSPECT stores are available but were recently
unavailable. DEAD stores are unavailable and aren't expected to become
available.

Stores can also be on a DRAINING node.

Range Information

The current replica allocation and raft status. Lease status.


Integration


Store Input

Store -> Store Gossip -> [GOSSIP] -> CB(Store Rebalancer) -> allocator2.ProcessNodeLoadResponse

Range Load Input

Leaseholder (local).LoadStats -> topKRanges -> allocator2.ProcessNodeLoadResponse

Range Input

Leaseholder (local) -> Range Descriptor 

Lease Input

Leaseholder (local) -> LeaseStatus

LivenessInput

NodeVitality -> onChangeCB(Store Rebalancer) -> allocator2.UpdateNodeFdSummary

Span Config Input

SpanConfigStore -> onChange -> ...


The store rebalancer will call ComputeChanges every 10s. Given the changes
returned, the Store Rebalancer will apply the changes in the order given,
limited by the concurrency of the receivers snapshot queue and some fixed total
limit.

When a change is complete, the store rebalancer will report the change as
enacted. 

Store inputs such as locality, load and attributes, will come from the store
descriptor gossiped every 10s, and on capacity changes. 

Range inputs will be a immutable version from the local leaseholder replica.

- Questions about the change:

  - What components in CockroachDB need to change? How do they change?
      The allocation system entirely. This RFC is scoped to only include
      replacing the rebalancing system. The rebalancing system currently
      consists of the store rebalancer and replicate queue. The store
      rebalancer is run every 60 seconds, once per store. The store rebalancer
      moves range leases and replicas in order to maintain its own QPS or
      replica CPU below 115% of the cluster mean. The replicate queue
      rebalances leases and replicas in order to maintain +-5% thresholds of
      the mean. The replicate queue processes ranges (to be rebalanced) in
      random order, considering each range once every 10 minutes (not true on
      small/high density stores). Each store can only make allocate for ranges
      the store is the leaseholder of (distributed allocation).

      Multi-metric allocation replaces both these compontents responsibilities,
      in a similar distributed manner. Every node will have a rebalancing
      component which will apply allocation changes at 10s (configurable, TBD)
      interval. The allocation changes will be suggested by allocator2, which
      is responsible for selecting changes which satisfy constraints and
      converges metrics towards goals.


  - Are there new abstractions introduced by the change? New concepts?
    If yes, provide definitions and examples.

  - How does the change behave in mixed-version deployments? During a
    version upgrade? Which migrations are needed?
    Multi-metric rebalancing is disabled in deployments which contains any
    older version. In future versions, compatibility will be maintained during
    mixed-version via applying the oldest versions behavior. The biggest risk
    during a mixed-version is opposing allocation behavior, which may lead to
    replicas and leases thrashing between stores.

  - Is the result/usage of this change different for CC end-users than
    for on-prem deployments? How?

  - What are the possible interactions with other features or
    sub-systems inside CockroachDB? How does the behavior of other code
    change implicitly as a result of the changes outlined in the RFC?

    (Provide examples if relevant.)

  - Is there other ongoing or recent RFC work that is related?
    (Cross-reference the relevant RFCs.)

  - What are the edge cases? What are example uses or inputs that we
    think are uncommon but are still possible and thus need to be
    handled? How are these edge cases handled? Provide examples.

  - What are the effect of possible mistakes by other CockroachDB team
    members trying to use the feature in their own code? How does the
    change impact how they will troubleshoot things?

- Questions about performance:

  - Does the change impact performance? How?

  - If new algorithms are
    introduced whose execution time depend on per-deployment parameters
    (e.g. number of users, number of ranges, etc), what is their
    high-level worst case algorithmic complexity?

  - How is resource usage affected for “large” loads? For example,
    what do we expect to happen when there are 100000 ranges? 100000
    tables? 10000 databases? 10000 tenants? 10000 SQL users?  1000000
    concurrent SQL queries?

- Stability questions:

  - Can this new functionality affect the stability of a node or the
    entire cluster? How does the behavior of a node or a cluster degrade
    if there is an error in the implementation?

  - Can the new functionality be disabled? Can a user opt out? How?

  - Can the new functionality affect clusters which are not explicitly
    using it?

  - What testing and safe guards are being put in place to
    protect against unexpected problems?

- Security questions:

  - Does the change concern authentication or authorization logic? If
    so, mention this explicitly tag the relevant security-minded
    reviewer as reviewer to the RFC.

  - Does the change create a new way to communicate data over the
    network?  What rules are in place to ensure that this cannot be
    used by a malicious user to extract confidential data?

  - Is there telemetry or crash reporting? What mechanisms are used to
    ensure no sensitive data is accidentally exposed?

- Observability and usage questions:

  - Is the change affecting asynchronous / background subsystems?

    - If so, how can users and our team observe the run-time state via tracing?

    - Which other inspection APIs exist?

    (In general, think about how your coworkers and users will gain
    access to the internals of the change after it has happened to
    either gain understanding during execution or troubleshoot
    problems.)

  - Are there new APIs, or API changes (either internal or external)?

    - How would you document the new APIs? Include example usage.

    - What are the other components or teams that need to know about the
      new APIs and changes?

    - Which principles did you apply to ensure the APIs are consistent
      with other related features / APIs? (Cross-reference other APIs
      that are similar or related, for comparison.)

  - Is the change visible to users of CockroachDB or operators who run CockroachDB clusters?

    - Are there any user experience (UX) changes needed as a result of this RFC?

    - Are the UX changes necessary or clearly beneficial? (Cross-reference the motivation section.)

    - Which principles did you apply to ensure the user experience
      (UX) is consistent with other related features?
      (Cross-reference other CLI / GUI / SQL elements or features
      that have related UX, for comparison.)

    - Which other engineers or teams have you polled for input on the
      proposed UX changes? Which engineers or team may have relevant
      experience to provide feedback on UX?

  - Is usage of the new feature observable in telemetry? If so,
    mention where in the code telemetry counters or metrics would be
    added.

The section should return to the user stories in the motivations
ection, and explain more fully how the detailed proposal makes those
stories work.

## Drawbacks

Why should we *not* do this?

If applicable, list mitigating factors that may make each drawback acceptable.

Investigate the consequences of the proposed change onto other areas
of CockroachDB. If other features are impacted, especially UX, list
this impact as a reason not to do the change. If possible, also
investigate and suggest mitigating actions that would reduce the
impact. You can for example consider additional validation testing,
additional documentation or doc changes, new user research, etc.

Also investigate the consequences of the proposed change on
performance. Pay especially attention to the risk that introducing a
possible performance improvement in one area can slow down another
area in an unexpected way. Examine all the current "consumers" of the
code path you are proposing to change and consider whether the
performance of any of them may be negatively impacted by the proposed
change. List all these consequences as possible drawbacks.

## Rationale and Alternatives

This section is extremely important. See the
[README](README.md#rfc-process) file for details.

- Why is this design the best in the space of possible designs?
- What other designs have been considered and what is the rationale for not choosing them?
- What is the impact of not doing this?

# Explain it to someone else

How do we teach this?

Explain the proposal as if it was already included in the project and
you were teaching it to an end-user, or a CockroachDB team member in a different project area.

Consider the following writing prompts:

- Which new concepts have been introduced to end-users? Can you
  provide examples for each?

- How would end-users change their apps or thinking to use the change?

- Are there new error messages introduced? Can you provide examples?
  If there are SQL errors, what are their 5-character SQLSTATE codes?

- Are there new deprecation warnings? Can you provide examples?

- How are clusters affected that were created before this change? Are
  there migrations to consider?

# Unresolved questions

- What parts of the design do you expect to resolve through the RFC
  process before this gets merged?

- What parts of the design do you expect to resolve through the
  implementation of this feature before stabilization?

- What related issues do you consider out of scope for this RFC that
  could be addressed in the future independently of the solution that
  comes out of this RFC?

