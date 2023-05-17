// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package allocator2

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// TODO: tests for
// - storeIDPostingList
// - localityTierInterner
// - localityTiers.diversityScore
// - rangeAnalyzedConstraints initialization: pool and release; stateForInit, finishInit
// - rangeAnalyzedConstraints read-only functions.

func TestNormalizedSpanConfig(t *testing.T) {
	interner := newStringInterner()
	datadriven.RunTest(t, "testdata/normalize_config",
		func(t *testing.T, d *datadriven.TestData) string {
			parseConstraints := func(fields []string) []roachpb.Constraint {
				var cc []roachpb.Constraint
				for _, field := range fields {
					var typ roachpb.Constraint_Type
					switch field[0] {
					case '+':
						typ = roachpb.Constraint_REQUIRED
					case '-':
						typ = roachpb.Constraint_PROHIBITED
					default:
						t.Fatalf(fmt.Sprintf("unexpected start of field %s", field))
					}
					kv := strings.Split(field[1:], "=")
					if len(kv) != 2 {
						t.Fatalf("unexpected field %s", field)
					}
					cc = append(cc, roachpb.Constraint{
						Type:  typ,
						Key:   kv[0],
						Value: kv[1],
					})
				}
				return cc
			}
			parseConstraintsConj := func(fields []string) roachpb.ConstraintsConjunction {
				var cc roachpb.ConstraintsConjunction
				if strings.HasPrefix(fields[0], "num-replicas=") {
					val := strings.TrimPrefix(fields[0], "num-replicas=")
					replicas, err := strconv.Atoi(val)
					require.NoError(t, err)
					cc.NumReplicas = int32(replicas)
					fields = fields[1:]
				}
				cc.Constraints = parseConstraints(fields)
				return cc
			}
			printSpanConf := func(b *strings.Builder, conf roachpb.SpanConfig) {
				fmt.Fprintf(b, " num-replicas=%d num-voters=%d\n", conf.NumReplicas, conf.NumVoters)
				if len(conf.Constraints) > 0 {
					fmt.Fprintf(b, " constraints:\n")
					for _, cc := range conf.Constraints {
						fmt.Fprintf(b, "   %s\n", cc.String())
					}
				}
				if len(conf.VoterConstraints) > 0 {
					fmt.Fprintf(b, " voter-constraints:\n")
					for _, cc := range conf.VoterConstraints {
						fmt.Fprintf(b, "   %s\n", cc.String())
					}
				}
				if len(conf.LeasePreferences) > 0 {
					fmt.Fprintf(b, " lease-preferences:\n")
					for _, lp := range conf.LeasePreferences {
						fmt.Fprintf(b, "   ")
						for i, cons := range lp.Constraints {
							if i > 0 {
								b.WriteRune(',')
							}
							b.WriteString(cons.String())
						}
						fmt.Fprintf(b, "\n")
					}
				}
			}
			switch d.Cmd {
			case "normalize":
				var numReplicas, numVoters int
				var conf roachpb.SpanConfig
				d.ScanArgs(t, "num-replicas", &numReplicas)
				conf.NumReplicas = int32(numReplicas)
				d.ScanArgs(t, "num-voters", &numVoters)
				conf.NumVoters = int32(numVoters)
				for _, line := range strings.Split(d.Input, "\n") {
					parts := strings.Fields(line)
					switch parts[0] {
					case "constraint":
						cc := parseConstraintsConj(parts[1:])
						conf.Constraints = append(conf.Constraints, cc)
					case "voter-constraint":
						cc := parseConstraintsConj(parts[1:])
						conf.VoterConstraints = append(conf.VoterConstraints, cc)
					case "lease-preference":
						cc := parseConstraints(parts[1:])
						conf.LeasePreferences = append(conf.LeasePreferences, roachpb.LeasePreference{
							Constraints: cc,
						})
					default:
						return fmt.Sprintf("unknown field: %s", parts[0])
					}
				}
				var b strings.Builder
				fmt.Fprintf(&b, "input:\n")
				printSpanConf(&b, conf)
				nConf, err := makeNormalizedSpanConfig(&conf, interner)
				if err != nil {
					fmt.Fprintf(&b, "err=%s\n", err.Error())
				}
				if nConf != nil {
					fmt.Fprintf(&b, "output:\n")
					printSpanConf(&b, nConf.uninternedConfig())
				}
				return b.String()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

// candidatesToConvertFromNonVoterToVoter []storeID
// constraintsForAddingVoter constraintsDisj
// constraintsForAddingNonVoter constraintsDisj
// candidatesToConverFromVoterToNonVoter []storeID
// candidatesForRoleSwapForConstraints [2][]storeID
// candidatesToRemove  []storeID
// candidatesVoterConstraintsUnsatisfied []storeID constraintsDisj
// candidatesNonVoterConstraintsUnsatisfied  []storeID constraintsDisj
// candidatesToReplaceVoterForRebalance constraintsDisj
// candidatesToReplaceNonVoterForRebalance constraintsDisj
//
// TestRangeAnalyzedConstraints is a data-driven test that asserts on the
// candidates and constraints disjunction of the readOnly functions on
// rangeAnalyzedConstraints. The following syntax is provided.
//
//   - "config" num-replicas=<int> num-voters=<int>
//     constraint <constraint-conjunction>
//     voter-constraint <constraint-conjunction>
//     lease-preference <constraint>
//
//   - "replicas"
//     store=<int> kind=(voter|non-voter) attrs=<[key=value]>
//
//   - "analyze" (nonVoterToVoter | addingVoter | addingNonVoter |
//     nonVoterToVoter | roleSwap | toRemove | voterUnsatisfied |
//     nonVoterUnsatisfied | replaceVoterRebalance | replaceNonVoterRebalance)
func TestRangeAnalyzedConstraintsCandidates(t *testing.T) {
	type replStoreInfo struct {
		storeID  roachpb.StoreID
		kind     roachpb.ReplicaType
		locality roachpb.Locality
	}
	var replInfos []replStoreInfo
	var normalizedConf *normalizedSpanConfig
	var rac *rangeAnalyzedConstraints

	interner := newStringInterner()
	localityInterner := newLocalityTierInterner(interner)

	datadriven.RunTest(t, "testdata/range_analyzed_constraints",
		func(t *testing.T, d *datadriven.TestData) string {

			parseConstraints := func(fields []string) []roachpb.Constraint {
				var cc []roachpb.Constraint
				for _, field := range fields {
					var typ roachpb.Constraint_Type
					switch field[0] {
					case '+':
						typ = roachpb.Constraint_REQUIRED
					case '-':
						typ = roachpb.Constraint_PROHIBITED
					default:
						t.Fatalf(fmt.Sprintf("unexpected start of field %s", field))
					}
					kv := strings.Split(field[1:], "=")
					if len(kv) != 2 {
						t.Fatalf("unexpected field %s", field)
					}
					cc = append(cc, roachpb.Constraint{
						Type:  typ,
						Key:   kv[0],
						Value: kv[1],
					})
				}
				return cc
			}
			parseConstraintsConj := func(fields []string) roachpb.ConstraintsConjunction {
				var cc roachpb.ConstraintsConjunction
				if strings.HasPrefix(fields[0], "num-replicas=") {
					val := strings.TrimPrefix(fields[0], "num-replicas=")
					replicas, err := strconv.Atoi(val)
					require.NoError(t, err)
					cc.NumReplicas = int32(replicas)
					fields = fields[1:]
				}
				cc.Constraints = parseConstraints(fields)
				return cc
			}

			parseReplica := func(fields []string) (
				storeID roachpb.StoreID, kind roachpb.ReplicaType, locality roachpb.Locality,
			) {
				for _, field := range fields {
					parts := strings.Split(field, "=")
					id, data := parts[0], parts[1]
					switch {
					case strings.HasPrefix(id, "store"):
						store, err := strconv.Atoi(data)
						require.NoError(t, err)
						storeID = roachpb.StoreID(store)
					case strings.HasPrefix(id, "attrs"):
						if data != "" {
							localityString := strings.TrimPrefix(field, "attrs=")
							require.NoError(t, locality.Set(localityString))
						}
					case strings.HasPrefix(id, "kind"):
						if data == "voter" {
							kind = roachpb.VOTER_FULL
						} else if data == "non-voter" {
							kind = roachpb.NON_VOTER
						} else {
							t.Fatalf("unexpected replica type %s", data)
						}
					default:
						t.Fatalf("malformed fields %v", fields)
					}
				}
				return
			}

			printSpanConf := func(b *strings.Builder, conf roachpb.SpanConfig) {
				fmt.Fprintf(b, " num-replicas=%d num-voters=%d\n", conf.NumReplicas, conf.NumVoters)
				if len(conf.Constraints) > 0 {
					fmt.Fprintf(b, " constraints:\n")
					for _, cc := range conf.Constraints {
						fmt.Fprintf(b, "   %s\n", cc.String())
					}
				}
				if len(conf.VoterConstraints) > 0 {
					fmt.Fprintf(b, " voter-constraints:\n")
					for _, cc := range conf.VoterConstraints {
						fmt.Fprintf(b, "   %s\n", cc.String())
					}
				}
				if len(conf.LeasePreferences) > 0 {
					fmt.Fprintf(b, " lease-preferences:\n")
					for _, lp := range conf.LeasePreferences {
						fmt.Fprintf(b, "   ")
						for i, cons := range lp.Constraints {
							if i > 0 {
								b.WriteRune(',')
							}
							b.WriteString(cons.String())
						}
						fmt.Fprintf(b, "\n")
					}
				}
			}
			// config num-replicas=... num-voters=...
			//     constraint ....
			//     constraint ....
			//     voter-constraint ...
			//     lease-preference ...
			// replicas
			//     store=1 attrs=region=a,zone=a1
			switch d.Cmd {
			case "config":
				var numReplicas, numVoters int
				var conf roachpb.SpanConfig
				d.ScanArgs(t, "num-replicas", &numReplicas)
				conf.NumReplicas = int32(numReplicas)
				d.ScanArgs(t, "num-voters", &numVoters)
				conf.NumVoters = int32(numVoters)
				for _, line := range strings.Split(d.Input, "\n") {
					parts := strings.Fields(line)
					switch parts[0] {
					case "constraint":
						cc := parseConstraintsConj(parts[1:])
						conf.Constraints = append(conf.Constraints, cc)
					case "voter-constraint":
						cc := parseConstraintsConj(parts[1:])
						conf.VoterConstraints = append(conf.VoterConstraints, cc)
					case "lease-preference":
						cc := parseConstraints(parts[1:])
						conf.LeasePreferences = append(conf.LeasePreferences, roachpb.LeasePreference{
							Constraints: cc,
						})
					default:
						return fmt.Sprintf("unknown field: %s", parts[0])
					}
				}
				var b strings.Builder
				nConf, err := makeNormalizedSpanConfig(&conf, interner)
				if err != nil {
					fmt.Fprintf(&b, "err=%s\n", err.Error())
				}
				printSpanConf(&b, nConf.uninternedConfig())
				normalizedConf = nConf
				return b.String()
			case "replicas":
				nextReplInfos := []replStoreInfo{}
				for _, line := range strings.Split(d.Input, "\n") {
					parts := strings.Fields(line)
					storeID, kind, locality := parseReplica(parts)
					nextReplInfos = append(nextReplInfos, replStoreInfo{storeID, kind, locality})
				}
				replInfos = nextReplInfos
				return ""
			case "analyze":
				newRac := &rangeAnalyzedConstraints{}
				consMatcher := newConstraintMatcher(interner)
				analyzeBuf := newRac.stateForInit()
				for _, info := range replInfos {
					locality := localityInterner.intern(info.locality)
					analyzeBuf.tryAddingStore(info.storeID, info.kind, locality)
					storeDescriptor := roachpb.StoreDescriptor{
						StoreID: info.storeID,
						Node: roachpb.NodeDescriptor{
							NodeID:   roachpb.NodeID(info.storeID),
							Locality: info.locality,
						},
					}
					consMatcher.setStore(storeDescriptor)
				}

				newRac.finishInit(normalizedConf, consMatcher)
				rac = newRac
				return rac.uninternString(interner, localityInterner)
			case "eval":
				var analyzeFn string
				var store roachpb.StoreID
				d.ScanArgs(t, "fn", &analyzeFn)
				if d.HasArg("store") {
					var storeID int
					d.ScanArgs(t, "store", &storeID)
					store = roachpb.StoreID(storeID)
				}
				var toRemove []roachpb.StoreID
				var toAdd constraintsDisj
				var err error
				switch analyzeFn {
				case "nonVoterToVoter":
					toRemove, err = rac.candidatesToConvertFromNonVoterToVoter()
				case "addingVoter":
					toAdd, err = rac.constraintsForAddingNonVoter()
				case "addingNonVoter":
					toAdd, err = rac.constraintsForAddingNonVoter()
				case "voterToNonVoter":
					toRemove, err = rac.candidatesToConvertFromVoterToNonVoter()
				case "roleSwap":
					var toSwap [numReplicaKinds][]roachpb.StoreID
					toSwap, err = rac.candidatesForRoleSwapForConstraints()
					toRemove = append(toRemove, toSwap[voterIndex]...)
					toRemove = append(toRemove, toSwap[nonVoterIndex]...)
				case "toRemove":
					toRemove, err = rac.candidatesToRemove()
				case "voterUnsatisfied":
					toRemove, toAdd, err = rac.candidatesVoterConstraintsUnsatisfied()
				case "nonVoterUnsatisfied":
					toRemove, toAdd, err = rac.candidatesNonVoterConstraintsUnsatisfied()
				case "replaceVoterRebalance":
					toAdd, err = rac.candidatesToReplaceVoterForRebalance(store)
				case "replaceNonVoterRebalance":
					toAdd, err = rac.candidatesToReplaceNonVoterForRebalance(store)
				default:
					t.Fatalf("unknown analyze command %s", analyzeFn)
				}

				var buf strings.Builder
				if err != nil {
					return fmt.Sprintf("err: %s\n", err.Error())
				}
				if toRemove != nil {
					fmt.Fprintf(&buf, "remove %s\n", toRemove)
				}
				if toAdd != nil {
					fmt.Fprintf(&buf, "add %s\n", toAdd.unintern(interner))
				}

				return ""
			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}
