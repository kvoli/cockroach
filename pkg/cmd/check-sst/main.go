package main

import (
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	_ "github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

var keyKindMap = make(map[pebble.InternalKeyKind]int)

var (
	startKey = keys.TimeseriesPrefix
	endKey   = startKey.PrefixEnd()
)

var (
	dataDirectory = flag.String("data-dir", "ssts", "path to directory containing SSTs")
	makeRequests  = flag.Bool("make-requests", false, "make the clear range requests to delete the non-zero mvcc keys in `tsd`")
	outDirectory  = flag.String("out-dir", "artifacts", "path to output directory where requests will be saved")
)

func main() {
  flag.Parse()
	dir := *dataDirectory
	out := *outDirectory
	var keys, tsKeys int
	var nonZeroKeys []mvccKey

	fmt.Printf("data-dir=%s\n", dir)
	err := filepath.Walk(dir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if !strings.HasSuffix(filepath.Base(path), ".sst") {
			return nil
		}
		s, err := scanFile(vfs.Default, path)
		if err != nil {
			return err
		}
		keys += s.keys
		tsKeys += s.tsKeys
		nonZeroKeys = append(nonZeroKeys, s.nonZeroTSKeys...)
		return nil
	})
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("--- SUMMARY ---\n")
	fmt.Printf("total keys: %d (100%%)\n", keys)
	fmt.Printf("total ts keys: %d (%.2f%%)\n", tsKeys, 100*float32(tsKeys)/float32(keys))
	fmt.Printf("total non-zero keys: %d (%.2f%%)\n", len(nonZeroKeys), 100*float32(len(nonZeroKeys))/float32(keys))
	if len(nonZeroKeys) > 0 {
		fmt.Println("non-zero keys:")
		sort.Slice(nonZeroKeys, func(i, j int) bool {
			return nonZeroKeys[i].Timestamp.Less(nonZeroKeys[j].Timestamp)
		})
		for _, k := range nonZeroKeys {
			ts := timeutil.Unix(0, k.Timestamp.WallTime).Format(time.RFC3339Nano)
			fmt.Printf("%s,%s (%s)\n", k.String(), k.kind, ts)
		}
	}
	fmt.Println("pebble key kind counts:")
	for kind, count := range keyKindMap {
		fmt.Printf("%s:%d\n", kind, count)
	}

	// Create the clear-range and get key requests if the make requests flag is
	// set to true.
	if *makeRequests {
		for i, k := range nonZeroKeys {
			clearRangePath := filepath.Join(dir, fmt.Sprintf("%s/clear-range-%d.json", out, i))
			if err := os.WriteFile(clearRangePath, constructClearRange(k.Key), 0644); err != nil {
				panic(err)
			}
			getPath := filepath.Join(dir, fmt.Sprintf("%s/get-%d.json", out, i))
			if err := os.WriteFile(getPath, constructGetRequest(k.Key), 0644); err != nil {
				panic(err)
			}
		}
	}
}

type mvccKey struct {
	storage.MVCCKey
	kind pebble.InternalKeyKind
}

type stats struct {
	keys, tsKeys  int
	nonZeroTSKeys []mvccKey
}

func scanFile(fs vfs.FS, path string) (stats, error) {
	var s stats
	timeStart := time.Now()
	fmt.Printf("scanning SST %s:\n", filepath.Base(path))
	f, err := fs.Open(path)
	if err != nil {
		return s, err
	}
	sr, err := sstable.NewSimpleReadable(f)
	if err != nil {
		return s, err
	}
	r, err := sstable.NewReader(sr, sstable.ReaderOptions{
		Comparer:   storage.EngineComparer,
		MergerName: storage.MVCCMerger.Name,
	})
	if err != nil {
		return s, err
	}
	defer func() {
		if err := r.Close(); err != nil {
			panic(err)
		}
	}()
	s.keys = int(r.Properties.NumEntries)

	keyToStr := func(k *pebble.InternalKey) (string, error) {
		ek, ok := storage.DecodeEngineKey(k.UserKey)
		if !ok {
			return "", errors.Newf("could not decode engine key: %s", k.String())
		}
		return ek.Key.String(), nil
	}
	iter, err := r.NewIter(nil, nil)
	if err != nil {
		return s, err
	}
	k, _ := iter.First()
	first, err := keyToStr(k)
	if err != nil {
		return s, err
	}
	k, _ = iter.Last()
	last, err := keyToStr(k)
	if err != nil {
		return s, err
	}
	fmt.Printf("start: %s\nend: %s\n", first, last)

	iter, err = r.NewIter(startKey, endKey)
	if err != nil {
		return s, err
	}
	defer func() {
		if err := iter.Close(); err != nil {
			panic(err)
		}
	}()
	for k, _ := iter.SeekGE(startKey, 0); k != nil; k, _ = iter.Next() {
		s.tsKeys++
		kind := k.Kind()
		if _, ok := keyKindMap[kind]; !ok {
			keyKindMap[kind] = 0
		}
		keyKindMap[kind]++
		ek, ok := storage.DecodeEngineKey(k.UserKey)
		if !ok {
			return s, fmt.Errorf("not an engine key: %x", k.UserKey)
		}
		mvccK, err := ek.ToMVCCKey()
		if err != nil {
			return s, err
		}
		if mvccK.Timestamp.WallTime != 0 {
			s.nonZeroTSKeys = append(s.nonZeroTSKeys, mvccKey{
				MVCCKey: mvccK.Clone(),
				kind:    k.Kind(),
			})
		}
		_, _, _, _, err = decodeDataKeySuffix(mvccK.Key[4:])
		if err != nil {
			ts := timeutil.Unix(0, mvccK.Timestamp.WallTime).Format(time.RFC3339Nano)
			err = errors.Wrapf(err, "could not decode key %s (%s)", mvccK, ts)
			fmt.Printf("WARN: %s\n", err)
			continue
		}
	}
	fmt.Printf(
		"entries: %d\nts entries: %d\nnon-zero ts entries: %d\n",
		s.keys, s.tsKeys, len(s.nonZeroTSKeys),
	)

	duration := time.Since(timeStart)
	fmt.Printf("scanned in %s\n\n", duration)
	return s, nil
}

// --- The following is all cribbed from pkg/ts ---.

// Resolution enumeration values are directly serialized and persisted into
// system keys; these values must never be altered or reordered. If new rollup
// resolutions are added, the IsRollup() method must be modified as well.
const (
	// Resolution10s stores data with a sample resolution of 10 seconds.
	Resolution10s Resolution = 1
	// Resolution30m stores roll-up data from a higher resolution at a sample
	// resolution of 30 minutes.
	Resolution30m Resolution = 2
	// resolution1ns stores data with a sample resolution of 1 nanosecond. Used
	// only for testing.
	resolution1ns Resolution = 998
	// resolution50ns stores roll-up data from the 1ns resolution at a sample
	// resolution of 50 nanoseconds. Used for testing.
	resolution50ns Resolution = 999
	// resolutionInvalid is an invalid resolution used only for testing. It causes
	// an error to be thrown in certain methods. It is invalid because its sample
	// period is not a divisor of its slab period.
	resolutionInvalid Resolution = 1000
)

var slabDurationByResolution = map[Resolution]int64{
	Resolution10s:     int64(time.Hour),
	Resolution30m:     int64(time.Hour * 24),
	resolution1ns:     10,   // 1ns resolution only for tests.
	resolution50ns:    1000, // 50ns rollup only for tests.
	resolutionInvalid: 11,
}

func decodeDataKeySuffix(key roachpb.Key) (string, string, Resolution, int64, error) {
	// Decode series name.
	remainder, name, err := encoding.DecodeBytesAscending(key, nil)
	if err != nil {
		return "", "", 0, 0, err
	}
	// Decode resolution.
	remainder, resolutionInt, err := encoding.DecodeVarintAscending(remainder)
	if err != nil {
		return "", "", 0, 0, err
	}
	resolution := Resolution(resolutionInt)
	// Decode timestamp.
	remainder, timeslot, err := encoding.DecodeVarintAscending(remainder)
	if err != nil {
		return "", "", 0, 0, err
	}
	timestamp := timeslot * resolution.SlabDuration()
	// The remaining bytes are the source.
	source := remainder

	return string(name), string(source), resolution, timestamp, nil
}

type Resolution int64

func (r Resolution) SlabDuration() int64 {
	duration, ok := slabDurationByResolution[r]
	if !ok {
		panic(fmt.Sprintf("no slab duration found for resolution value %v", r))
	}
	return duration
}

func constructGetRequest(key roachpb.Key) []byte {
	var ba kvpb.BatchRequest
	ba.Add(kvpb.NewGet(key, false /* forUpdate */))
	jsonpb := protoutil.JSONPb{}
	jsonProto, err := jsonpb.Marshal(&ba)
	if err != nil {
		panic(err)
	}
	return jsonProto
}

func constructClearRange(key roachpb.Key) []byte {
	var ba kvpb.BatchRequest
	clr := &kvpb.ClearRangeRequest{
		RequestHeader: kvpb.RequestHeader{
			Key:    key,
			EndKey: key.Next(),
		},
	}
	ba.Add(clr)
	jsonpb := protoutil.JSONPb{}
	jsonProto, err := jsonpb.Marshal(&ba)
	if err != nil {
		panic(err)
	}
	return jsonProto
}
