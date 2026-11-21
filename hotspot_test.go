package freecache

import (
	"bytes"
	"fmt"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// hotSegmentKeyCount is the number of distinct hot keys used by the
// same-segment benchmarks and tests.
const hotSegmentKeyCount = 4096

// makeSameSegmentKeys returns n keys sharing the given table prefix that all
// land on the same segment when the segment is chosen by the low bits of the
// hash (hashVal&segmentAndOpVal == segID). It simulates a batch of hot cache
// keys of a single table that happen to collide on one segment.
func makeSameSegmentKeys(prefix string, n int, segID uint64) [][]byte {
	keys := make([][]byte, 0, n)
	for i := 0; len(keys) < n; i++ {
		key := []byte(prefix + strconv.Itoa(i))
		if hashFunc(key)&segmentAndOpVal == segID {
			keys = append(keys, key)
		}
	}
	return keys
}

// BenchmarkParallelCacheSetSameSegment is BenchmarkParallelCacheSet with a
// batch of same-table keys that all map to one segment by the low hash bits.
func BenchmarkParallelCacheSetSameSegment(b *testing.B) {
	cache := NewCache(256 * 1024 * 1024)
	keys := makeSameSegmentKeys("user:profile:", hotSegmentKeyCount, 0)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			cache.Set(keys[counter%hotSegmentKeyCount], make([]byte, 8), 0)
			counter++
		}
	})
}

// BenchmarkParallelCacheGetSameSegment is BenchmarkParallelCacheGet with a
// batch of same-table keys that all map to one segment by the low hash bits.
func BenchmarkParallelCacheGetSameSegment(b *testing.B) {
	b.StopTimer()
	cache := NewCache(256 * 1024 * 1024)
	keys := makeSameSegmentKeys("user:profile:", hotSegmentKeyCount, 0)
	value := make([]byte, 8)
	for _, key := range keys {
		if err := cache.Set(key, value, 0); err != nil {
			b.Fatal(err)
		}
	}
	b.StartTimer()
	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			cache.Get(keys[counter%hotSegmentKeyCount])
			counter++
		}
	})
}

// TestSameSegmentLockContention hammers a batch of same-table hot keys that
// collide on a single segment under low-bit sharding and reports the share of
// CPU time workers spend blocked on segment locks.
func TestSameSegmentLockContention(t *testing.T) {
	cpus := runtime.GOMAXPROCS(0)
	if cpus < 2 {
		t.Skip("needs at least 2 CPUs")
	}
	workers := 4 * cpus
	cache := NewCache(256 * 1024 * 1024)
	keys := makeSameSegmentKeys("user:profile:", hotSegmentKeyCount, 0)
	value := make([]byte, 8)

	runtime.SetMutexProfileFraction(1)
	defer runtime.SetMutexProfileFraction(0)

	const duration = 2 * time.Second
	counts := make([]int64, workers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			<-start
			deadline := time.Now().Add(duration)
			var n int64
			for i := 0; ; i++ {
				if i&1023 == 0 && time.Now().After(deadline) {
					break
				}
				cache.Set(keys[i%hotSegmentKeyCount], value, 0)
				n++
			}
			counts[w] = n
		}(w)
	}
	begin := time.Now()
	close(start)
	wg.Wait()
	wall := time.Since(begin)

	waitNs := mutexProfileWaitNs(t)
	var ops int64
	for _, c := range counts {
		ops += c
	}
	contention := waitNs / (float64(wall.Nanoseconds()) * float64(workers))
	t.Logf("workers=%d ops=%d throughput=%.0f op/s lockWait=%v wall=%v contention=%.1f%%",
		workers, ops, float64(ops)/wall.Seconds(), time.Duration(waitNs), wall, contention*100)
	if contention > 0.5 {
		t.Fatalf("segment lock contention too high: %.1f%%", contention*100)
	}
}

// mutexProfileWaitNs returns the total mutex contention delay recorded in the
// current mutex profile, in nanoseconds. The text format carries the
// cycles/second calibration needed to convert the recorded cycles.
func mutexProfileWaitNs(t *testing.T) float64 {
	t.Helper()
	var buf bytes.Buffer
	if err := pprof.Lookup("mutex").WriteTo(&buf, 1); err != nil {
		t.Fatal(err)
	}
	var cyclesPerSecond, waitCycles float64
	for _, line := range strings.Split(buf.String(), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 1 && strings.HasPrefix(fields[0], "cycles/second=") {
			if _, err := fmt.Sscanf(fields[0], "cycles/second=%g", &cyclesPerSecond); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if len(fields) >= 3 && fields[2] == "@" {
			cycles, err := strconv.ParseFloat(fields[0], 64)
			if err != nil {
				t.Fatal(err)
			}
			waitCycles += cycles
		}
	}
	if cyclesPerSecond == 0 {
		t.Fatal("mutex profile missing cycles/second calibration")
	}
	return waitCycles / cyclesPerSecond * 1e9
}
