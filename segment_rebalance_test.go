package freecache

import (
	"bytes"
	"fmt"
	"testing"
)

func TestSegmentRebalanceExpandsHotSegment(t *testing.T) {
	const cacheSize = 1024 * 1024
	cache := NewCache(cacheSize)

	hotSeg := -1
	var keys, values [][]byte
	for i := 0; len(keys) < 20; i++ {
		key := []byte(fmt.Sprintf("hot-key-%d", i))
		segID := cache.segmentIndex(hashFunc(key))
		if hotSeg == -1 {
			hotSeg = segID
		}
		if segID != hotSeg {
			continue
		}
		value := bytes.Repeat([]byte{byte(i)}, 80)
		keys = append(keys, key)
		values = append(values, value)
		if err := cache.Set(key, value, 0); err != nil {
			t.Fatal(err)
		}
	}

	initialCap := len(cache.segments[hotSeg].rb.data)
	for i := 0; i < 2048; i++ {
		idx := i % len(keys)
		value, err := cache.Get(keys[idx])
		if err != nil || !bytes.Equal(value, values[idx]) {
			t.Fatalf("Get(%q) = %q, %v; want %q", keys[idx], value, err, values[idx])
		}
	}

	hotCap := len(cache.segments[hotSeg].rb.data)
	if hotCap <= initialCap {
		t.Fatalf("hot segment capacity = %d, want more than %d", hotCap, initialCap)
	}

	totalCap := 0
	minColdCap := int(^uint(0) >> 1)
	for i := range cache.segments {
		capSize := len(cache.segments[i].rb.data)
		totalCap += capSize
		if i != hotSeg && capSize < minColdCap {
			minColdCap = capSize
		}
	}
	if totalCap != cacheSize {
		t.Fatalf("total segment capacity = %d, want %d", totalCap, cacheSize)
	}
	if minColdCap >= initialCap {
		t.Fatalf("cold segment capacity = %d, want less than %d", minColdCap, initialCap)
	}

	for i, key := range keys {
		value, err := cache.Get(key)
		if err != nil || !bytes.Equal(value, values[i]) {
			t.Fatalf("entry %q was lost during expansion: value %q, err %v", key, value, err)
		}
	}

	added := 0
	for i := 0; added < 50; i++ {
		key := []byte(fmt.Sprintf("hot-after-resize-%d", i))
		if cache.segmentIndex(hashFunc(key)) != hotSeg {
			continue
		}
		value := bytes.Repeat([]byte{byte(i)}, 80)
		if err := cache.Set(key, value, 0); err != nil {
			t.Fatal(err)
		}
		got, err := cache.Get(key)
		if err != nil || !bytes.Equal(got, value) {
			t.Fatalf("new entry %q after expansion: value %q, err %v", key, got, err)
		}
		added++
	}
}

func TestSegmentRebalanceShrinkKeepsColdEntries(t *testing.T) {
	const cacheSize = 4 * 1024 * 1024
	cache := NewCache(cacheSize)

	keys := make([][]byte, 12000)
	values := make([][]byte, 12000)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("cold-key-%d", i))
		values[i] = bytes.Repeat([]byte{byte(i)}, 60)
		if err := cache.Set(keys[i], values[i], 0); err != nil {
			t.Fatal(err)
		}
	}

	hotSeg := -1
	var hotKeys [][]byte
	for _, key := range keys {
		segID := cache.segmentIndex(hashFunc(key))
		if hotSeg == -1 {
			hotSeg = segID
		}
		if segID == hotSeg {
			hotKeys = append(hotKeys, key)
			if len(hotKeys) == 16 {
				break
			}
		}
	}
	for i := 0; i < 2048; i++ {
		cache.Get(hotKeys[i%len(hotKeys)])
	}

	totalCap := 0
	for i := range cache.segments {
		totalCap += len(cache.segments[i].rb.data)
	}
	if totalCap != cacheSize {
		t.Fatalf("total segment capacity = %d, want %d", totalCap, cacheSize)
	}

	for i, key := range keys {
		value, err := cache.Get(key)
		if err != nil || !bytes.Equal(value, values[i]) {
			t.Fatalf("entry %q was lost during cold-segment shrink: value %q, err %v", key, value, err)
		}
	}
}
