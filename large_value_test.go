package freecache

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

func TestSetLargeChunkedValue(t *testing.T) {
	cacheSize := 8 * 1024 * 1024
	cache := NewCache(cacheSize)
	key := []byte("large-object")
	value := make([]byte, 1024*1024)
	for i := range value {
		value[i] = byte(i*31 + i/251)
	}

	oldLimit := cacheSize/1024 - ENTRY_HDR_SIZE - len(key)
	if len(value) <= oldLimit {
		t.Fatalf("test value length %d is not beyond old limit %d", len(value), oldLimit)
	}

	if err := cache.Set(key, value, 0); err != nil {
		t.Fatalf("Set large value: %v", err)
	}
	if got, err := cache.Get(key); err != nil || !bytes.Equal(got, value) {
		t.Fatalf("Get large value: err=%v equal=%v", err, bytes.Equal(got, value))
	}
	if err := cache.GetFn(key, func(got []byte) error {
		if !bytes.Equal(got, value) {
			t.Fatal("GetFn large value mismatch")
		}
		return nil
	}); err != nil {
		t.Fatalf("GetFn large value: %v", err)
	}
	if count := cache.EntryCount(); count != 1 {
		t.Fatalf("EntryCount after Set = %d, want 1", count)
	}

	smaller := value[:12345]
	if err := cache.Set(key, smaller, 0); err != nil {
		t.Fatalf("overwrite with smaller value: %v", err)
	}
	if got, err := cache.Get(key); err != nil || !bytes.Equal(got, smaller) {
		t.Fatalf("Get smaller overwrite: err=%v equal=%v", err, bytes.Equal(got, smaller))
	}

	larger := append(append([]byte(nil), value...), 7, 8, 9)
	if err := cache.Set(key, larger, 60); err != nil {
		t.Fatalf("overwrite with larger value: %v", err)
	}
	if got, expireAt, err := cache.GetWithExpiration(key); err != nil || !bytes.Equal(got, larger) {
		t.Fatalf("Get larger overwrite: err=%v equal=%v", err, bytes.Equal(got, larger))
	} else if expireAt == 0 {
		t.Fatal("large overwrite did not preserve expiration")
	}

	it := cache.NewIterator()
	var sawLarge bool
	for entry := it.Next(); entry != nil; entry = it.Next() {
		if bytes.Equal(entry.Key, key) {
			sawLarge = true
			if !bytes.Equal(entry.Value, larger) {
				t.Fatalf("iterator large value length = %d, want %d", len(entry.Value), len(larger))
			}
		}
	}
	if !sawLarge {
		t.Fatal("iterator did not visit large entry")
	}

	if !cache.Del(key) {
		t.Fatal("Del large entry returned false")
	}
	if _, err := cache.Get(key); err != ErrNotFound {
		t.Fatalf("Get after Del err = %v, want ErrNotFound", err)
	}
	if count := cache.EntryCount(); count != 0 {
		t.Fatalf("EntryCount after Del = %d, want 0", count)
	}
}

func TestLargeChunkedValueSmallCache(t *testing.T) {
	cache := NewCache(512 * 1024)
	for _, size := range []int{4096, 32 * 1024, 100 * 1024} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			key := []byte("k")
			value := bytes.Repeat([]byte{byte(size), byte(size >> 8), byte(size >> 16)}, size/3+1)[:size]
			if err := cache.Set(key, value, 0); err != nil {
				t.Fatalf("Set size %d: %v", size, err)
			}
			got, err := cache.Get(key)
			if err != nil {
				t.Fatalf("Get size %d: %v", size, err)
			}
			if !bytes.Equal(got, value) {
				t.Fatalf("value mismatch for size %d", size)
			}
		})
	}
}

func TestLargeChunkedValueConcurrent(t *testing.T) {
	cache := NewCache(16 * 1024 * 1024)
	var wg sync.WaitGroup
	for goroutine := 0; goroutine < 8; goroutine++ {
		wg.Add(1)
		go func(goroutine int) {
			defer wg.Done()
			key := []byte(fmt.Sprintf("large-%d", goroutine))
			first := bytes.Repeat([]byte{byte(goroutine), 1, 2, 3}, 40*1024)
			second := bytes.Repeat([]byte{byte(goroutine), 4, 5, 6}, 40*1024)[:130000]
			for iteration := 0; iteration < 10; iteration++ {
				value := first
				if iteration%2 == 0 {
					value = second
				}
				if err := cache.Set(key, value, 0); err != nil {
					t.Errorf("Set %s: %v", key, err)
					return
				}
				got, err := cache.Get(key)
				if err != nil {
					t.Errorf("Get %s: %v", key, err)
					return
				}
				if !bytes.Equal(got, value) {
					t.Errorf("value mismatch for %s iteration %d", key, iteration)
					return
				}
			}
		}(goroutine)
	}
	wg.Wait()
}

func TestLargeChunkedValueAtomicWrappers(t *testing.T) {
	cache := NewCache(4 * 1024 * 1024)
	key := []byte("atomic-large")
	value := bytes.Repeat([]byte("chunk-"), 20*1024)

	ret, err := cache.GetOrSet(key, value, 0)
	if err != nil || ret != nil {
		t.Fatalf("first GetOrSet ret=%v err=%v", ret, err)
	}
	other := bytes.Repeat([]byte("other-"), 20*1024)
	ret, err = cache.GetOrSet(key, other, 0)
	if err != nil || !bytes.Equal(ret, value) {
		t.Fatalf("existing GetOrSet err=%v equal=%v", err, bytes.Equal(ret, value))
	}

	ret, found, err := cache.SetAndGet(key, other, 0)
	if err != nil || !found || !bytes.Equal(ret, value) {
		t.Fatalf("SetAndGet found=%v err=%v old-equal=%v", found, err, bytes.Equal(ret, value))
	}
	got, err := cache.Get(key)
	if err != nil || !bytes.Equal(got, other) {
		t.Fatalf("SetAndGet value err=%v equal=%v", err, bytes.Equal(got, other))
	}

	found, replaced, err := cache.Update(key, func(old []byte, found bool) ([]byte, bool, int) {
		if !found || !bytes.Equal(old, other) {
			t.Errorf("unexpected old value, found=%v", found)
		}
		return append(append([]byte(nil), old...), []byte("-extended")...), true, 0
	})
	if err != nil || !found || !replaced {
		t.Fatalf("Update found=%v replaced=%v err=%v", found, replaced, err)
	}
	got, err = cache.Get(key)
	if err != nil || !bytes.HasSuffix(got, []byte("-extended")) || !bytes.HasPrefix(got, other) {
		t.Fatalf("Update result err=%v prefix=%v suffix=%v", err, bytes.HasPrefix(got, other), bytes.HasSuffix(got, []byte("-extended")))
	}
}
