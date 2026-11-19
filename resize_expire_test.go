package freecache

import (
	"testing"
	"time"
)

// controlledTimer lets a test advance the cache clock deterministically.
type controlledTimer struct {
	now uint32
}

func (t *controlledTimer) Now() uint32 { return t.now }

// TestResizePreservesExpiration ensures migrated entries keep their absolute
// expiration rather than being granted a fresh TTL by the copy.
func TestResizePreservesExpiration(t *testing.T) {
	timer := &controlledTimer{now: 1000}
	cache := NewCacheCustomTimer(2*1024*1024, timer)

	key := []byte("ttl-key")
	if err := cache.Set(key, []byte("v"), 100); err != nil {
		t.Fatal(err)
	}
	expireBefore, err := cache.TTL(key)
	if err != nil {
		t.Fatal(err)
	}

	cache.Resize(4 * 1024 * 1024)

	expireAfter, err := cache.TTL(key)
	if err != nil {
		t.Fatalf("key lost during resize: %v", err)
	}
	// No wall-clock time advanced in the controlled timer, so TTL must be
	// identical before and after migration.
	if expireAfter != expireBefore {
		t.Fatalf("TTL changed through resize: before=%d after=%d", expireBefore, expireAfter)
	}

	// Advancing beyond the original expiration must expire the copied entry.
	timer.now = 1101
	if _, err := cache.Get(key); err != ErrNotFound {
		t.Fatalf("expected expiration, got err=%v", err)
	}
}

// TestResizeWithRealTimerExpiry sanity-checks expiry under the default timer.
func TestResizeWithRealTimerExpiry(t *testing.T) {
	cache := NewCache(2 * 1024 * 1024)
	key := []byte("short")
	if err := cache.Set(key, []byte("v"), 1); err != nil {
		t.Fatal(err)
	}
	cache.Resize(4 * 1024 * 1024)
	time.Sleep(2 * time.Second)
	if _, err := cache.Get(key); err != ErrNotFound {
		t.Fatalf("expected entry to expire, got err=%v", err)
	}
}
