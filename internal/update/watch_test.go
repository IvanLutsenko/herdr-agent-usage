/**
 * Tests for the idle $limit watcher lock and skip/tick loop.
 */
package update

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func isolateWatch(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("USAGEBAR_STATE_DIR", dir)
	t.Setenv("USAGEBAR_WATCH_LOCK_PATH", filepath.Join(dir, watchLockName))
	t.Setenv("USAGEBAR_PANE_HEARTBEAT_PATH", filepath.Join(dir, heartbeatFileName))
}

func TestTryAcquireWatchLock_SecondInstanceLoses(t *testing.T) {
	isolateWatch(t)
	now := time.UnixMilli(1_700_000_000_000)
	first, ok := tryAcquireWatchLock(now)
	if !ok {
		t.Fatal("first acquire must succeed")
	}
	defer releaseWatchLock(first)
	if _, ok := tryAcquireWatchLock(now.Add(time.Second)); ok {
		t.Fatal("second live watcher must lose")
	}
}

func TestWatchAlreadyRunning(t *testing.T) {
	isolateWatch(t)
	now := time.UnixMilli(1_700_000_000_000)
	if WatchAlreadyRunning(now) {
		t.Fatal("missing lock is not running")
	}
	first, ok := tryAcquireWatchLock(now)
	if !ok {
		t.Fatal("acquire")
	}
	defer releaseWatchLock(first)
	if !WatchAlreadyRunning(now.Add(time.Second)) {
		t.Fatal("live lock must look running")
	}
	if WatchAlreadyRunning(now.Add(watchLockStale + time.Second)) {
		t.Fatal("stale lock must not look running")
	}
}

func TestTryAcquireWatchLock_StaleLockIsReclaimed(t *testing.T) {
	isolateWatch(t)
	now := time.UnixMilli(1_700_000_000_000)
	if err := os.WriteFile(watchLockPath(), []byte("stale\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	stale := now.Add(-watchLockStale - time.Second)
	if err := os.Chtimes(watchLockPath(), stale, stale); err != nil {
		t.Fatal(err)
	}
	lock, ok := tryAcquireWatchLock(now)
	if !ok {
		t.Fatal("stale lock must be reclaimed")
	}
	releaseWatchLock(lock)
}

func TestWatchLoop_SecondAcquireDoesNotTick(t *testing.T) {
	ticks := 0
	watchLoop{
		now:   func() time.Time { return time.UnixMilli(1) },
		sleep: func(time.Duration) {},
		stop:  alreadyStopped(),
		tick:  func() { ticks++ },
		acquire: func(time.Time) (*os.File, bool) {
			return nil, false
		},
	}.run()
	if ticks != 0 {
		t.Fatalf("ticks=%d", ticks)
	}
}

func TestWatchLoop_SkipsThenTicks(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000)
	ticks := 0
	skips := 0
	sleeps := 0
	stop := make(chan struct{})
	watchLoop{
		now: func() time.Time { return now },
		sleep: func(d time.Duration) {
			if d != WatchRefreshInterval {
				t.Fatalf("slept %s", d)
			}
			sleeps++
			now = now.Add(d)
		},
		stop: stop,
		skip: func(time.Time) bool {
			skips++
			return skips == 1
		},
		tick: func() {
			ticks++
			close(stop)
		},
		acquire: func(time.Time) (*os.File, bool) { return nil, true },
		release: func(*os.File) {},
		touch:   func(*os.File, time.Time) {},
	}.run()
	if ticks != 1 || skips != 2 || sleeps != 1 {
		t.Fatalf("ticks=%d skips=%d sleeps=%d", ticks, skips, sleeps)
	}
}

func TestReleaseWatchLock_KeepsReclaimedLock(t *testing.T) {
	isolateWatch(t)
	now := time.UnixMilli(1_700_000_000_000)
	old, ok := tryAcquireWatchLock(now)
	if !ok {
		t.Fatal("first acquire must succeed")
	}
	later := now.Add(watchLockStale + time.Second)
	replacement, ok := tryAcquireWatchLock(later)
	if !ok {
		t.Fatal("stale lock must be reclaimed")
	}
	defer releaseWatchLock(replacement)
	if ownsWatchLock(old) {
		t.Fatal("reclaimed lock must not belong to the old watcher")
	}
	if !ownsWatchLock(replacement) {
		t.Fatal("replacement must own the lock")
	}
	releaseWatchLock(old)
	if !WatchAlreadyRunning(later) {
		t.Fatal("old watcher must not remove the replacement lock")
	}
}

// A displaced watcher's heartbeat must not make its replacement appear fresh.
func TestTouchWatchLock_OnlyTouchesAcquiredInode(t *testing.T) {
	isolateWatch(t)
	now := time.UnixMilli(1_700_000_000_000)
	old, ok := tryAcquireWatchLock(now)
	if !ok {
		t.Fatal("first acquire")
	}
	later := now.Add(watchLockStale + time.Second)
	replacement, ok := tryAcquireWatchLock(later)
	if !ok {
		t.Fatal("stale lock must be reclaimed")
	}
	defer releaseWatchLock(replacement)
	defer releaseWatchLock(old)

	touchWatchLock(old, later.Add(time.Minute))
	current, err := os.Stat(watchLockPath())
	if err != nil {
		t.Fatal(err)
	}
	if !current.ModTime().Equal(later) {
		t.Fatalf("replacement mtime = %s, want %s", current.ModTime(), later)
	}
	held, err := old.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if !held.ModTime().Equal(later.Add(time.Minute)) {
		t.Fatalf("old inode mtime = %s", held.ModTime())
	}
}

// The stale claimant starts after release checked ownership but before unlink.
// It must acquire the lock only after release finishes, then retain it.
func TestReleaseWatchLock_SerializesReclaimAfterOwnershipCheck(t *testing.T) {
	isolateWatch(t)
	now := time.UnixMilli(1_700_000_000_000)
	old, ok := tryAcquireWatchLock(now)
	if !ok {
		t.Fatal("first acquire")
	}
	type acquired struct {
		lock *os.File
		ok   bool
	}
	started := make(chan struct{})
	result := make(chan acquired, 1)
	releaseWatchLockWithHook(old, func() {
		go func() {
			close(started)
			lock, ok := tryAcquireWatchLock(now.Add(watchLockStale + time.Second))
			result <- acquired{lock, ok}
		}()
		<-started
		select {
		case got := <-result:
			if got.lock != nil {
				_ = got.lock.Close()
			}
			t.Fatal("claimant acquired before release removed the old inode")
		case <-time.After(20 * time.Millisecond):
		}
	})
	select {
	case got := <-result:
		if !got.ok || !ownsWatchLock(got.lock) {
			t.Fatal("new watcher did not retain the lock after release")
		}
		releaseWatchLock(got.lock)
	case <-time.After(2 * time.Second):
		t.Fatal("claimant could not acquire after release")
	}
}

func TestWatchLoop_ExitsAfterLockIsReclaimed(t *testing.T) {
	ticks := 0
	owned := true
	watchLoop{
		now:     func() time.Time { return time.UnixMilli(1) },
		sleep:   func(time.Duration) { owned = false },
		tick:    func() { ticks++ },
		acquire: func(time.Time) (*os.File, bool) { return nil, true },
		release: func(*os.File) {},
		touch:   func(*os.File, time.Time) {},
		owns:    func(*os.File) bool { return owned },
	}.run()
	if ticks != 1 {
		t.Fatalf("ticks=%d", ticks)
	}
}

func TestWatchLoop_BeatsOnlyAroundCollect(t *testing.T) {
	beats, stops := 0, 0
	skips := 0
	stop := make(chan struct{})
	watchLoop{
		now:   func() time.Time { return time.UnixMilli(1) },
		sleep: func(time.Duration) {},
		stop:  stop,
		skip: func(time.Time) bool {
			skips++
			return skips == 1
		},
		tick: func() {
			if beats != 1 || stops != 0 {
				t.Fatalf("collect ran outside the heartbeat: beats=%d stops=%d", beats, stops)
			}
			close(stop)
		},
		acquire: func(time.Time) (*os.File, bool) { return nil, true },
		release: func(*os.File) {},
		touch:   func(*os.File, time.Time) {},
		beat: func(*os.File) func() {
			beats++
			return func() { stops++ }
		},
	}.run()
	if beats != 1 || stops != 1 {
		t.Fatalf("beats=%d stops=%d", beats, stops)
	}
}

func alreadyStopped() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}
