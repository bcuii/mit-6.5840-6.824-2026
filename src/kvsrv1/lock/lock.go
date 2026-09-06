package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

// The value stored under the lock's key when nobody holds the lock.
const free = ""

// How long to wait before re-checking a lock held by someone else.
const retryInterval = 10 * time.Millisecond

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk

	// key holds the lock's state in the k/v server: either free, or
	// the id of the client that currently holds the lock.
	key string

	// id identifies this lock client. It lets us recognize a lock we
	// acquired ourselves, which is how we recover when a Put reports
	// ErrMaybe.
	id string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{
		ck:  ck,
		key: lockname,
		id:  kvtest.RandValue(8),
	}
	return lk
}

func (lk *Lock) Acquire() {
	for {
		val, ver, err := lk.ck.Get(lk.key)
		switch {
		case err == rpc.ErrNoKey:
			// Nobody has ever taken this lock; create the key. A
			// version of 0 makes the Put fail if another client
			// creates it first.
			if lk.ck.Put(lk.key, lk.id, 0) == rpc.OK {
				return
			}
		case val == lk.id:
			// We already hold the lock: an earlier Put did take
			// effect even though it reported ErrMaybe.
			return
		case val == free:
			// Claim it. Putting at ver fails with ErrVersion if
			// somebody else claimed it since our Get.
			if lk.ck.Put(lk.key, lk.id, ver) == rpc.OK {
				return
			}
		}
		// Either the lock is held by someone else, or our own attempt
		// lost the race (ErrVersion) or is of unknown outcome
		// (ErrMaybe). In every case the next Get settles it: if the
		// lock is ours, we will see our own id.
		time.Sleep(retryInterval)
	}
}

func (lk *Lock) Release() {
	val, ver, err := lk.ck.Get(lk.key)
	if err != rpc.OK || val != lk.id {
		// We don't hold the lock; nothing to release.
		return
	}
	// ErrMaybe here still means the release happened: while we hold
	// the lock no other client writes this key, so the ErrVersion that
	// produced ErrMaybe can only have been caused by our own Put
	// having already succeeded.
	lk.ck.Put(lk.key, free, ver)
}
