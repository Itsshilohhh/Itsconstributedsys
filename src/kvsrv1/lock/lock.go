package lock

import (
	"math/rand"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

// A Lock stores its state in the key/value server under the lock's
// name: the empty string means the lock is free, and any other string
// is the unique identifier of the client that currently holds it.
//
// Every write is a conditional Put using the version just read by Get.
// The version check is what makes the lock safe: when several clients
// race to take a free lock, each Put carries the same version, so the
// server applies exactly one of them and answers ErrVersion to the
// rest.  Likewise, Release only succeeds if no one else has changed the
// lock since the Get, so a client can never clobber another client's
// ownership.

const (
	// Time to wait before retrying while the lock is held by someone
	// else or after losing a race; randomized to avoid a thundering herd.
	minWait = 10 * time.Millisecond
	maxWait = 30 * time.Millisecond
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk

	lockname string
	// id identifies this lock user among all clients competing for the
	// same lock.
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
		ck:       ck,
		lockname: lockname,
		id:       kvtest.RandValue(8),
	}
	return lk
}

// read returns the lock's current value and version.  A missing key is
// the same as a free lock whose version is 0.
func (lk *Lock) read() (string, rpc.Tversion) {
	for {
		val, ver, err := lk.ck.Get(lk.lockname)
		switch err {
		case rpc.OK:
			return val, ver
		case rpc.ErrNoKey:
			return "", 0
		}
		// Clerk.Get only returns OK or ErrNoKey, but be defensive.
		time.Sleep(waitTime())
	}
}

func waitTime() time.Duration {
	return minWait + time.Duration(rand.Int63n(int64(maxWait-minWait)))
}

func (lk *Lock) Acquire() {
	for {
		val, ver := lk.read()
		if val == lk.id {
			// We already hold it.  This can happen if an earlier Put was
			// applied but its reply was lost.
			return
		}
		if val != "" {
			// Held by another client; wait and look again.
			time.Sleep(waitTime())
			continue
		}

		switch err := lk.ck.Put(lk.lockname, lk.id, ver); err {
		case rpc.OK:
			return
		case rpc.ErrMaybe:
			// The Put may have been applied.  Since nobody else can write
			// the lock while we hold it, reading it back tells us whether
			// we won.
			if v, _ := lk.read(); v == lk.id {
				return
			}
		}
		// ErrVersion: another client took the free lock first, so start
		// over and look again.
		time.Sleep(waitTime())
	}
}

func (lk *Lock) Release() {
	for {
		val, ver := lk.read()
		if val != lk.id {
			// We no longer own the lock.  Either the release below was
			// already applied (its reply was lost), or somebody acquired
			// the lock after our release.
			return
		}

		switch err := lk.ck.Put(lk.lockname, "", ver); err {
		case rpc.OK:
			return
		case rpc.ErrMaybe:
			// The release may already have been applied; loop and let
			// read() decide.  A re-sent Put can also come back ErrVersion
			// because someone acquired the lock after our release.
		}
	}
}
