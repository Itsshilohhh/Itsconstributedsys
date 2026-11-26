# Lab 2: Distributed Lock

## Goal

Implement a distributed lock on top of the Lab 2 key/value service. Multiple clients may contend for the same lock name, but at most one client should believe it owns that lock at a time.

## Representation

The lock is stored as a key/value pair:

- Key: the lock name.
- Value: an empty string means free; a non-empty value is the unique ID of the client that currently owns the lock.
- Version: every conditional `Put` uses the version returned by `Get`.

Each `Lock` instance generates its client ID once in `MakeLock`. The ID remains stable across retries, which lets the client recognize whether an operation whose reply was lost actually took effect.

## Acquire

1. Read the current value and version.
2. If the value is this client's ID, treat the lock as already acquired.
3. If the value is non-empty and belongs to another client, wait briefly and retry.
4. If the value is empty, attempt `Put(lockname, clientID, version)`.
5. On `OK`, acquisition succeeds.
6. On `ErrVersion`, another client changed the key first; read again and retry.
7. On `ErrMaybe`, the write may have reached the server even though its reply was lost. Read the key back:
   - If it contains this client's ID, acquisition succeeded.
   - Otherwise, retry.

The wait interval is randomized (10–30 ms) to reduce synchronized retries when several clients contend.

## Release

1. Read the current value and version.
2. If the value is not this client's ID, return without changing the key. This prevents releasing another client's lock.
3. If this client still owns it, attempt `Put(lockname, "", version)`.
4. On `OK`, release succeeds.
5. On an uncertain result (`ErrMaybe`), read again:
   - If the value is no longer this client's ID, the release has taken effect or ownership has changed; return.
   - If it is still this client's ID, retry the conditional release.

Release sets the value to the empty string; it does not delete the key.

## Why the version matters

The value check alone is not enough to safely update the lock. A client may read a free lock, pause, and then attempt a write after another client has already acquired and released it. The version check makes the write conditional on the exact state that was read, preventing a stale operation from overwriting a newer state (the ABA-style race).

## Missing-key behavior

The helper `read()` treats `ErrNoKey` as a free lock at version 0. This allows the first acquisition to create the key through the conditional `Put`.

## Validation

The implementation passed all six lock tests in the local test run on 2026-11-26:

- Reliable basic acquire/release
- Reliable nested acquisition
- One client on a reliable network
- Multiple clients on a reliable network
- One client on an unreliable network
- Multiple clients on an unreliable network

A race-detector run (`go test -race -v ./kvsrv1/lock`) is a separate follow-up check and is not claimed as passed here.

## Main implementation

See [lock.go](../../src/kvsrv1/lock/lock.go).
