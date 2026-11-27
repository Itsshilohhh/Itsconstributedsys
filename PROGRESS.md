# MIT 6.5840 Progress

Last updated: 2026-11-27

## Lab status

| Lab | Status | Notes |
| --- | --- | --- |
| Lab 1 — MapReduce | Completed | Completion confirmed by the learner on 2026-11-26. |
| Lab 2 — Key/Value Server | Completed | Completion confirmed by the learner on 2026-11-26. |
| Lab 3A — Raft Leader Election | In progress | Initial election test passes; heartbeat implementation and remaining 3A tests are still pending. |

## 2026-11-27 — Lab 3A checkpoint

### Implemented so far
- Added Raft peer election state: `currentTerm`, `votedFor`, role/state, election contact time, and randomized election timeout.
- Implemented `GetState()`.
- Implemented the `RequestVote` RPC handler, including term checks, one-vote-per-term behavior, and log freshness comparison.
- Implemented election initiation and concurrent `RequestVote` RPC fan-out with majority vote counting.
- Added ticker-based election timeout checks.

### Verification
- Built the test daemon locally: `go build -o main/raft1d ./main/raft1d.go`.
- Compile check passed: `go test -run '^$' ./raft1`.
- Test passed: `go test -run TestInitialElection3A -v ./raft1`.
  - Result: `PASS`
  - Test cluster: 3 peers
  - Runtime: approximately 3.38s
  - RPCs: 12

### Known follow-up
- The passing initial-election test printed: `warning: term changed even though there were no failures`.
- This may be caused by the absence of leader heartbeats, allowing followers to time out and start another election.
- Implement `AppendEntries` heartbeat handling and periodic leader heartbeats, then run the full Lab 3A test suite.
- Revisit election/ticker concurrency and persistence requirements as implementation progresses.

## Next step

Implement heartbeat-only `AppendEntries` for Lab 3A, reset follower election timers on valid leader contact, and verify stable leadership with the 3A tests.
