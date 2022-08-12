
## Bugs

### Not able to start cluster with empty state machine
- Clean everything for node 0 and 1
- Start node 0 and bootstrap it
- Start node 1 
- Add learner 1 by calling api in node 0
- Add member 1by calling on node 0
- Error while doing log compaction and last applied log index = 0 , but not present in raf log