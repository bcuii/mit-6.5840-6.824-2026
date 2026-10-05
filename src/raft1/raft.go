package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.


import (
	"bytes"
	"math/rand"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)


// serverState is a peer's current role: follower, candidate, or leader.
type serverState int

const (
	follower serverState = iota
	candidate
	leader
)

// votedFor value meaning no vote has been cast in currentTerm.
const noVote = -1

const (
	// The tester limits leaders to tens of heartbeats per second, so the
	// election timeout must be well above the paper's 150-300ms.
	// ticker() only checks the deadline every 50-350ms, so the actual
	// timeout is 300-950ms.
	heartbeatInterval  = 100 * time.Millisecond
	electionTimeoutMin = 300 * time.Millisecond
	electionTimeoutMax = 600 * time.Millisecond
)

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int // candidateId that received vote in currentTerm, or noVote

	state            serverState
	electionDeadline time.Time // start an election if no leader is heard from by then

	// log[0] stands for the last entry covered by the snapshot (before
	// the first snapshot, a placeholder at index 0); only its Term is
	// used. log[i] holds the entry at log index snapshotIndex+i.
	log           []LogEntry
	snapshotIndex int    // log index of log[0]
	snapshot      []byte // most recent snapshot; covers log indexes 1..snapshotIndex

	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry sent on applyCh

	// leader only, reinitialized after each election.
	nextIndex  []int // for each peer, index of the next log entry to send it
	matchIndex []int // for each peer, index of highest log entry known to be replicated on it

	applyCh   chan raftapi.ApplyMsg
	applyCond *sync.Cond // signaled when commitIndex advances

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == leader
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var snapshotIndex int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&snapshotIndex) != nil ||
		d.Decode(&log) != nil {
		panic("readPersist: failed to decode persisted state")
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.snapshotIndex = snapshotIndex
	rf.log = log
	rf.snapshot = rf.persister.ReadSnapshot()
	// the service restores the snapshot itself when it starts, so only
	// entries after the snapshot are left to apply.
	rf.commitIndex = snapshotIndex
	rf.lastApplied = snapshotIndex
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// ignore a snapshot that is no newer than ours, e.g. one that raced
	// with an InstallSnapshot from the leader.
	if index <= rf.snapshotIndex {
		return
	}
	// the entry at index becomes the new log[0]. copy, so the memory of
	// the discarded entries can be freed.
	rf.log = append([]LogEntry(nil), rf.log[index-rf.snapshotIndex:]...)
	rf.log[0].Command = nil
	rf.snapshotIndex = index
	rf.snapshot = snapshot
	rf.persist()

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	// only vote for a candidate whose log is at least as up-to-date
	// as ours (§5.4.1).
	upToDate := args.LastLogTerm > rf.lastLogTerm() ||
		(args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.lastLogIndex())
	if (rf.votedFor == noVote || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.resetElectionTimer()
		reply.VoteGranted = true
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntries RPC arguments structure. Entries is empty for a
// heartbeat.
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int // index of the log entry immediately preceding Entries
	PrevLogTerm  int // term of the PrevLogIndex entry
	Entries      []LogEntry
	LeaderCommit int // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int
	Success bool // true if the follower's log matched PrevLogIndex and PrevLogTerm

	// set when Success is false because of a log mismatch, so the
	// leader can move nextIndex back past a whole term at once instead
	// of one entry at a time.
	LogTooShort   bool // the follower has no entry at PrevLogIndex
	LastLogIndex  int  // the follower's last log index; set if LogTooShort
	ConflictTerm  int  // term of the follower's entry at PrevLogIndex; set if !LogTooShort
	ConflictIndex int  // index of the follower's first entry with ConflictTerm; set if !LogTooShort
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	// the sender is the leader of our current term; a candidate in
	// the same term steps down.
	rf.state = follower
	rf.resetElectionTimer()

	// reject unless our log has an entry at PrevLogIndex whose term
	// matches PrevLogTerm, and tell the leader where the mismatch is.
	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.LogTooShort = true
		reply.LastLogIndex = rf.lastLogIndex()
		return
	}
	// entries up to snapshotIndex are in our snapshot. it only holds
	// committed entries, which always match the leader's, so there is
	// nothing to check there.
	if args.PrevLogIndex >= rf.snapshotIndex && rf.entry(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.entry(args.PrevLogIndex).Term
		// walk back to our first entry of ConflictTerm, but not into
		// the snapshot.
		reply.ConflictIndex = args.PrevLogIndex
		for reply.ConflictIndex-1 > rf.snapshotIndex && rf.entry(reply.ConflictIndex-1).Term == reply.ConflictTerm {
			reply.ConflictIndex--
		}
		return
	}

	// append the entries we don't already have. only truncate our log
	// at a conflicting entry: the RPC may be an old, delayed one that
	// is shorter than our log, and must not remove newer entries.
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		if index <= rf.snapshotIndex {
			continue // already in our snapshot
		}
		if index > rf.lastLogIndex() || rf.entry(index).Term != entry.Term {
			rf.log = append(rf.log[:index-rf.snapshotIndex], args.Entries[i:]...)
			rf.persist()
			break
		}
	}

	// the last index this RPC has verified to match the leader's log
	// ("index of last new entry" in Figure 2). don't commit past it.
	lastNewIndex := args.PrevLogIndex + len(args.Entries)
	commit := min(args.LeaderCommit, lastNewIndex)
	if commit > rf.commitIndex {
		rf.commitIndex = commit
		rf.applyCond.Signal()
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// InstallSnapshot RPC arguments structure (Figure 13). the leader
// sends the whole snapshot in one RPC, so there is no offset or done.
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int // the snapshot replaces all entries up to and including this index
	LastIncludedTerm  int // term of the LastIncludedIndex entry
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// InstallSnapshot RPC handler.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	// the sender is the leader of our current term; a candidate in
	// the same term steps down.
	rf.state = follower
	rf.resetElectionTimer()

	// ignore a snapshot that brings nothing new: every entry it covers
	// is already committed in our log or in our own snapshot.
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}
	if args.LastIncludedIndex <= rf.lastLogIndex() && rf.entry(args.LastIncludedIndex).Term == args.LastIncludedTerm {
		// our log has the snapshot's last entry: keep the entries after
		// it, and it becomes the new log[0].
		rf.log = append([]LogEntry(nil), rf.log[args.LastIncludedIndex-rf.snapshotIndex:]...)
		rf.log[0].Command = nil
	} else {
		// otherwise our whole log is outdated; discard it.
		rf.log = []LogEntry{{Term: args.LastIncludedTerm}}
	}
	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshot = args.Data
	rf.persist()

	// the snapshot only holds committed entries. lastApplied is now
	// behind snapshotIndex, so applier() hands the snapshot to the
	// service.
	rf.commitIndex = args.LastIncludedIndex
	rf.applyCond.Signal()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// startElection becomes a candidate for a new term and asks every
// peer for its vote. rf.mu must be held.
func (rf *Raft) startElection() {
	rf.state = candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	rf.resetElectionTimer()

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.lastLogTerm(),
	}
	votes := 1 // protected by rf.mu
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func() {
			reply := &RequestVoteReply{}
			if !rf.sendRequestVote(i, args, reply) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}
			// ignore replies to an election that is over or was
			// superseded by a later term.
			if rf.state != candidate || rf.currentTerm != args.Term {
				return
			}
			if !reply.VoteGranted {
				return
			}
			votes++
			if votes > len(rf.peers)/2 {
				rf.becomeLeader()
			}
		}()
	}
}

// rf.mu must be held.
func (rf *Raft) becomeLeader() {
	rf.state = leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = rf.lastLogIndex() + 1
	}
	go rf.heartbeater(rf.currentTerm)
}

// heartbeater sends heartbeats every heartbeatInterval for as long as
// this peer remains leader of term.
func (rf *Raft) heartbeater(term int) {
	for {
		rf.mu.Lock()
		if rf.state != leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		rf.broadcastAppendEntries()
		rf.mu.Unlock()

		time.Sleep(heartbeatInterval)
	}
}

// broadcastAppendEntries sends every peer the log entries it is
// missing; for a peer that is up to date, that is a heartbeat.
// rf.mu must be held.
func (rf *Raft) broadcastAppendEntries() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.sendEntriesTo(i)
	}
}

// sendEntriesTo sends server one AppendEntries carrying the entries
// from rf.nextIndex[server] onwards. if server rejects it, the reply
// handler below moves nextIndex[server] back (see
// nextIndexAfterMismatch) and calls sendEntriesTo again, so the leader
// keeps retrying from earlier entries until server accepts. if those
// entries have already been discarded into our snapshot, it sends the
// snapshot instead. rf.mu must be held.
func (rf *Raft) sendEntriesTo(server int) {
	prevLogIndex := rf.nextIndex[server] - 1
	if prevLogIndex < rf.snapshotIndex {
		rf.sendSnapshotTo(server)
		return
	}
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.entry(prevLogIndex).Term,
		// copy, since the RPC reads Entries after we release rf.mu.
		Entries:      append([]LogEntry(nil), rf.log[prevLogIndex+1-rf.snapshotIndex:]...),
		LeaderCommit: rf.commitIndex,
	}
	go func() {
		reply := &AppendEntriesReply{}
		if !rf.sendAppendEntries(server, args, reply) {
			return
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			return
		}
		// ignore replies to RPCs sent in an earlier term.
		if rf.state != leader || rf.currentTerm != args.Term {
			return
		}
		if reply.Success {
			// replies may arrive out of order; never move matchIndex
			// backwards.
			match := prevLogIndex + len(args.Entries)
			if match > rf.matchIndex[server] {
				rf.matchIndex[server] = match
				rf.nextIndex[server] = match + 1
				rf.advanceCommitIndex()
			}
			return
		}
		// the peer's log doesn't match at PrevLogIndex. act on this
		// reply only if it is not stale:
		if prevLogIndex == rf.nextIndex[server]-1 && // nextIndex hasn't moved since this request was built
			prevLogIndex > rf.matchIndex[server] { // the peer isn't already known to match at PrevLogIndex
			// move nextIndex back and send again. that request's reply
			// comes back here, so this repeats until the peer accepts.
			rf.nextIndex[server] = rf.nextIndexAfterMismatch(reply)
			rf.sendEntriesTo(server)
		}
	}()
}

// nextIndexAfterMismatch returns where to resume sending entries to a
// follower whose log didn't match at PrevLogIndex, skipping back a
// whole term at a time. rf.mu must be held.
func (rf *Raft) nextIndexAfterMismatch(reply *AppendEntriesReply) int {
	if reply.LogTooShort {
		return reply.LastLogIndex + 1
	}
	// if we also have entries from ConflictTerm, the follower's entries
	// of that term match ours up to our last one, so resume after it.
	for i := rf.lastLogIndex(); i > rf.snapshotIndex; i-- {
		if rf.entry(i).Term == reply.ConflictTerm {
			return i + 1
		}
	}
	// otherwise none of the follower's ConflictTerm entries are in our
	// log; resume at the first of them.
	return reply.ConflictIndex
}

// sendSnapshotTo sends server our snapshot, for when the entries it
// needs are no longer in our log. rf.mu must be held.
func (rf *Raft) sendSnapshotTo(server int) {
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.snapshotIndex,
		LastIncludedTerm:  rf.log[0].Term,
		Data:              rf.snapshot,
	}
	go func() {
		reply := &InstallSnapshotReply{}
		if !rf.sendInstallSnapshot(server, args, reply) {
			return
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			return
		}
		// ignore replies to RPCs sent in an earlier term.
		if rf.state != leader || rf.currentTerm != args.Term {
			return
		}
		// the peer now has everything up to LastIncludedIndex. replies
		// may arrive out of order; never move matchIndex backwards.
		if args.LastIncludedIndex > rf.matchIndex[server] {
			rf.matchIndex[server] = args.LastIncludedIndex
			rf.nextIndex[server] = args.LastIncludedIndex + 1
		}
	}()
}

// advanceCommitIndex commits the highest index replicated on a
// majority. rf.mu must be held.
func (rf *Raft) advanceCommitIndex() {
	for n := rf.lastLogIndex(); n > rf.commitIndex; n-- {
		// a leader only commits entries from its current term by
		// counting replicas; earlier ones are committed indirectly
		// (§5.4.2). terms never decrease along the log, so stop here.
		if rf.entry(n).Term != rf.currentTerm {
			return
		}
		count := 1 // ourselves
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= n {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			rf.applyCond.Signal()
			return
		}
	}
}

// applier sends each newly committed entry on applyCh, in log order.
// if a snapshot from the leader has replaced entries not yet applied,
// it sends that snapshot instead.
func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for {
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		var msg raftapi.ApplyMsg
		if rf.lastApplied < rf.snapshotIndex {
			msg = raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotTerm:  rf.log[0].Term,
				SnapshotIndex: rf.snapshotIndex,
			}
			rf.lastApplied = rf.snapshotIndex
		} else {
			rf.lastApplied++
			msg = raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.entry(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
			}
		}
		// don't hold rf.mu while the service is slow to read applyCh.
		rf.mu.Unlock()
		rf.applyCh <- msg
		rf.mu.Lock()
	}
}

// entry returns the log entry at index, which must be between
// snapshotIndex and lastLogIndex(). rf.mu must be held.
func (rf *Raft) entry(index int) LogEntry {
	return rf.log[index-rf.snapshotIndex]
}

// rf.mu must be held.
func (rf *Raft) lastLogIndex() int {
	return rf.snapshotIndex + len(rf.log) - 1
}

// rf.mu must be held.
func (rf *Raft) lastLogTerm() int {
	return rf.entry(rf.lastLogIndex()).Term
}

// becomeFollower adopts a newer term seen in an RPC request or reply.
// rf.mu must be held.
func (rf *Raft) becomeFollower(term int) {
	if rf.state == leader {
		// ticker() ignores electionDeadline while we are leader, so it
		// is long past; reset it, or we would start an election at once.
		rf.resetElectionTimer()
	}
	rf.state = follower
	rf.currentTerm = term
	rf.votedFor = noVote
	rf.persist()
}

// resetElectionTimer picks a new randomized election timeout.
// rf.mu must be held.
func (rf *Raft) resetElectionTimer() {
	spread := int64(electionTimeoutMax - electionTimeoutMin)
	timeout := electionTimeoutMin + time.Duration(rand.Int63n(spread))
	rf.electionDeadline = time.Now().Add(timeout)
}


// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	if rf.state != leader {
		isLeader = false
	} else {
		rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
		rf.persist()
		index = rf.lastLogIndex()
		term = rf.currentTerm
		// replicate now rather than waiting for the next heartbeat.
		rf.broadcastAppendEntries()
	}
	rf.mu.Unlock()


	return index, term, isLeader
}

func (rf *Raft) ticker() {
	for true {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.state != leader && time.Now().After(rf.electionDeadline) {
			rf.startElection()
		}
		rf.mu.Unlock()


		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.votedFor = noVote
	rf.state = follower
	rf.resetElectionTimer()
	rf.log = []LogEntry{{Term: 0}}
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	// start applier goroutine to send committed entries on applyCh
	go rf.applier()


	return rf
}
