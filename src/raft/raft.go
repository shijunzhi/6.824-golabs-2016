package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	"sync"
	"time"

	"labrpc"
)

const (
	LEADER = iota
	CANDIDATE
	FOLLOWER

	MaxElectionTime = 500
	MinElectionTime = 300

	HeartBeatInterval time.Duration = 150 * time.Millisecond

	MaxChanLen = 1000
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role      int
	applyChan chan ApplyMsg

	stopChan chan struct{}

	electionTimer    *raftTimer
	stopElectionChan chan struct{}

	heartBeatTimer *raftTimer

	replInProgress []bool
	applyLogChan   chan int

	CurrentTerm int
	VotedFor    int
	commitIndex int
	lastApplied int
	Logs        []LogEntry

	firstIndex   int
	lastLogIndex int
	lastLogTerm  int

	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here.
	rf.mu.Lock()
	term := rf.CurrentTerm
	isLeader := (rf.role == LEADER)
	rf.mu.Unlock()

	//if isLeader {
	//	DPrintf("%d is leader on term %d\n", rf.me, term)
	//}
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	buf := new(bytes.Buffer)
	encoder := gob.NewEncoder(buf)
	encoder.Encode(rf.CurrentTerm)
	encoder.Encode(rf.VotedFor)
	encoder.Encode(rf.Logs)
	data := buf.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	rf.commitIndex = 0
	rf.lastApplied = 0
	if len(data) == 0 {
		rf.CurrentTerm = 0
		rf.VotedFor = -1
		rf.Logs = []LogEntry{}
	} else {
		reader := bytes.NewBuffer(data)
		decoder := gob.NewDecoder(reader)
		decoder.Decode(&rf.CurrentTerm)
		decoder.Decode(&rf.VotedFor)
		decoder.Decode(&rf.Logs)
	}

	logCount := len(rf.Logs)
	if logCount == 0 {
		rf.firstIndex = 1
		rf.lastLogIndex = 0
		rf.lastLogTerm = 0
	} else {
		rf.firstIndex = rf.Logs[0].Index
		rf.lastLogIndex = rf.Logs[logCount-1].Index
		rf.lastLogTerm = rf.Logs[logCount-1].Term
	}

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for index, _ := range rf.peers {
		rf.nextIndex[index] = rf.lastLogIndex + 1
		rf.matchIndex[index] = 0
	}
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

func (rf *Raft) changeRole(old int, new int) {
	if (old == CANDIDATE) && (new == FOLLOWER) {
		rf.role = FOLLOWER
		rf.stopElectionChan <- struct{}{}
	} else if (old == CANDIDATE) && (new == LEADER) {
		rf.role = LEADER
		for index, _ := range rf.peers {
			rf.nextIndex[index] = rf.lastLogIndex + 1
			rf.matchIndex[index] = 0
		}
	} else if (old == LEADER) && (new == FOLLOWER) {
		rf.role = FOLLOWER
		rf.heartBeatTimer.stop()
		rf.electionTimer.reset()
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	stateChanged := false

	rf.mu.Lock()
	if rf.CurrentTerm > args.Term {
		DPrintf("%d(%d/%d/%d/%d) reject vote for %d on term %d\n",
			rf.me, rf.CurrentTerm, rf.VotedFor, rf.lastLogIndex, rf.lastLogTerm, args.CandidateID, args.Term)
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	} else if rf.CurrentTerm < args.Term {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		stateChanged = true

		if rf.role == CANDIDATE {
			rf.changeRole(CANDIDATE, FOLLOWER)
		} else if rf.role == LEADER {
			rf.changeRole(LEADER, FOLLOWER)
		}
	}

	if ((rf.VotedFor == -1) || (rf.VotedFor == args.CandidateID)) &&
		((rf.lastLogTerm < args.LastLogTerm) ||
			((rf.lastLogTerm == args.LastLogTerm) && (rf.lastLogIndex <= args.LastLogIndex))) {
		DPrintf("%d(%d/%d/%d/%d) vote %d on term %d\n",
			rf.me, rf.CurrentTerm, rf.VotedFor, rf.lastLogIndex, rf.lastLogTerm, args.CandidateID, args.Term)
		rf.CurrentTerm = args.Term
		rf.VotedFor = args.CandidateID
		rf.persist()

		reply.Term = rf.CurrentTerm
		reply.VoteGranted = true
		rf.mu.Unlock()

		rf.electionTimer.reset()
		return
	}

	DPrintf("%d(%d/%d/%d/%d) reject vote for %d on term %d, request last log %d/%d\n",
		rf.me, rf.CurrentTerm, rf.VotedFor, rf.lastLogIndex, rf.lastLogTerm,
		args.CandidateID, args.Term, args.LastLogIndex, args.LastLogTerm)
	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false
	if stateChanged {
		rf.persist()
	}
	rf.mu.Unlock()
	return
}

type AppendEntriesArg struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args AppendEntriesArg, reply *AppendEntriesReply) {
	stateChanged := false

	rf.mu.Lock()
	defer rf.mu.Unlock()

	defer func() {
		if stateChanged {
			rf.persist()
		}
	}()

	if (args.LeaderID == rf.VotedFor) && (args.Term == rf.CurrentTerm) {
		rf.electionTimer.reset()
	}

	if len(args.Entries) != 0 {
		DPrintf("%d receive append log RPC from %d on term %d (my team is %d/%d), log len %d, first log %d/%d\n",
			rf.me, args.LeaderID, args.Term, rf.CurrentTerm, rf.VotedFor, len(args.Entries), args.Entries[0].Index, args.Entries[0].Term)
	} else {
		DPrintf("%d receive heart beat from %d on term %d (my team is %d/%d)\n",
			rf.me, args.LeaderID, args.Term, rf.CurrentTerm, rf.VotedFor)
	}

	if (args.Term > rf.CurrentTerm) || ((args.Term == rf.CurrentTerm) && (rf.VotedFor == -1)) {
		rf.CurrentTerm = args.Term
		rf.VotedFor = args.LeaderID

		stateChanged = true

		if rf.role == CANDIDATE {
			rf.changeRole(CANDIDATE, FOLLOWER)
			rf.electionTimer.reset()
		} else if rf.role == LEADER {
			rf.changeRole(LEADER, FOLLOWER)
		} else {
			rf.electionTimer.reset()
		}
	} else if args.Term < rf.CurrentTerm {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		reply.ConflictIndex = 0
		reply.ConflictTerm = 0
		return
	}

	if args.PrevLogIndex != 0 {
		if len(rf.Logs) < args.PrevLogIndex {
			reply.Success = false
			reply.Term = rf.CurrentTerm
			reply.ConflictIndex = len(rf.Logs)
			reply.ConflictTerm = 0
			DPrintf("%d not have %d log, conflict index %d\n",
				rf.me, args.PrevLogIndex, reply.ConflictIndex)
			return
		} else {
			prevLog := rf.Logs[args.PrevLogIndex-1]
			if prevLog.Term != args.PrevLogTerm {
				reply.Success = false
				reply.Term = rf.CurrentTerm
				reply.ConflictTerm = prevLog.Term
				for i := args.PrevLogIndex - 1; i >= 0; i-- {
					if rf.Logs[i].Term != prevLog.Term {
						reply.ConflictIndex = i + 2
						break
					}
				}
				DPrintf("%d have %d log but term different %d/%d, conflict index %d term %d\n",
					rf.me, args.PrevLogIndex, prevLog.Term, args.PrevLogTerm, reply.ConflictIndex, reply.ConflictTerm)
				return
			}
		}
	}

	for index, entry := range args.Entries {
		if entry.Index > len(rf.Logs) {
			rf.Logs = append(rf.Logs, args.Entries[index:]...)
			stateChanged = true
			break
		} else {
			if rf.Logs[entry.Index-1].Term != entry.Term {
				rf.Logs = append(rf.Logs[:entry.Index-1], args.Entries[index:]...)
				stateChanged = true
				break
			}
		}
	}

	if len(rf.Logs) != 0 {
		lastLog := rf.Logs[len(rf.Logs)-1]
		rf.lastLogIndex = lastLog.Index
		rf.lastLogTerm = lastLog.Term
	}

	if len(args.Entries) != 0 {
		lastAcceptLog := args.Entries[len(args.Entries)-1]
		DPrintf("%d accept %d entries from %d/%d, the index is %d(first)/%d(last), update last log index to %d\n",
			rf.me, len(args.Entries), args.LeaderID, args.Term, args.Entries[0].Index, lastAcceptLog.Index, rf.lastLogIndex)
	}

	reply.Success = true
	reply.Term = rf.CurrentTerm
	reply.ConflictTerm = 0
	reply.ConflictIndex = 0

	if args.LeaderCommit > rf.commitIndex {
		var lastNewLogIndex int
		if len(args.Entries) == 0 {
			lastNewLogIndex = args.PrevLogIndex
		} else {
			lastNewLogIndex = args.Entries[len(args.Entries)-1].Index
		}
		if lastNewLogIndex < args.LeaderCommit {
			rf.commitIndex = lastNewLogIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		//DPrintf("%d commit index is %d\n", rf.me, rf.commitIndex)

		rf.applyLogChan <- rf.commitIndex
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendHeartBeat() {
	rf.mu.Lock()
	if rf.role != LEADER {
		rf.mu.Unlock()
		return
	}
	heartBeatArg := AppendEntriesArg{
		Term:         rf.CurrentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: rf.lastLogIndex,
		PrevLogTerm:  rf.lastLogTerm,
		Entries:      []LogEntry{},
		LeaderCommit: rf.commitIndex}
	rf.mu.Unlock()

	asyncCall := func(server int) {
		reply := AppendEntriesReply{}
		ok := rf.peers[server].Call("Raft.AppendEntries", heartBeatArg, &reply)
		if !ok {
			return
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.CurrentTerm {
			rf.CurrentTerm = reply.Term
			rf.VotedFor = -1

			if rf.role == LEADER {
				rf.changeRole(LEADER, FOLLOWER)
			} else if rf.role == CANDIDATE {
				rf.changeRole(CANDIDATE, FOLLOWER)
			}
		} else if !reply.Success && rf.role == LEADER {
			// sync log to follower
			if !rf.replInProgress[server] {
				rf.replInProgress[server] = true
				go rf.replicateLog(server)
			}
		}
	}
	for index, _ := range rf.peers {
		if index == rf.me {
			continue
		}
		go asyncCall(index)
	}
}

func (rf *Raft) replicateLog(server int) {
	var startIndex int
	var args AppendEntriesArg
	var reply AppendEntriesReply
	var replicas int

	rf.mu.Lock()
	for {
		if rf.role != LEADER {
			rf.replInProgress[server] = false
			rf.mu.Unlock()
			return
		}
		startIndex = rf.nextIndex[server] - 1
		args = AppendEntriesArg{
			Term:         rf.CurrentTerm,
			LeaderID:     rf.me,
			Entries:      rf.Logs[startIndex:],
			LeaderCommit: rf.commitIndex}
		if (len(rf.Logs) == 0) || (startIndex == 0) {
			args.PrevLogIndex = 0
			args.PrevLogTerm = 0
		} else {
			prevLog := rf.Logs[startIndex-1]
			args.PrevLogIndex = prevLog.Index
			args.PrevLogTerm = prevLog.Term
		}
		rf.mu.Unlock()

		ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply)
		if !ok {
			rf.mu.Lock()
			rf.replInProgress[server] = false
			rf.mu.Unlock()
			return
		}

		rf.mu.Lock()
		if rf.CurrentTerm < reply.Term {
			rf.CurrentTerm = reply.Term
			rf.VotedFor = -1
			rf.persist()

			if rf.role == LEADER {
				rf.changeRole(LEADER, FOLLOWER)
			} else if rf.role == CANDIDATE {
				rf.changeRole(CANDIDATE, FOLLOWER)
			}
			rf.replInProgress[server] = false
			rf.mu.Unlock()
			return
		}

		if rf.CurrentTerm != args.Term {
			if rf.role != LEADER {
				rf.replInProgress[server] = false
				rf.mu.Unlock()
				return
			} else {
				continue
			}
		}

		if reply.Success {
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1

			for _, entry := range args.Entries {
				if entry.Index <= rf.commitIndex {
					continue
				}

				replicas = 1
				for _, matchIndex := range rf.matchIndex {
					if matchIndex >= entry.Index {
						replicas++
					}
				}
				if replicas <= len(rf.peers)/2 {
					break
				} else if entry.Term == rf.CurrentTerm {
					rf.commitIndex = entry.Index
					rf.applyLogChan <- rf.commitIndex
				}
			}
			if rf.nextIndex[server] > rf.lastLogIndex {
				rf.replInProgress[server] = false
				rf.mu.Unlock()
				return
			}
		} else {
			rf.decideNextIndex(server, &reply)
		}
	}
}

func (rf *Raft) decideNextIndex(server int, reply *AppendEntriesReply) {
	if reply.ConflictTerm != 0 {
		index := -1
		for i := 0; i < len(rf.Logs); i++ {
			if rf.Logs[i].Term == reply.ConflictTerm {
				index = i
				continue
			} else if index != -1 {
				break
			}
		}
		if index == -1 {
			rf.nextIndex[server] = reply.ConflictIndex
		} else {
			rf.nextIndex[server] = index + 1
		}
	} else {
		if reply.ConflictIndex != 0 {
			rf.nextIndex[server] = reply.ConflictIndex
		} else {
			rf.nextIndex[server] = 1
		}
	}
	DPrintf("%d decide next index is %d for %d\n", rf.me, rf.nextIndex[server], server)
}

func (rf *Raft) applyLog() {
	for {
		select {
		case commitIndex := <-rf.applyLogChan:
			for rf.lastApplied < commitIndex {
				applyLog := rf.Logs[rf.lastApplied]
				//DPrintf("%d apply log %d/%d/%d\n", rf.me, applyLog.Index, applyLog.Term, applyLog.Command.(int))
				rf.applyChan <- ApplyMsg{Index: applyLog.Index, Command: applyLog.Command}
				rf.lastApplied++
			}
		case <-rf.stopChan:
			return
		}
	}
}

func (rf *Raft) electLeader() {
	var votedCount int
	var reqVoteReply RequestVoteReply

	rf.mu.Lock()
	if rf.role == FOLLOWER {
		rf.role = CANDIDATE
	} else {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	for {
		rf.mu.Lock()
		if rf.role != CANDIDATE {
			rf.mu.Unlock()
			return
		}

		rf.CurrentTerm += 1
		rf.VotedFor = rf.me
		rf.persist()

		reqVoteArgs := RequestVoteArgs{
			Term:         rf.CurrentTerm,
			CandidateID:  rf.me,
			LastLogIndex: rf.lastLogIndex,
			LastLogTerm:  rf.lastLogTerm}
		rf.mu.Unlock()

		DPrintf("%d start election on term %d/%d/%d\n",
			rf.me, reqVoteArgs.Term, reqVoteArgs.LastLogIndex, reqVoteArgs.LastLogTerm)

		votedCount = 1
		replyChan := make(chan RequestVoteReply, len(rf.peers))

		rf.electionTimer.reset()
		for index, _ := range rf.peers {
			if index == rf.me {
				continue
			}
			go func(server int) {
				var reply RequestVoteReply
				ok := rf.sendRequestVote(server, reqVoteArgs, &reply)
				if ok {
					replyChan <- reply
				}
			}(index)
		}

	waitVoteResult:
		for {
			select {
			case reqVoteReply = <-replyChan:
				rf.mu.Lock()
				if rf.CurrentTerm < reqVoteReply.Term {
					rf.CurrentTerm = reqVoteReply.Term
					rf.VotedFor = -1

					if rf.role == CANDIDATE {
						rf.role = FOLLOWER
					}
					rf.mu.Unlock()
					return
				}

				if reqVoteArgs.Term != rf.CurrentTerm {
					rf.mu.Unlock()
					continue
				}

				if reqVoteReply.VoteGranted {
					votedCount++
					if votedCount > len(rf.peers)/2 {
						rf.electionTimer.stop()

						rf.changeRole(CANDIDATE, LEADER)
						rf.mu.Unlock()

						DPrintf("%d win election on term %d\n", rf.me, reqVoteArgs.Term)
						rf.heartBeatTimer.reset()
						rf.sendHeartBeat()
						return
					}
				}
				rf.mu.Unlock()
			case <-rf.stopElectionChan:
				return
			case <-rf.electionTimer.t.C:
				//DPrintf("%d election timeout on term %d\n", rf.me, reqVoteArgs.Term)
				break waitVoteResult
			}
		}
	}
}

func (rf *Raft) run() {
	go rf.applyLog()
	rf.electionTimer.reset()
	for {
		select {
		case <-rf.electionTimer.t.C:
			rf.electLeader()
		case <-rf.heartBeatTimer.t.C:
			rf.mu.Lock()
			if rf.role == LEADER {
				rf.heartBeatTimer.reset()
			}
			rf.mu.Unlock()
			rf.sendHeartBeat()
		case <-rf.stopChan:
			return
		}
	}
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	if rf.role != LEADER {
		return -1, 0, false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	log := LogEntry{Index: rf.lastLogIndex + 1, Term: rf.CurrentTerm, Command: command}
	rf.Logs = append(rf.Logs, log)
	rf.lastLogIndex++
	rf.lastLogTerm = rf.CurrentTerm
	rf.persist()

	DPrintf("%d get new log %d/%d/%d\n", rf.me, log.Index, log.Term, log.Command.(int))

	for index, _ := range rf.peers {
		if index == rf.me {
			continue
		}
		if !rf.replInProgress[index] {
			rf.replInProgress[index] = true
			go rf.replicateLog(index)
		}
	}

	return log.Index, log.Term, true
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	close(rf.stopChan)

	rf.persist()
	rf.mu.Unlock()

}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}

	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyChan = applyCh

	// Your initialization code here.
	rf.role = FOLLOWER
	rf.stopChan = make(chan struct{}, 1)
	rf.stopElectionChan = make(chan struct{}, 1)
	rf.applyLogChan = make(chan int, MaxChanLen)

	rf.heartBeatTimer = newRaftTimer(HeartBeatInterval)
	electionTime := time.Duration(randomInt(MinElectionTime, MaxElectionTime)) * time.Millisecond
	//DPrintf("%d election timeout is %d\n", rf.me, electionTime)
	rf.electionTimer = newRaftTimer(electionTime)

	rf.replInProgress = make([]bool, len(peers))
	for index, _ := range rf.replInProgress {
		rf.replInProgress[index] = false
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.run()

	return rf
}
