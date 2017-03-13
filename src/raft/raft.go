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
	"context"
	"encoding/gob"

	"sync"
	"time"

	"labrpc"
)

const (
	LEADER = iota
	CANDIDATE
	FOLLOWER

	MaxElectionTime = 300
	MinElectionTime = 150

	HeartBeatInterval time.Duration = 100 * time.Millisecond
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
	Term      int
	Index     int
	Committed bool
	Command   interface{}
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
	stopChan  chan struct{}

	electionTimer    *raftTimer
	stopElectionChan chan struct{}

	heartBeatTimer *raftTimer

	replInProgress []bool
	newLogChan     chan *LogEntry

	CurrentTerm int
	VotedFor    int
	Logs        []LogEntry

	firstIndex   int
	lastLogIndex int
	lastLogTerm  int
	commitIndex  int
	lastApplied  int

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

	if isLeader {
		DPrintf("%d is leader on term %d\n", rf.me, term)
	}
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
		rf.commitIndex = 0
		rf.lastApplied = 0
	} else {
		rf.firstIndex = rf.Logs[0].Index
		rf.lastLogIndex = rf.Logs[logCount-1].Index
		rf.lastLogTerm = rf.Logs[logCount-1].Term

		var log LogEntry
		for i := (logCount - 1); i >= 0; i-- {
			log = rf.Logs[i]
			if log.Committed {
				rf.commitIndex = log.Index
				rf.lastApplied = log.Index
				break
			}
		}
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
		rf.electionTimer.reset(0)
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.CurrentTerm > args.Term {
		DPrintf("%d(%d/%d/%d/%d) reject vote for %d on term %d\n",
			rf.me, rf.CurrentTerm, rf.VotedFor, rf.lastLogIndex, rf.lastLogTerm, args.CandidateID, args.Term)
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	} else if rf.CurrentTerm < args.Term {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.persist()

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

		rf.electionTimer.reset(0)
		return
	}

	DPrintf("%d(%d/%d/%d/%d) reject vote for %d on term %d, request last log %d/%d\n",
		rf.me, rf.CurrentTerm, rf.VotedFor, rf.lastLogIndex, rf.lastLogTerm,
		args.CandidateID, args.Term, args.LastLogIndex, args.LastLogTerm)
	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false
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
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArg, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.LeaderID == rf.VotedFor {
		rf.electionTimer.reset(0)
	}

	DPrintf("%d receive append log RPC from %d on term %d (my team is %d)\n", rf.me, args.LeaderID, args.Term, rf.CurrentTerm)
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = args.LeaderID
		rf.persist()

		if rf.role == CANDIDATE {
			rf.changeRole(CANDIDATE, FOLLOWER)
		} else if rf.role == LEADER {
			rf.changeRole(LEADER, FOLLOWER)
		}
	} else if args.Term < rf.CurrentTerm {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		return
	}

	if (len(args.Entries) == 0) || (len(rf.Logs) < args.PrevLogIndex) {
		reply.Success = false
		reply.Term = rf.CurrentTerm
	} else {
		if args.PrevLogIndex == 0 {
			rf.Logs = args.Entries
			rf.persist()

			lastLog := rf.Logs[len(rf.Logs)-1]
			rf.lastLogIndex = lastLog.Index
			rf.lastLogTerm = lastLog.Term
			DPrintf("(first)%d accept %d entries from %d/%d, the first index is %d, update last log index to %d\n",
				rf.me, len(args.Entries), args.LeaderID, args.Term, args.Entries[0].Index, rf.lastLogIndex)

			reply.Success = true
			reply.Term = rf.CurrentTerm
		} else {
			log := rf.Logs[args.PrevLogIndex-1]
			if log.Term != args.PrevLogTerm {
				reply.Success = false
				reply.Term = rf.CurrentTerm
				return
			} else {
				rf.Logs = append(rf.Logs[:args.PrevLogIndex], args.Entries...)
				rf.persist()

				lastLog := rf.Logs[len(rf.Logs)-1]
				rf.lastLogIndex = lastLog.Index
				rf.lastLogTerm = lastLog.Term
				DPrintf("%d accept %d entries from %d/%d, the first index is %d, update last log index to %d\n",
					rf.me, len(args.Entries), args.LeaderID, args.Term, args.Entries[0].Index, rf.lastLogIndex)

				reply.Success = true
				reply.Term = rf.CurrentTerm
			}
		}
	}

	// not directly commit previous term log
	if (args.LeaderCommit != 0) && (len(rf.Logs) != 0) {
		lastCommitIndex := len(rf.Logs)
		if lastCommitIndex > args.LeaderCommit {
			lastCommitIndex = args.LeaderCommit
		}
		if rf.Logs[lastCommitIndex-1].Term == rf.CurrentTerm {
			for i := rf.commitIndex; i < lastCommitIndex; i++ {
				rf.Logs[i].Committed = true
				rf.commitIndex = i + 1
				rf.persist()
				DPrintf("%d commit log %d/%d/%d on index %d\n",
					rf.me, rf.Logs[i].Index, rf.Logs[i].Term, rf.Logs[i].Command.(int), i+1)

				rf.applyChan <- ApplyMsg{Index: rf.Logs[i].Index, Command: rf.Logs[i].Command}
			}
		}
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
	heartBeatArg := AppendEntriesArg{
		Term:         rf.CurrentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: rf.commitIndex}
	rf.mu.Unlock()

	asyncCall := func(server int) {
		reply := AppendEntriesReply{}
		ok := rf.peers[server].Call("Raft.AppendEntries", heartBeatArg, &reply)
		if ok {
			rf.mu.Lock()
			if reply.Term > rf.CurrentTerm {
				rf.CurrentTerm = reply.Term
				rf.VotedFor = server
				if rf.role == LEADER {
					rf.changeRole(LEADER, FOLLOWER)
				}
			}
			rf.mu.Unlock()
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
	rf.mu.Lock()
	if rf.replInProgress[server] {
		rf.mu.Unlock()
		return
	} else {
		rf.replInProgress[server] = true
	}
	rf.mu.Unlock()

	var startIndex int
	var args AppendEntriesArg
	var reply AppendEntriesReply
	for {
		rf.mu.Lock()
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

		rf.mu.Lock()
		//DPrintf("%d receive reply from %d on term %d, role %d, result %t\n",
		//	rf.me, server, args.Term, rf.role, ok)
		if rf.role != LEADER {
			rf.replInProgress[server] = false
			rf.mu.Unlock()
			return
		}

		if ok {
			if reply.Success {
				var replicas int

				for _, entry := range args.Entries {
					rf.matchIndex[server] = entry.Index
					rf.nextIndex[server] = entry.Index + 1
					if entry.Index <= rf.commitIndex {
						continue
					}

					replicas = 1
					for _, matchIndex := range rf.matchIndex {
						if matchIndex >= entry.Index {
							replicas++
						}
					}
					if (replicas > len(rf.peers)/2) && (entry.Term == rf.CurrentTerm) {
						if rf.commitIndex != entry.Index-1 {
							// commit previous team's log
							for i := rf.commitIndex; i < (entry.Index - 1); i++ {
								DPrintf("%d commit previous team entry %d\n", rf.me, rf.Logs[i].Index)
								rf.Logs[i].Committed = true
								rf.persist()
								rf.applyChan <- ApplyMsg{Index: rf.Logs[i].Index, Command: rf.Logs[i].Command}
							}
						}

						DPrintf("%d decide to commit entry %d\n", rf.me, entry.Index)
						rf.commitIndex = entry.Index
						rf.Logs[entry.Index-1].Committed = true
						rf.persist()
						rf.applyChan <- ApplyMsg{Index: entry.Index, Command: entry.Command}
					}
				}
				if rf.nextIndex[server] > rf.lastLogIndex {
					rf.replInProgress[server] = false
					rf.mu.Unlock()
					return
				}
			} else {
				if (rf.CurrentTerm < reply.Term) && (rf.role == LEADER) {
					rf.CurrentTerm = reply.Term
					rf.VotedFor = server
					rf.persist()
					rf.changeRole(LEADER, FOLLOWER)
					rf.mu.Unlock()
					return
				} else {
					rf.nextIndex[server]--
				}
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) startElection() bool {
	rf.mu.Lock()
	if rf.role == FOLLOWER {
		rf.role = CANDIDATE
	} else if rf.role == LEADER {
		return true
	}
	rf.CurrentTerm += 1
	rf.VotedFor = rf.me
	rf.persist()
	DPrintf("%d start election on term %d\n", rf.me, rf.CurrentTerm)

	reqVoteArgs := RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.lastLogIndex,
		LastLogTerm:  rf.lastLogTerm}
	rf.mu.Unlock()

	var lock sync.Mutex
	votedNum := 1
	win := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())

	asyncCall := func(server int, votedNum *int) {
		var ok bool
		reply := RequestVoteReply{}
		c := make(chan bool, 1)
		go func() {
			c <- rf.sendRequestVote(server, reqVoteArgs, &reply)
		}()
		select {
		case ok = <-c:
		case <-ctx.Done():
			return
		}
		if ok {
			if reply.VoteGranted {
				lock.Lock()
				(*votedNum)++
				if (*votedNum) > len(rf.peers)/2 {
					win <- struct{}{}
				}
				lock.Unlock()
			} else {
				rf.mu.Lock()
				if rf.CurrentTerm < reply.Term {
					rf.CurrentTerm = reply.Term
					rf.VotedFor = server

					rf.role = FOLLOWER
					rf.stopElectionChan <- struct{}{}
				}
				rf.mu.Unlock()
			}
		}
	}

	rf.electionTimer.reset(0)
	for index, _ := range rf.peers {
		if index == rf.me {
			continue
		}
		go asyncCall(index, &votedNum)
	}

	select {
	case <-win:
		DPrintf("%d win election on term %d\n", rf.me, rf.CurrentTerm)
		rf.electionTimer.stop()
		cancel()
	case <-rf.stopElectionChan:
		rf.electionTimer.stop()
		cancel()
		return false
	case <-rf.electionTimer.t.C:
		//DPrintf("%d election timeout on term %d\n", rf.me, rf.CurrentTerm)
		rf.mu.Lock()
		if rf.role == CANDIDATE {
			rf.VotedFor = -1
		}
		rf.mu.Unlock()
		cancel()
		return false
	}

	rf.mu.Lock()
	rf.changeRole(CANDIDATE, LEADER)
	rf.mu.Unlock()
	rf.sendHeartBeat()
	return true
}

func (rf *Raft) run() {
	rf.electionTimer.reset(0)
	for {
		select {
		case <-rf.electionTimer.t.C:
			success := rf.startElection()

			rf.mu.Lock()
			if rf.role == LEADER {
				rf.heartBeatTimer.reset(0)
			}
			rf.mu.Unlock()

			if !success {
				electionTime := time.Duration(randomInt(MinElectionTime, MaxElectionTime)) * time.Millisecond
				rf.electionTimer.reset(electionTime)
			}
		case <-rf.heartBeatTimer.t.C:
			rf.mu.Lock()
			if rf.role == LEADER {
				rf.heartBeatTimer.reset(0)
			}
			rf.mu.Unlock()

			rf.sendHeartBeat()
		case <-rf.stopChan:
			return
		case <-rf.stopElectionChan:
			// consume notification to avoid affect next round election
			continue
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
	rf.mu.Lock()

	if rf.role != LEADER {
		rf.mu.Unlock()
		return -1, 0, false
	}

	index := rf.lastLogIndex + 1
	term := rf.CurrentTerm
	log := LogEntry{Index: index, Term: term, Committed: false, Command: command}
	rf.Logs = append(rf.Logs, log)
	rf.lastLogIndex++
	rf.lastLogTerm = rf.CurrentTerm
	rf.persist()
	DPrintf("%d get new log %d/%d/%d\n", rf.me, index, term, command.(int))
	for index, _ := range rf.peers {
		if index == rf.me {
			continue
		}
		if !rf.replInProgress[index] {
			go rf.replicateLog(index)
		}
	}
	rf.mu.Unlock()

	return index, term, true
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
	close(rf.stopElectionChan)
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

	rf.heartBeatTimer = newRaftTimer(HeartBeatInterval)
	electionTime := time.Duration(randomInt(MinElectionTime, MaxElectionTime)) * time.Millisecond
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
