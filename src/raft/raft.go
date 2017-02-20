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
	"fmt"
	"math/rand"
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
	role int

	electionTimeout  time.Duration
	electionTimer    *time.Timer
	stopElectionChan chan struct{}

	heartBeatTimer *time.Timer

	CurrentTerm int
	VotedFor    int
	Logs        []LogEntry

	lastLogIndex int
	lastLogTerm  int
	prevLogIndex int
	prevLogTerm  int
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
		fmt.Printf("%d is leader on term %d\n", rf.me, term)
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
		return
	}
	reader := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(reader)
	decoder.Decode(&rf.CurrentTerm)
	decoder.Decode(&rf.VotedFor)
	decoder.Decode(&rf.Logs)

	logCount := len(rf.Logs)
	if logCount == 0 {
		return
	} else {
		rf.lastLogTerm = rf.Logs[logCount-1].Term
		rf.lastLogIndex = rf.Logs[logCount-1].Index
		rf.prevLogIndex = rf.lastLogIndex
		rf.prevLogTerm = rf.lastLogTerm
	}

	var log LogEntry
	for i := (logCount - 1); i >= 0; i-- {
		log = rf.Logs[i]
		if log.Committed {
			rf.commitIndex = log.Index
			rf.lastApplied = log.Index
			break
		}
	}
	for index, _ := range rf.peers {
		rf.nextIndex[index] = rf.lastLogIndex + 1
		rf.matchIndex = 0
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.CurrentTerm > args.Term {
		fmt.Printf("%d(%d/%d/%d/%d) reject vote for %d on term %d\n",
			rf.me, rf.CurrentTerm, rf.VotedFor, rf.lastLogIndex, rf.lastLogTerm, args.CandidateID, args.Term)
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	} else if (rf.CurrentTerm < args.Term) &&
		((rf.lastLogTerm < args.LastLogTerm) ||
			((rf.lastLogTerm == args.LastLogTerm) && (rf.lastLogIndex <= args.LastLogIndex))) {
		fmt.Printf("%d(%d/%d/%d/%d) vote %d on term %d\n",
			rf.me, rf.CurrentTerm, rf.VotedFor, rf.lastLogIndex, rf.lastLogTerm, args.CandidateID, args.Term)
		rf.CurrentTerm = args.Term
		rf.VotedFor = args.CandidateID
		if rf.role == CANDIDATE {
			rf.role = FOLLOWER
			rf.stopElectionChan <- struct{}{}
		} else if rf.role == LEADER {
			rf.role = FOLLOWER
			rf.heartBeatTimer.Stop()
		}
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = true

		rf.electionTimer.Reset(rf.electionTimeout)
		return
	}

	if ((rf.VotedFor == -1) || (rf.VotedFor == args.CandidateID)) &&
		((rf.lastLogTerm < args.LastLogTerm) ||
			((rf.lastLogTerm == args.LastLogTerm) && (rf.lastLogIndex <= args.LastLogIndex))) {
		fmt.Printf("%d(%d/%d/%d/%d) vote %d on term %d\n",
			rf.me, rf.CurrentTerm, rf.VotedFor, rf.lastLogIndex, rf.lastLogTerm, args.CandidateID, args.Term)
		rf.CurrentTerm = args.Term
		rf.VotedFor = args.CandidateID

		reply.Term = rf.CurrentTerm
		reply.VoteGranted = true

		rf.electionTimer.Reset(rf.electionTimeout)
		return
	}

	fmt.Printf("%d(%d/%d/%d/%d) reject vote for %d on term %d, request last log %d/%d\n",
		rf.me, rf.CurrentTerm, rf.VotedFor, rf.lastLogIndex, rf.lastLogTerm,
		args.CandidateID, args.Term,
		args.LastLogIndex, args.LastLogTerm)
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
	if len(args.Entries) == 0 {
		rf.mu.Lock()

		if args.Term > rf.CurrentTerm {
			rf.CurrentTerm = args.Term
			rf.VotedFor = args.LeaderID

			if rf.role == CANDIDATE {
				rf.stopElectionChan <- struct{}{}
			} else if rf.role == LEADER {
				rf.role = FOLLOWER
				rf.heartBeatTimer.Stop()
			}
		}
		reply.Success = false
		reply.Term = rf.CurrentTerm
		rf.mu.Unlock()
	} else {

	}
	rf.resetElectionTimer()
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
		PrevLogIndex: rf.prevLogIndex,
		PrevLogTerm:  rf.prevLogTerm,
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
					rf.role = FOLLOWER
				}
			}
			rf.mu.Unlock()
		}
	}
	for index, _ := range rf.peers {
		go asyncCall(index)
	}
}

//func (rf *Raft) replicateLog() {
//	rf.mu.Lock()
//	arg := AppendEntriesArg{
//		Term: rf.CurrentTerm,
//		LeaderID: rf.me,
//		PrevLogIndex: rf.prevLogIndex,
//		PrevLogTerm: rf.prevLogTerm,
//		Entries: rf.Logs[rf.lastLogIndex + 1:],
//		LeaderCommit: rf.commitIndex}
//	rf.mu.Unlock()
//
//	asyncCall := func(server int) {
//		rf.mu.Lock()
//		nextIndex := rf.nextIndex[server]
//		log := rf.Logs[nextIndex - 1]
//		arg.PrevLogIndex = log.Index
//		arg.PrevLogTerm = log.Term
//		arg.Entries = rf.Logs[nextIndex:]
//		rf.mu.Unlock()
//
//		reply := AppendEntriesReply{}
//		ok := rf.peers[server].Call("Raft.AppendEntries", arg, &reply)
//		if ok {
//
//		}
//	}
//	for index, _ := range rf.peers {
//		go asyncCall(index)
//	}
//
//}

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
	defer rf.mu.Unlock()

	if rf.role != LEADER {
		return -1, 0, false
	}

	index := rf.lastLogIndex + 1
	term := rf.CurrentTerm
	//log := LogEntry{Index: index, Term: term, Committed: false, Command: command}
	//rf.Logs = append(rf.Logs, log)

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
}

func (rf *Raft) startElection() {
	var lock sync.Mutex
	for {
		rf.mu.Lock()
		if rf.role == FOLLOWER {
			rf.role = CANDIDATE
		} else if rf.role == LEADER {
			return
		}
		rf.CurrentTerm += 1
		rf.VotedFor = rf.me
		fmt.Printf("%d start election on term %d\n", rf.me, rf.CurrentTerm)

		reqVoteArgs := RequestVoteArgs{
			Term:         rf.CurrentTerm,
			CandidateID:  rf.me,
			LastLogIndex: rf.lastLogIndex,
			LastLogTerm:  rf.lastLogTerm}
		rf.mu.Unlock()

		votedNum := 1
		win := make(chan struct{}, 1)
		ctx, cancel := context.WithCancel(context.Background())

		asyncCall := func(server int, votedNum *int) {
			reply := RequestVoteReply{}
			c := make(chan bool, 1)
			select {
			case c <- rf.sendRequestVote(server, reqVoteArgs, &reply):
			case <-ctx.Done():
				return
			}
			ok := <-c
			if ok {
				if reply.VoteGranted {
					//fmt.Printf("%d vote me(%d) term %d\n", server, rf.me, reply.Term)
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

		rf.resetElectionTimer()
		for index, _ := range rf.peers {
			if index == rf.me {
				continue
			}
			go asyncCall(index, &votedNum)
		}

		select {
		case <-win:
			fmt.Printf("%d win election on term %d\n", rf.me, rf.CurrentTerm)
			rf.electionTimer.Stop()
			cancel()
		case <-rf.electionTimer.C:
			fmt.Printf("%d election timeout on term %d\n", rf.me, rf.CurrentTerm)
			cancel()
			continue
		case <-rf.stopElectionChan:
			rf.electionTimer.Stop()
			cancel()
			return
		}

		rf.mu.Lock()
		rf.role = LEADER
		rf.sendHeartBeat()
		rf.mu.Unlock()
		return
	}
}

func (rf *Raft) run() {
	rf.electionTimer = time.NewTimer(rf.electionTimeout)
	for {
		select {
		case <-rf.electionTimer.C:
			rf.startElection()

			rf.mu.Lock()
			if rf.role == LEADER {
				rf.heartBeatTimer.Reset(HeartBeatInterval)
			}
			rf.mu.Unlock()
		case <-rf.heartBeatTimer.C:
			rf.sendHeartBeat()

			rf.mu.Lock()
			if rf.role == LEADER {
				rf.heartBeatTimer.Reset(HeartBeatInterval)
			}
			rf.mu.Unlock()
		}
		rf.electionTimer.Reset(rf.electionTimeout)
	}
}

func (rf *Raft) randomElectionTime() {
	var n int
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for {
		n = r.Intn(MaxElectionTime)
		if n > MinElectionTime {
			rf.electionTimeout = time.Duration(n) * time.Millisecond
			break
		}
	}
	//fmt.Printf("%d election timeout time is %d\n", rf.me, n)
}

func (rf *Raft) resetElectionTimer() {
	if !rf.electionTimer.Stop() {
		select {
		case <-rf.electionTimer.C:
		default:
		}
	}
	rf.electionTimer.Reset(rf.electionTimeout)
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

	// Your initialization code here.
	rf.role = FOLLOWER
	rf.stopElectionChan = make(chan struct{}, 1)
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.Logs = make([]LogEntry, 0)
	rf.randomElectionTime()
	rf.heartBeatTimer = createTimer(HeartBeatInterval)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.run()

	return rf
}

func createTimer(interval time.Duration) *time.Timer {
	t := time.NewTimer(interval)
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	return t
}
