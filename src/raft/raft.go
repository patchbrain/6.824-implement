package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool // 代表ApplyMsg是否有新提交的日志
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Rule int

const (
	Follower = iota
	Candidate
	Leader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	cond      *sync.Cond
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 角色
	rule Rule

	// 需要持久化
	currentTerm int
	votedFor    int
	log         []LogEntry
	leaderID    int

	// 不需持久化
	commitIndex int // 最新提交的日志
	lastApplied int // 最近应用到状态机的日志编号

	// 领导人相关
	nextIndex  []int // 初始化为leader最新日志索引 + 1
	matchIndex []int

	// channel
	applyCh chan ApplyMsg

	// 计时相关
	heartBeatTime time.Duration // 单位为ms
	electionTime  time.Time
}

type LogEntry struct {
	Command interface{}
	Term    int // 任期
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.rule == Leader {
		isleader = true
	}

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	index = len(rf.log)
	term, isLeader = rf.GetState()

	if !isLeader {
		// 非leader则直接返回false
		return -1, term, isLeader
	}
	DPrintf("[%d] Start: 添加一条日志，当前日志 %d 条\n", rf.me, len(rf.log))

	log := LogEntry{
		Command: command,
		Term:    term,
	}

	rf.log = append(rf.log, log)

	rf.heartBeat()

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	DPrintf("Kill: 节点: %d 被杀死\n", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		time.Sleep(rf.heartBeatTime * time.Millisecond)

		rf.mu.Lock()
		if rf.rule != Leader {
			// 非Leader，就要接收心跳包
			// rules for servers: Follower: 2
			if time.Now().After(rf.electionTime) {
				// 进行一次选举
				rf.rule = Candidate
				rf.leaderElection()
			}
		} else {
			// rules for servers: Leader: 1
			rf.heartBeat() // 发送心跳包
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {
		//if rf.rule != Leader {
		//	continue
		//}
		if rf.lastApplied < rf.commitIndex && len(rf.log)-1 > rf.lastApplied {
			rf.lastApplied++
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			DPrintf("[%d] applier: 成功apply日志 %d \n", rf.me, rf.lastApplied)
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
		} else {
			rf.cond.Wait()
		}
	}
}

func (rf *Raft) apply() {
	rf.cond.Broadcast()
}

func (rf *Raft) resetElectionTime() {
	now := time.Now()
	d := time.Millisecond * time.Duration(150+rand.Int63n(151))
	rf.electionTime = now.Add(d)
}

func (rf *Raft) updateTerm(term int) {
	rf.currentTerm = term
	rf.rule = Follower
	rf.votedFor = -1
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
	rf.votedFor = -1
	rf.leaderID = -1
	rf.heartBeatTime = 100
	rf.rule = Follower
	rf.currentTerm = 0
	rf.cond = sync.NewCond(&rf.mu)

	// Your initialization code here (2A, 2B, 2C).
	rf.nextIndex = []int{}
	rf.matchIndex = []int{}

	rf.applyCh = applyCh

	// 填充索引0
	rf.log = append(rf.log, LogEntry{
		Command: nil,
		Term:    0,
	})

	for _ = range peers {
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}

	rf.mu.Lock()
	rf.resetElectionTime()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}
