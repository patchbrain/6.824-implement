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

// 接收到appendentry rpc后生成 heartbeatinfo
type HeartBeatInfo struct {
	beCandidate bool
	beFollower  bool // 需要成为Follower
	term        int
}

// 接收到appendentry rpc后生成 heartbeatinfo
type RequestVoteInfo struct {
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
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

	// 不需持久化
	commitIndex int // 最新提交的日志
	lastApplied int // 最近应用到状态机的日志编号

	// 领导人相关
	nextIndex  []int // 初始化为leader最新日志索引 + 1
	matchIndex []int

	// channel
	applyCh       chan *ApplyMsg
	heartBeatCh   chan *HeartBeatInfo
	requestVoteCh chan *RequestVoteInfo

	// 计时相关
	heartBeatTime time.Duration // 单位为ms
	electionTime  time.Time
}

type LogEntry struct {
	Command interface{}
	Term    int // 任期
	Index   int
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[%d] RequestVote resetElectionTime\n", rf.me)
	rf.resetElectionTime()
	// 确定任期
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		DPrintf("[%d] RequestVote: 候选人任期落后了，候选者: %d\n", rf.me, args.CandidateId)
		return
	} else if args.Term > rf.currentTerm {
		DPrintf("[%d] RequestVote: 候选人任期较高，更新本节点term，候选者: %d\n", rf.me, args.CandidateId)
		rf.currentTerm = args.Term
		rf.rule = Follower
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
		DPrintf("[%d] RequestVote: 已经投给: %d，本次候选者: %d\n", rf.me, rf.votedFor, args.CandidateId)
		return
	}

	rf.votedFor = args.CandidateId
	reply.VoteGranted = true

	// todo: 2B再修改
	//if args.LastLogTerm >= rf.log[len(rf.log)-1].Term {
	//	if args.LastLogIndex >= rf.commitIndex {
	//		rf.votedFor = args.CandidateId
	//		reply.VoteGranted = true
	//		return
	//	} else {
	//		DPrintf("[%d] RequestVote: 投票人最新日志索引大于候选人最新日志索引，本次候选者: %d\n", rf.me, args.CandidateId)
	//	}
	//} else {
	//	DPrintf("[%d] RequestVote: 投票人最新日志任期大于候选人最新日志任期，本次候选者: %d\n", rf.me, args.CandidateId)
	//}
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
// The labrpc package simulates a lossy（有损的） network, in which servers
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// 用于追加日志以及心跳包
type AppendEntriesArgs struct {
	Term         int        // 发送rpc的leader任期
	LeaderId     int        // 领导者ID
	PrevLogIndex int        // 紧挨着新日志的日志索引
	PrevLogTerm  int        // 紧挨着新日志的日志任期
	Entries      []LogEntry // 要追加的logs，如果是心跳包，则该项为空切片
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool // follower需拥有一条日志，该日志的任期和索引和PrevLogIndex、PrevLogTerm相等
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[%d] AppendEntries resetElectionTime\n", rf.me)
	rf.resetElectionTime()
	if args.Term < rf.currentTerm {
		DPrintf("[%d] AppendEntries: 本节点任期较高，任期为 %d\n", rf.me, rf.currentTerm)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	// 对面的任期更大，则更新自己的任期，如果自己是leader则让位
	if args.Term > rf.currentTerm {
		DPrintf("[%d] AppendEntries: 更新当前任期, 更新任期为: %d\n", rf.me, args.Term)
		rf.updateTerm(args.Term)
	}

	// 为心跳包
	if args.Entries == nil {
		reply.Success = true
		reply.Term = rf.currentTerm
		return
	}

	// 以下为 2B 内容
	if args.Entries != nil && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("AppendEntries: PrevLogIndex(Term)不匹配，领导人: %d, 追随者: %d\n", args.LeaderId, rf.me)
		reply.Success = false
		return
	}

	// 为本节点增加日志
	cnt := 0
	for i := args.PrevLogIndex + 1; i < len(rf.log); i++ {
		if rf.log[i].Term != args.Entries[cnt].Term {
			rf.log[i] = args.Entries[cnt]
		}
		cnt++
	}
	for i := cnt; i < len(args.Entries); i++ {
		rf.log = append(rf.log, args.Entries[i])
	}

	// 如果发送者commitIndex大于本节点，则更新本节点的commitIndex
	// AppendEntries 5
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
	}

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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
		rf.votedFor = -1

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
	}
}

func (rf *Raft) leaderElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	defer func() {
		DPrintf("[%d] leaderElection resetElectionTime\n", rf.me)
		rf.resetElectionTime()
	}()
	if rf.rule != Candidate {
		DPrintf("[%d] leaderElection failed, 当前节点不是选举者\n", rf.me)
		return
	}
	rf.currentTerm++
	DPrintf("[%d] into leaderElection, 当前节点任期：%d\n", rf.me, rf.currentTerm)
	rf.votedFor = rf.me
	voteCnt := 1

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log[len(rf.log)-1].Index,
		LastLogTerm:  0,
	}
	// 向其他节点发送投票请求
	var once sync.Once
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			// 向每个节点发送一个rpc发起投票
			go func(id int, voteCnt *int, peerCnt int, voteArgs *RequestVoteArgs, once *sync.Once) {
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(id, voteArgs, reply)
				if !ok {
					return
				}

				if reply.Term > rf.currentTerm {
					DPrintf("[%d] leaderElection: 投票方 term 高，更新本节点状态，退出选举\n", rf.me)
					rf.updateTerm(reply.Term)
					return
				}

				if !reply.VoteGranted {
					DPrintf("[%d] leaderElection: 投票方 %d 未投给本节点\n", rf.me, id)
					return
				}

				*voteCnt++

				if *voteCnt > peerCnt/2 && rf.rule == Candidate && rf.currentTerm == args.Term {
					once.Do(func() {
						// 成为leader,先给其他节点发送心跳包
						DPrintf("[%d] leaderElection: 选举成功，任期为：%d\n", rf.me, rf.currentTerm)
						rf.rule = Leader
						rf.heartBeat()
					})
				}
			}(i, &voteCnt, len(rf.peers), &args, &once)
		}
	}
}

func (rf *Raft) heartBeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	func() {
		DPrintf("[%d] heartBeat resetElectionTime\n", rf.me)
		rf.resetElectionTime()
	}()
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.log[len(rf.log)-1].Index,
		PrevLogTerm:  rf.log[len(rf.log)-1].Term,
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
	}
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			if rf.rule != Leader {
				DPrintf("[%d] heartBeat: 发现有新领导者，更新本节点状态\n", rf.me)
				return
			}
			go rf.appendEntries(true, &args, i)
		}
	}
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
	rf.resetElectionTime()
}

func (rf *Raft) appendEntries(isHeartBeat bool, args *AppendEntriesArgs, id int) {
	// todo: 写2B的时候重新初始化切片，并添加日志同步
	reply := &AppendEntriesReply{}
	if !isHeartBeat {
		args.Entries = make([]LogEntry, len(rf.log))
	}
	ok := rf.sendAppendEntries(id, args, reply)
	if !ok {
		return
	}

	if reply.Term > rf.currentTerm {
		DPrintf("[%d] appendEntries: 发现节点 %d term 高，更新本节点状态，领导者让位\n", rf.me, id)
		rf.updateTerm(reply.Term)
		return
	}

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
	rf.heartBeatTime = 120
	rf.rule = Follower
	rf.currentTerm = 0

	// Your initialization code here (2A, 2B, 2C).
	rf.nextIndex = []int{}
	rf.matchIndex = []int{}

	rf.applyCh = make(chan *ApplyMsg, 0)
	rf.heartBeatCh = make(chan *HeartBeatInfo, 0)
	rf.requestVoteCh = make(chan *RequestVoteInfo, 0)

	// 填充索引0
	rf.log = append(rf.log, LogEntry{
		Command: nil,
		Term:    0,
	})

	for _ = range peers {
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}

	rf.resetElectionTime()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
