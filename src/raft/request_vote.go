package raft

import "sync"

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
	// 确定任期
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		DPrintf("[%d] RequestVote: 候选人任期落后了，候选者: %d\n", rf.me, args.CandidateId)
		return
	} else if args.Term > rf.currentTerm {
		DPrintf("[%d] RequestVote: 候选人任期较高，更新本节点term，候选者: %d\n", rf.me, args.CandidateId)
		rf.updateTerm(args.Term)
		rf.persist()
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
		DPrintf("[%d] RequestVote: 已经投给: %d，本次候选者: %d\n", rf.me, rf.votedFor, args.CandidateId)
		return
	}

	// 先比较最后一条日志的任期，再比较最后一条日志的索引
	if args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
		(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1) {
		// 本节点更新
		rf.resetElectionTime()
		rf.rule = Follower
		rf.votedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
		return
	} else {
		DPrintf("[%d] RequestVote: 本节点日志比候选人 %d 更新,放弃投给候选人\n", rf.me, args.CandidateId)
		return
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
func (rf *Raft) leaderElection() {
	DPrintf("[%d] leaderElection resetElectionTime\n", rf.me)
	rf.resetElectionTime()

	rf.currentTerm++
	DPrintf("[%d] into leaderElection, 当前节点任期：%d\n", rf.me, rf.currentTerm)
	rf.votedFor = rf.me
	rf.persist()

	voteCnt := 1

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
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

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.currentTerm {
					DPrintf("[%d] leaderElection: 投票方 term 高，更新本节点状态，退出选举\n", rf.me)
					rf.updateTerm(reply.Term)
					rf.persist()
					return
				}

				if !reply.VoteGranted {
					DPrintf("[%d] leaderElection: 投票方 %d 未投给本节点\n", rf.me, id)
					return
				}

				*voteCnt++

				if *voteCnt > peerCnt/2 && *voteCnt > 1 && rf.rule == Candidate && rf.currentTerm == args.Term {
					once.Do(func() {
						// 成为leader,先给其他节点发送心跳包
						DPrintf("[%d] leaderElection: 选举成功，任期为：%d\n", rf.me, rf.currentTerm)
						rf.convertToLeader()
						rf.heartBeat()
					})
				}
			}(i, &voteCnt, len(rf.peers), &args, &once)
		}
	}
}

func (rf *Raft) convertToLeader() {
	// 角色，初始化nextIndex
	rf.rule = Leader
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = len(rf.log) - 1
}
