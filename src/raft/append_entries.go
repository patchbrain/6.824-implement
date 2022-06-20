package raft

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

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		DPrintf("[%d] AppendEntries: 本节点任期较高，任期为 %d\n", rf.me, rf.currentTerm)
		rf.leaderID = -1
		return
	}

	DPrintf("[%d] AppendEntries resetElectionTime\n", rf.me)
	rf.resetElectionTime()
	// 对面的任期更大，则更新自己的任期，如果自己是leader则让位
	if args.Term > rf.currentTerm {
		DPrintf("[%d] AppendEntries: 更新当前任期, 更新任期为: %d\n", rf.me, args.Term)
		rf.updateTerm(args.Term)
		return
	}

	if rf.rule == Candidate {
		rf.rule = Follower
	}

	// 以下为 2B 内容
	if args.PrevLogIndex > len(rf.log)-1 {
		// 没找到
		reply.Success = false
		DPrintf("[%d] AppendEntries: 未找到 PrevLogIndex 对应的日志", rf.me)
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("[%d] AppendEntries: 未找到 PrevLogTerm 对应任期的日志", rf.me)
		reply.Success = false
		if args.PrevLogIndex > 0 {
			rf.log = rf.log[:args.PrevLogIndex]
		}
		return
	}

	// 为本节点增加日志
	cnt := 0
	DPrintf("[%d] AppendEntries: 本节点日志: %v\n", rf.me, rf.log)
	for i := 0; i < len(args.Entries); i++ {
		pos := args.PrevLogIndex + 1 + i
		if pos >= len(rf.log) {
			break
		}
		if pos < len(rf.log) && args.Entries[i].Term != rf.log[pos].Term {
			rf.log = rf.log[:pos]
			break
		}
		cnt++
	}
	for i := cnt; i < len(args.Entries); i++ {
		rf.log = append(rf.log, args.Entries[i])
	}

	//for i := args.PrevLogIndex + 1; i < len(rf.log); i++ {
	//	if cnt == len(args.Entries) {
	//		rf.log = rf.log[:i]
	//		break
	//	}
	//	if rf.log[i].Term != args.Entries[cnt].Term {
	//		rf.log = rf.log[:i]
	//		break
	//	}
	//	cnt++
	//}
	//for i := cnt; i < len(args.Entries); i++ {
	//	rf.log = append(rf.log, args.Entries[i])
	//}
	DPrintf("[%d] AppendEntries: 为本节点增加日志后: %v\n", rf.me, rf.log)

	// 如果发送者commitIndex大于本节点，则更新本节点的commitIndex
	// AppendEntries 5
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, len(rf.log)-1)
		rf.apply()
		DPrintf("[%d] AppendEntries: 更新本节点 commitIndex: %d\n", rf.me, rf.commitIndex)
		DPrintf("[%d] AppendEntries: 当前日志: %+v\n", rf.me, rf.log)
	}

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft) heartBeat() {

	//DPrintf("[%d] heartBeat resetElectionTime\n", rf.me)
	//rf.resetElectionTime()

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: -1,
		PrevLogTerm:  -1,
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
	}

	for i := range rf.peers {
		if i != rf.me {
			if rf.rule != Leader || rf.currentTerm != args.Term {
				DPrintf("[%d] heartBeat: 发现有新领导者或本领导者任期有变\n", rf.me)
				return
			}
			//wg.Add(1)
			go rf.appendEntries(args, i)
		}
	}
	//wg.Wait()

	//// 处理因日志出错的peers
	//for len(failedPeers) > 0 {
	//	time.Sleep(time.Millisecond * 10)
	//	tempFailedPeers := make([]int, 0)
	//	for i := range failedPeers {
	//		if rf.rule != Leader {
	//			DPrintf("[%d] heartBeat: 发现有新领导者，更新本节点状态\n", rf.me)
	//			return
	//		}
	//		//wg.Add(1)
	//		go rf.appendEntries(heartBeat, args, i, &highestLogIndexCnt, tempFailedPeers, &wg)
	//	}
	//	failedPeers = append([]int{}, tempFailedPeers...)
	//	//wg.Wait()
	//}
}

func (rf *Raft) appendEntries(args AppendEntriesArgs, id int) {
	reply := &AppendEntriesReply{}
	justHeartBeat := false

	rf.mu.Lock()

	args.PrevLogIndex = rf.nextIndex[id] - 1
	args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
	args.LeaderCommit = rf.commitIndex
	if rf.nextIndex[id] <= 0 {
		rf.nextIndex[id] = 1
	}
	if rf.nextIndex[id] > len(rf.log) {
		rf.nextIndex[id] = len(rf.log) - 1
	}
	if len(rf.log) > rf.nextIndex[id]-1 {
		// 填充日志
		args.Entries = make([]LogEntry, len(rf.log)-rf.nextIndex[id])
		copy(args.Entries, rf.log[rf.nextIndex[id]:])
	} else {
		justHeartBeat = true
	}

	rf.mu.Unlock()

	ok := rf.sendAppendEntries(id, &args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		DPrintf("[%d] appendEntries: 发现节点 %d term 高，更新本节点状态，领导者让位\n", rf.me, id)
		rf.updateTerm(reply.Term)
		return
	}

	if justHeartBeat {
		return
	}

	if reply.Success == false {
		DPrintf("[%d] appendEntries: 发现节点 %d 中对应 index 日志任期不同,或者无该 index 日志， 因此 index-1， index: %d\n", rf.me, id, rf.nextIndex[id])
		if rf.nextIndex[id] > 1 {
			rf.nextIndex[id]--
		}
	} else {
		DPrintf("[%d] appendEntries: 成功更新节点 %d 的日志: %d\n", rf.me, id, len(rf.log)-1)
		match := args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[id] = match + 1
		rf.matchIndex[id] = match
		for i := rf.commitIndex + 1; i < len(rf.log); i++ {
			cnt := 1 // 算上自己的
			if rf.log[i].Term != rf.currentTerm {
				// 不提交旧leader的日志
				continue
			}

			for k := range rf.peers {
				if k != rf.me && rf.matchIndex[k] >= i {
					cnt++
				}

				if cnt > len(rf.peers)/2 {
					rf.commitIndex = i
					rf.apply()
					DPrintf("[%d] appendEntries: 提交日志 %d \n", rf.me, len(rf.log)-1)
					// 提前结束
					break
				}
			}
		}
	}

}
