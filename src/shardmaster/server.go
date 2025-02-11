package shardmaster

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"sync"
	"syscall"
	"time"
)

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	configs     []Config         // indexed by config num
	lastApplied int              //the highest paxos operation applied to our loggy log
	pendingReqs map[int]chan *Op //requests we have yet to respond to
	count       int
}

type Op struct {
	OpCode int
	ID     string
	Args   interface{}
}

const (
	OpQuery = iota + 1
	OpJoin
	OpLeave
	OpMove
	OpNoop
)

func (sm *ShardMaster) resetReply(reply interface{}) {
	if r, ok := reply.(*QueryReply); ok {
		r.Config = Config{}
	}
}

func (sm *ShardMaster) genSeqChanID() (int, chan *Op, string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	seq := sm.findAvailableSeq()
	ID := fmt.Sprintf("%d-%d", sm.me, sm.count)
	sm.count++
	c := make(chan *Op)
	sm.pendingReqs[seq] = c
	return seq, c, ID
}

func (sm *ShardMaster) findAvailableSeq() int {
	seq := sm.px.Max() + 1
	for {
		if _, ok := sm.pendingReqs[seq]; !ok && sm.lastApplied < seq {
			return seq
		}
		seq++
	}
}

func (sm *ShardMaster) status() {
	seq := 0
	to := 10 * time.Millisecond

	for !sm.dead {
		decided, r := sm.px.Status(seq)

		if decided {
			op, isOp := r.(*Op)
			if !isOp {
				log.Fatal("Fatal error: could not cast Op")
			}

			sm.mu.Lock()
			sm.applyChange(seq, op)

			if ch, ok := sm.pendingReqs[seq]; ok {
				ch <- op
				sm.mu.Unlock()
				<-ch
			} else {
				sm.mu.Unlock()
			}

			seq++
			to = 10 * time.Millisecond

		} else {
			time.Sleep(to)
			to = min(to*2, 25*time.Millisecond)

			if to == 10*time.Millisecond {
				sm.px.Start(seq, &Op{OpCode: OpNoop})
			}
		}
	}
}

func min(x, y time.Duration) time.Duration {
	if x < y {
		return x
	}
	return y
}

func (sm *ShardMaster) applyChange(seq int, op interface{}) {
	castedOp, ok := op.(*Op)
	if !ok {
		log.Fatal("Fatal error: could not cast Op")
	}

	switch castedOp.OpCode {
	case OpJoin:
		args := castedOp.Args.(*JoinArgs)
		sm.applyJoin(args)
	case OpLeave:
		args := castedOp.Args.(*LeaveArgs)
		sm.applyLeave(args)
	case OpMove:
		args := castedOp.Args.(*MoveArgs)
		sm.applyMove(args)
	case OpQuery, OpNoop:
		// No state changes
	}

	sm.lastApplied = seq
	sm.px.Done(seq)
}

func (sm *ShardMaster) applyJoin(args *JoinArgs) {
	c := Config{}
	c.Num = len(sm.configs)
	c.Shards = sm.redistShard(OpJoin, args.GID)
	c.Groups = sm.updateMap(OpJoin, args.GID, args.Servers)
	sm.configs = append(sm.configs, c)
}

func (sm *ShardMaster) applyLeave(args *LeaveArgs) {
	c := Config{}
	c.Num = len(sm.configs)
	c.Shards = sm.redistShard(OpLeave, args.GID)
	c.Groups = sm.updateMap(OpLeave, args.GID, make([]string, 1))
	sm.configs = append(sm.configs, c)
}

func (sm *ShardMaster) applyMove(args *MoveArgs) {
	old := sm.configs[len(sm.configs)-1]
	n := Config{}
	n.Num = len(sm.configs)
	n.Shards = old.Shards
	n.Shards[args.Shard] = args.GID
	n.Groups = make(map[int64][]string)
	for k, v := range old.Groups {
		n.Groups[k] = v
	}
	sm.configs = append(sm.configs, n)
}

func (sm *ShardMaster) redistShard(opCode int, GID int64) [10]int64 {
	current := sm.configs[len(sm.configs)-1]
	newShards := current.Shards
	count := initializeShardCount(current.Groups)

	switch opCode {
	case OpJoin:
		newShards = addGroup(GID, newShards, count)
	case OpLeave:
		newShards = removeGroup(GID, newShards, count)
	}

	return rebalance(count, newShards)
}

func (sm *ShardMaster) updateMap(opCode int, GID int64, servers []string) map[int64][]string {
	currentGroups := sm.configs[len(sm.configs)-1].Groups
	newMap := currentGroups
	switch opCode {
	case OpJoin:
		if _, exists := currentGroups[GID]; !exists {
			newMap = make(map[int64][]string, len(currentGroups)+1)
			for k, v := range currentGroups {
				newMap[k] = v
			}
			newMap[GID] = servers
		} else {
			newMap[GID] = servers
		}
	case OpLeave:
		if _, exists := currentGroups[GID]; exists {
			newMap = make(map[int64][]string, len(currentGroups)-1)
			for k, v := range currentGroups {
				if k != GID {
					newMap[k] = v
				}
			}
		}
	}
	return newMap
}

func initializeShardCount(groups map[int64][]string) map[int64]int {
	count := make(map[int64]int)
	for gid := range groups {
		count[gid] = 0
	}
	return count
}

func addGroup(GID int64, shards [10]int64, count map[int64]int) [10]int64 {
	count[GID] = 0
	for i, v := range shards {
		if v == 0 { // unassigned shard
			shards[i] = GID
			count[GID]++
		} else {
			count[v]++
		}
	}
	return shards
}

func removeGroup(GID int64, shards [10]int64, count map[int64]int) [10]int64 {
	delete(count, GID)
	var random int64
	for gid := range count {
		random = gid
		break
	}

	for i, v := range shards {
		if v == GID {
			shards[i] = random
		}
		count[shards[i]]++
	}
	return shards
}

func rebalance(count map[int64]int, newShards [10]int64) [10]int64 {
	for {
		least, most := findMinMaxGroups(count)

		if count[most] <= count[least]+1 {
			break
		}

		newShards = reassignShard(most, least, newShards)
		count[most]--
		count[least]++
	}

	return newShards
}

func findMinMaxGroups(count map[int64]int) (least, most int64) {
	leastCount := NShards + 1
	mostCount := -1

	for group, shardCount := range count {
		if shardCount < leastCount {
			least = group
			leastCount = shardCount
		}
		if shardCount > mostCount {
			most = group
			mostCount = shardCount
		}
	}
	return least, most
}

func reassignShard(from int64, to int64, a [10]int64) [10]int64 {
	for i := 0; i < len(a); i++ {
		if a[i] == from {
			a[i] = to
			break // Exit loop after the first match
		}
	}
	return a
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	sm.resetReply(reply)
	for {
		seq, c, ID := sm.genSeqChanID()
		proposedVal := &Op{OpCode: OpJoin, ID: ID, Args: args}

		sm.px.Start(seq, proposedVal)
		acceptedVal := <-c

		sm.mu.Lock()
		if proposedVal.ID == acceptedVal.ID {
			delete(sm.pendingReqs, seq)
			c <- proposedVal
			sm.mu.Unlock()
			break
		} else {
			delete(sm.pendingReqs, seq)
			c <- proposedVal
			sm.mu.Unlock()
		}
	}
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	for {
		seq, c, ID := sm.genSeqChanID()
		proposedVal := &Op{OpCode: OpLeave, ID: ID, Args: args}

		sm.px.Start(seq, proposedVal)
		acceptedVal := <-c

		sm.mu.Lock()
		if proposedVal.ID == acceptedVal.ID {
			delete(sm.pendingReqs, seq)
			c <- proposedVal
			sm.mu.Unlock()
			break
		} else {
			delete(sm.pendingReqs, seq)
			c <- proposedVal
			sm.mu.Unlock()
		}
	}
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	sm.resetReply(reply)
	seq, c, ID := sm.genSeqChanID()
	proposedVal := &Op{OpCode: OpMove, ID: ID, Args: args}
	retryProposal := func() bool {
		sm.px.Start(seq, proposedVal)
		select {
		case acceptedVal := <-c:
			return proposedVal.ID == acceptedVal.ID
		case <-time.After(200 * time.Millisecond):
			return false
		}
	}

	for !retryProposal() {
		sm.mu.Lock()
		delete(sm.pendingReqs, seq)
		sm.mu.Unlock()

		seq, c, ID = sm.genSeqChanID()
		proposedVal.ID = ID
	}

	defer func() {
		sm.mu.Lock()
		delete(sm.pendingReqs, seq)
		sm.mu.Unlock()
		c <- proposedVal
	}()

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	sm.resetReply(reply)
	for {
		seq, c, ID := sm.genSeqChanID()
		proposedVal := &Op{OpCode: OpQuery, ID: ID, Args: args}

		sm.px.Start(seq, proposedVal)
		acceptedVal := <-c

		sm.mu.Lock()
		if proposedVal.ID == acceptedVal.ID {
			if args.Num != -1 && args.Num < len(sm.configs) {
				reply.Config = sm.configs[args.Num]
			} else {
				reply.Config = sm.configs[len(sm.configs)-1]
			}
			delete(sm.pendingReqs, seq)
			c <- proposedVal
			sm.mu.Unlock()
			break
		} else {
			delete(sm.pendingReqs, seq)
			c <- proposedVal
			sm.mu.Unlock()
		}
	}
	return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
	sm.dead = true
	sm.l.Close()
	sm.px.Kill()
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
func StartServer(servers []string, me int) *ShardMaster {
	gob.Register(&Op{})
	gob.Register(&JoinArgs{})
	gob.Register(&LeaveArgs{})
	gob.Register(&MoveArgs{})
	gob.Register(&QueryArgs{})

	sm := new(ShardMaster)
	sm.me = me
	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}
	sm.pendingReqs = make(map[int]chan *Op)
	sm.lastApplied = -1
	sm.count = 0

	rpcs := rpc.NewServer()
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	go func() {
		for sm.dead == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.dead == false {
				if sm.unreliable && (rand.Int63()%1000) < 100 {
					conn.Close()
				} else if sm.unreliable && (rand.Int63()%1000) < 200 {
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && sm.dead == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()
	go sm.status()
	return sm
}
