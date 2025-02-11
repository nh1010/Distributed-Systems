package shardkv

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"shardmaster"
	"strconv"
	"sync"
	"syscall"
	"time"
)

const Debug = 0

const (
	ErrNoSuchVersion   = "ErrNoSuchVersion"
	ErrNoSuchShard     = "ErrNoSuchShard"
	ErrUnknownOp       = "ErrUnknownOp"
	ErrOperationFailed = "ErrOperationFailed"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	Operation string
	Seq       int64
	Temp      interface{}
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	kvlock     sync.Mutex
	version    map[int]map[int]map[string]string
	KVStore    map[string]string
	shardMap   map[string]int
	min        int
	confg      shardmaster.Config
	ReqID      map[int64]string
	shardLocks map[int]*sync.Mutex
}

func (kv *ShardKV) Consensus(seq int) (Op, error) {
	retryInterval := time.Millisecond * 10
	maxInterval := time.Second
	maxRetries := 10

	for i := 0; i < maxRetries; i++ {
		decided, value := kv.px.Status(seq)
		if decided {
			return value.(Op), nil
		}
		time.Sleep(retryInterval)

		retryInterval *= 2
		if retryInterval > maxInterval {
			retryInterval = maxInterval
		}
	}
	return Op{}, fmt.Errorf("consensus failed after %d retries", maxRetries)
}

func (kv *ShardKV) mustConsensus(seq int) Op {
	op, err := kv.Consensus(seq)
	if err != nil {
		log.Fatalf("Consensus failed for seq=%d: %v", seq, err)
	}
	return op
}

func (kv *ShardKV) ApplyRPCLog(max int) (interface{}, error) {
	var result interface{}

	for seq := kv.min; seq <= max; seq++ {
		decided, value := kv.px.Status(seq)
		var op Op

		if decided {
			op = value.(Op)
		} else {
			emptyOp := Op{Operation: "NoOp", Seq: int64(seq), Temp: nil}
			kv.px.Start(seq, emptyOp)

			op = kv.mustConsensus(seq)
		}
		result = kv.applyOperator(op)
		DPrintf("ApplyRPCLog: Applied op %+v at seq=%d", op, seq)
	}

	kv.px.Done(max) 
	kv.min = max + 1
	return result, nil
}

func (kv *ShardKV) GetShardData(args *ValueArgs, reply *ValueReply) error {
	shardLock := kv.getShardLock(args.ShardID)
	shardLock.Lock()
	defer shardLock.Unlock()

	versionData := kv.getVersionData(args.ViewID)
	if versionData == nil {
		reply.Err = ErrNoSuchVersion
		DPrintf("GetShardData: No data for ViewID=%d", args.ViewID)
		return nil
	}

	shardData := kv.getShardData(versionData, args.ShardID)
	if shardData == nil {
		reply.Err = ErrNoSuchShard
		DPrintf("GetShardData: No data for ShardID=%d in ViewID=%d", args.ShardID, args.ViewID)
		return nil
	}

	reply.KeyValue = copyMap(shardData)
	reply.Err = OK
	return nil
}

func (kv *ShardKV) getShardLock(shardID int) *sync.Mutex {
	kv.mu.Lock() 
	defer kv.mu.Unlock()

	if _, exists := kv.shardLocks[shardID]; !exists {
		kv.shardLocks[shardID] = &sync.Mutex{}
	}

	return kv.shardLocks[shardID]
}

func (kv *ShardKV) getVersionData(ViewID int) map[int]map[string]string {
	versionData, exists := kv.version[ViewID]
	if !exists {
		return nil
	}
	return versionData
}

func (kv *ShardKV) getShardData(versionData map[int]map[string]string, shardID int) map[string]string {
	shardData, exists := versionData[shardID]
	if !exists {
		return nil
	}
	return shardData
}

// Helper function to copy a map
func copyMap(original map[string]string) map[string]string {
	copied := make(map[string]string, len(original))
	for key, value := range original {
		copied[key] = value
	}
	return copied
}

func (kv *ShardKV) applyOperator(op Op) interface{} {
	if value, done := kv.ReqID[op.Seq]; done {
		return value
	}

	switch op.Operation {
	case "Get":
		args := op.Temp.(GetArgs)
		value, _ := kv.KVStore[args.Key]
		return value

	case "Put":
		args := op.Temp.(PutArgs)
		kv.KVStore[args.Key] = args.Value
		kv.shardMap[args.Key] = args.ShardID
		kv.ReqID[op.Seq] = ""
		return ""

	case "Puthash":
		args := op.Temp.(PutArgs)
		kv.shardMap[args.Key] = args.ShardID
		oldValue, _ := kv.KVStore[args.Key]
		kv.ReqID[op.Seq] = oldValue
		kv.KVStore[args.Key] = strconv.Itoa(int(hash(oldValue + args.Value)))
		return oldValue

	case "Update":
		kv.handleUpdate(op)
		kv.ReqID[op.Seq] = ""
		return ""

	default:
		return ""
	}
}

func (kv *ShardKV) transferShardData(shardID int, oldConfig shardmaster.Config, ViewID int) error {
	maxRetries := 10
	retryInterval := time.Millisecond * 50

	for attempt := 0; attempt < maxRetries; attempt++ {
		servers := oldConfig.Groups[oldConfig.Shards[shardID]]
		for _, server := range servers {
			args := ValueArgs{ShardID: shardID, ViewID: ViewID}
			reply := ValueReply{}

			if call(server, "ShardKV.GetShardData", &args, &reply) && reply.Err == OK {
				kv.kvlock.Lock() 
				for key, value := range reply.KeyValue {
					kv.KVStore[key] = value
					kv.shardMap[key] = shardID
				}
				kv.kvlock.Unlock()
				return nil  
			}
		}

		DPrintf("transferShardData: Retry %d for shard %d", attempt+1, shardID)
		time.Sleep(retryInterval)
	}

	DPrintf("transferShardData: Failed to transfer shard %d after %d retries", shardID, maxRetries)
	return fmt.Errorf("failed to transfer shard %d after %d retries", shardID, maxRetries)
}

func (kv *ShardKV) handleUpdate(op Op) {
	oldConfig := kv.confg
	newConfig := op.Temp.(shardmaster.Config)

	for i := oldConfig.Num; i < newConfig.Num; i++ {
		newConfg := kv.sm.Query(oldConfig.Num + 1)

		kv.kvlock.Lock()
		for shardID := 0; shardID < len(newConfg.Shards); shardID++ {
			if oldConfig.Shards[shardID] == kv.gid && newConfg.Shards[shardID] != kv.gid {
				if _, exists := kv.version[i]; !exists {
					kv.version[i] = make(map[int]map[string]string)
				}
				shardLog := make(map[string]string)
				for key, value := range kv.KVStore {
					if kv.shardMap[key] == shardID {
						shardLog[key] = value
					}
				}
				kv.version[i][shardID] = shardLog
			}
		}
		kv.kvlock.Unlock()

		for shardID := 0; shardID < len(newConfg.Shards); shardID++ {
			if oldConfig.Shards[shardID] > 0 && oldConfig.Shards[shardID] != kv.gid && newConfg.Shards[shardID] == kv.gid {
				err := kv.transferShardData(shardID, oldConfig, i)
				if err != nil {
					DPrintf("handleUpdate: Failed to transfer shard %d: %v", shardID, err)
				}
			}
		}

		kv.confg = newConfg
		oldConfig = newConfg
	}
}

func (kv *ShardKV) executeOperator(op Op) (interface{}, error) {
	const maxAttempts = 7
	retryInterval := time.Millisecond * 50
	maxRetryInterval := time.Second

	n := kv.px.Max() + 1

	for i := 0; i < maxAttempts; i++ {
		DPrintf("executeOperator: Attempting op %+v at Paxos instance %d, attempt %d", op, n, i+1)

		kv.px.Start(n, op)
		decidedOp := kv.mustConsensus(n)

		if decidedOp.Operation == "Err" {
			DPrintf("executeOperator: Consensus failed for op %+v at instance %d", op, n)
			time.Sleep(retryInterval)
			retryInterval *= 2
			if retryInterval > maxRetryInterval {
				retryInterval = maxRetryInterval
			}
			continue
		}

		if decidedOp.Seq == op.Seq {
			DPrintf("executeOperator: Matching op %+v found at instance %d", decidedOp, n)
			rst, err := kv.ApplyRPCLog(n)
			if err == nil {
				DPrintf("executeOperator: Successfully completed op %+v at instance %d", op, n)
				return rst, nil
			}

			DPrintf("executeOperator: ApplyRPCLog failed for op %+v at instance %d: %v", op, n, err)
			return nil, fmt.Errorf("apply RPC log failed: %w", err)
		}

		n++
	}

	DPrintf("executeOperator: Exceeded retry attempts for op %+v", op)
	return nil, fmt.Errorf("exceeded retry attempts for op %+v", op)
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.ViewID != kv.confg.Num {
		reply.Err = ErrWrongGroup
		return nil
	}

	op := Op{Operation: "Get", Seq: args.ReqID, Temp: *args}
	result, err := kv.executeOperator(op)

	if err != nil {
		reply.Err = ErrOperationFailed
		return err
	}

	reply.Value = result.(string)
	reply.Err = OK
	return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.ViewID != kv.confg.Num {
		reply.Err = "Err"
		return nil
	}
	var op Op
	if args.DoHash {
		op = Op{"Puthash", args.ReqID, *args}
	} else {
		op = Op{"Put", args.ReqID, *args}
	}
	rst, Error := kv.executeOperator(op)
	if Error != nil {
		reply.Err = "Err"
	} else {
		reply.PrevVal = rst.(string)
		reply.Err = OK
	}
	return nil
}

// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	newConfig := kv.sm.Query(-1)
	kv.mu.Unlock()

	if newConfig.Num > kv.confg.Num {
		for {
			_, err := kv.executeOperator(Op{"Update", int64(newConfig.Num), newConfig})
			if err == nil {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// tell the server to shut itself down.
func (kv *ShardKV) kill() {
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//
//	servers that implement the shardmaster.
//
// servers[] contains the ports of the servers
//
//	in this replica group.
//
// Me is the index of this server in servers[].
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})
	gob.Register(GetArgs{})
	gob.Register(GetReply{})
	gob.Register(PutArgs{})
	gob.Register(PutReply{})
	gob.Register(shardmaster.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().
	kv.KVStore = make(map[string]string)
	kv.ReqID = make(map[int64]string)
	kv.min = 0
	kv.shardMap = make(map[string]int)
	kv.confg = kv.sm.Query(-1)
	kv.version = make(map[int]map[int]map[string]string)
	kv.shardLocks = make(map[int]*sync.Mutex)

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.dead == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.dead == false {
				if kv.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
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
			if err != nil && kv.dead == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.dead == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
