package kvpaxos

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"strconv"
	"sync"
	"syscall"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string // can be GET or PUT
	Key       string
	Value     string
	RequestID string
	DoHash    bool
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	// Your definitions here.
	globalSeq       int
	data            map[string]string
	visitedRequests map[string]string
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{
		Operation: "Get",
		Key:       args.Key,
		RequestID: args.RequestID,
	}

	DPrintf("Server %v received Get: %v\n", kv.me, op)

	ret1, ret2 := kv.makeAgreementAndApplyChange(op)
	reply.Err = ret1
	reply.Value = ret2

	return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := Op{
		Operation: "Put",
		Key:       args.Key,
		Value:     args.Value,
		DoHash:    args.DoHash,
		RequestID: args.RequestID,
	}

	DPrintf("Server %v received Put: %v\n", kv.me, op)

	ret1, ret2 := kv.makeAgreementAndApplyChange(op)
	reply.Err = ret1
	reply.PreviousValue = ret2

	return nil
}

// the caller should hold the lock
func (kv *KVPaxos) makeAgreementAndApplyChange(op Op) (Err, string) {

	// increase sequence number.
	seq := kv.globalSeq + 1

	// Retry till this operation is entered into log.
	for {
		DPrintf("Server %v is proposing seq %v\n", kv.me, seq)
		kv.px.Start(seq, op)
		agreedV := kv.WaitForAgreement(seq)
		if agreedV != op {
			seq++
			continue
		}
		DPrintf("Server %v propose seq %v OK\n", kv.me, seq)
		// OK.
		break
	}
	// If [globalSeq+1, seq) is not null, then there is some log
	// beyond our commit, catch up first:
	for idx := kv.globalSeq + 1; idx < seq; idx++ {
		v := kv.WaitForAgreement(idx)
		kv.applyChange(v.(Op))
	}
	// Now we can apply our op, and return the value
	ret1, ret2 := kv.applyChange(op)

	// Update global seq
	kv.globalSeq = seq

	// mark seq as done.
	kv.px.Done(seq)
	return ret1, ret2
}

// WaitForAgreement this function waits for seq to be decided by paxos, and returns the decided operation.
func (kv *KVPaxos) WaitForAgreement(seq int) interface{} {
	for {
		ok, v := kv.px.Status(seq)
		if ok {
			return v
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// applyChange Applies the operation (Op) to the server database.
// the caller should hold the lock
func (kv *KVPaxos) applyChange(op Op) (Err, string) {
	if op.Operation == "Put" {
		// Case 1: Put

		// check if already applied?
		prevV, found := kv.visitedRequests[op.RequestID]
		if found {
			return OK, prevV
		}

		// get the old value and new value based on Hash/noHash.
		oldValue := kv.data[op.Key]
		newValue := ""

		if op.DoHash {
			newValue = strconv.Itoa(int(hash(oldValue + op.Value)))
		} else {
			newValue = op.Value
		}

		DPrintf("Server %v, apply op: %v, old: %v, new: %v\n", kv.me, op, oldValue, newValue)

		// update db with new value.
		kv.data[op.Key] = newValue

		// Only PutHash needs old value:
		if !op.DoHash {
			// Discard to save memory
			oldValue = ""
		}

		// update request reply DB.
		kv.visitedRequests[op.RequestID] = oldValue
		return OK, oldValue

	} else if op.Operation == "Get" {
		// Case 2: Get, If ket present in DB return value else return ErrNoKey.
		value, found := kv.data[op.Key]

		if found {
			return OK, value
		} else {
			return ErrNoKey, ""
		}
	} else {
		// Invalid Operation
		return "", ""
	}
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

// StartServer
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.data = make(map[string]string)
	kv.visitedRequests = make(map[string]string)
	kv.globalSeq = -1

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
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
