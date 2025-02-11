package kvpaxos

import (
	"crypto/rand"
	"math/big"
	"net/rpc"
	"strconv"
	"sync/atomic"
	"time"
)

type Clerk struct {
	servers []string
	// You will have to modify this struct.
	transactionId int64
	clientID      int64
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.transactionId = 1
	// high probability unique client ID
	ck.clientID = nrand()
	return ck
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	// fmt.Println(err)
	return false
}

// the nrand function same as pbservice
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigX, _ := rand.Int(rand.Reader, max)
	x := bigX.Int64()
	return x
}

// lock needed
var globalId int64 = 0

// generate unique request id:
// ClientID-TimeStamp-ClientMonotonicallyIncreasingTransactionID-
// HighProbabilityUniqueRandomTransactionID-GlobalID
func (ck *Clerk) generateID() string {
	var ret string
	ck.transactionId++
	atomic.AddInt64(&globalId, 1)
	ret = strconv.FormatInt(ck.clientID, 10) + "-" +
		time.Now().String() + "-" +
		strconv.FormatInt(ck.transactionId, 10) + "-" +
		strconv.FormatInt(nrand(), 10) + "-" +
		strconv.FormatInt(globalId, 10)
	return ret
}

// Get
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{
		Key:       key,
		RequestID: ck.generateID(),
	}
	var reply GetReply
	for {
		for _, server := range ck.servers {
			ok := call(server, "KVPaxos.Get", &args, &reply)
			if ok {
				return reply.Value
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// PutExt
// set the value for a key.
// keeps trying until it succeeds.
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
	// You will have to modify this function.
	args := PutArgs{
		Key:       key,
		Value:     value,
		DoHash:    dohash,
		RequestID: ck.generateID(),
	}
	var reply PutReply
	for {
		for _, server := range ck.servers {
			ok := call(server, "KVPaxos.Put", &args, &reply)
			if ok {
				return reply.PreviousValue
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
	v := ck.PutExt(key, value, true)
	return v
}
