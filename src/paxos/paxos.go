package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"syscall"
	"time"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if 0 > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

func DPrintfRPC(format string, a ...interface{}) (n int, err error) {
	if 0 > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

func DPrintfCall(format string, a ...interface{}) (n int, err error) {
	if 0 > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type Instance struct {
	HighestAcN  int         // Na: highest accepted proposal
	HighestAcV  interface{} // Va: value
	HighestSeen int         // Np: highest seen proposal number
	Decided     bool
	DecidedV    interface{}
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool
	unreliable bool
	rpcCount   int
	peers      []string
	me         int // index into peers[]

	// Your data here.
	instances      map[int]Instance // map from seq number to paxos instance.
	acceptorIns    map[int]Instance // Similar to instances but stores all the intermediate phases (as a acceptor.).
	highestSeqSeen int              // Highest seq number scene.
	peerDone       map[int]int      // peers' done
	doneFreed      int              // indicate the freed highest done
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			DPrintfCall("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	DPrintfCall("%v\n", err)
	return false
}

const PEER_ID_BITS int = 8

func generateUniqueN(highestNSeen int, peerID int) int {
	newID := (highestNSeen >> PEER_ID_BITS) + 1
	return (newID << PEER_ID_BITS) | peerID
}

func parseN(n int) (int, int) {
	return n >> PEER_ID_BITS, n & ((1 << PEER_ID_BITS) - 1)
}

// free memory according to Done
func (px *Paxos) collectGarbage() {
	px.mu.Lock()
	defer px.mu.Unlock()

	// get current min.
	currentMin := px.getMin()

	// check if it's greater than already freed seq.
	// if it is greater than check and free memory from both maps (px.instances, px.acceptorIns).
	if currentMin > px.doneFreed {
		DPrintf("Running garbage collection!\n")
		for i, _ := range px.instances {
			if i < currentMin {
				delete(px.instances, i)
			}
		}
		for i, _ := range px.acceptorIns {
			if i < currentMin {
				delete(px.acceptorIns, i)
			}
		}

		// update the min already freed.
		px.doneFreed = currentMin
	}
}

// Start the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.

	// If seq is less than Min(), it should be ignored:
	if seq < px.Min() {
		return
	}

	px.mu.Lock()
	defer px.mu.Unlock()

	// update the highest seq number.
	if seq > px.highestSeqSeen {
		px.highestSeqSeen = seq
	}

	// check if seq is already decided.
	val, found := px.instances[seq]
	if found && val.Decided == true {
		return
	}

	// return immediately and propose in background (do not block.)
	go func() {

		firstCall := true
		penaltySleep := 10

		for px.dead == false {
			// Delete extra care of extra memory.
			px.collectGarbage()

			// If this is not the first call, we failed last time,
			// Sleep some time to give other proposers a chance
			if !firstCall {
				penaltySleep = (int)(float32(penaltySleep) * 1.5)

				// don't want to sleep for too long :).
				if penaltySleep > 50 {
					penaltySleep = 50
				}

				// random backoff to avoid issues during concurrent puts.
				randomSleepTime := (rand.Int() % penaltySleep) + penaltySleep
				DPrintf("Forced to sleep %vms (penalty:%v), seq %v, proposer: %v\n", randomSleepTime, penaltySleep, seq, px.me)
				time.Sleep(time.Duration(randomSleepTime) * time.Millisecond)
			}

			// marking as false to trigger backoff.
			firstCall = false

			// while not decided
			px.mu.Lock()
			if px.instances[seq].Decided {
				px.mu.Unlock()
				break
			}

			// Phase 1 Prepare

			// get the highest seen instance seen.
			highestSeen := -1
			acc, found := px.acceptorIns[seq]
			if found {
				highestSeen = acc.HighestSeen
			}

			myDone := px.peerDone[px.me]
			majorityPeerCount := len(px.peers)/2 + 1
			peerCount := len(px.peers)

			px.mu.Unlock()

			// generate N based on the highest seq number.
			n := generateUniqueN(highestSeen, px.me)

			DPrintf("Phase 1 Prepare: seq %v, proposer: %v, n: %v\n", seq, px.me, n)

			highestNAccepted := -1
			nextPhaseV := v
			prepareOKCount := 0

			args := &RPCPrepareArgs{
				Seq:    seq,
				N:      n,
				Sender: px.me,
				Done:   myDone,
			}
			var myReply RPCPrepareReply

			// Call myself first
			px.RPCPrepare(args, &myReply)

			if myReply.OK {
				if myReply.Na > highestNAccepted {
					highestNAccepted = myReply.Na
					nextPhaseV = myReply.Va
				}
				prepareOKCount++
			}

			// call peers
			prepareChannel := make(chan bool, peerCount)

			for id, name := range px.peers {
				if id != px.me {
					// concurrently do propose
					// get a copy
					peerID := id
					peer := name
					go func() {
						var reply RPCPrepareReply
						ok := call(peer, "Paxos.RPCPrepare", args, &reply)

						if ok {
							px.mu.Lock()
							if reply.OK == true && reply.Na > highestNAccepted {
								highestNAccepted = reply.Na
								nextPhaseV = reply.Va
							}

							// update done values.
							if reply.Done > px.peerDone[peerID] {
								px.peerDone[peerID] = reply.Done
							}
							px.mu.Unlock()
						}
						// add response to channel ok or not ok.
						prepareChannel <- ok && reply.OK
					}()
				}
			}

			// Wait till we have a majority of responses.
			// Note, even though we block this thread only till we hear majority responses,
			// the thread responsible for getting the responses from each peer might still be running.
			allResponse := 1 // one is from myself
			for {
				prepared := <-prepareChannel
				if prepared {
					prepareOKCount++
				}
				allResponse++
				if prepareOKCount >= majorityPeerCount {
					break
				}
				if allResponse >= peerCount {
					break
				}
			}

			DPrintf("Phase 1 Prepare Done with OKCount %v: seq %v, proposer: %v, n: %v\n", prepareOKCount, seq, px.me, n)

			if prepareOKCount < majorityPeerCount {
				// failed
				continue
			}

			px.mu.Lock()
			// We get a copy of nextPhaseV, so from now on,
			// non-returned prepare RPC will not affect v
			actualV := nextPhaseV
			px.mu.Unlock()

			// Phase 2 Accept:
			DPrintf("Phase 2 Accept: seq %v, proposer: %v, n: %v, v: %v\n", seq, px.me, n, actualV)
			highestNObserved := -1
			acceptOKCount := 0
			accArgs := &RPCAcceptArgs{
				Seq: seq,
				N:   n,
				V:   actualV,
			}
			var accReply RPCAcceptReply

			// call myself
			px.RPCAccept(accArgs, &accReply)

			if accReply.OK {
				acceptOKCount++
				if accReply.N > highestNObserved {
					highestNObserved = accReply.N
				}
			}

			// call peers
			acceptChannel := make(chan bool, peerCount)
			for id, name := range px.peers {
				if id != px.me {
					// concurrently do accept
					peer := name
					go func() {
						var reply RPCAcceptReply
						ok := call(peer, "Paxos.RPCAccept", accArgs, &reply)
						if ok && reply.OK == true {
							px.mu.Lock()
							if reply.N > highestNObserved {
								highestNObserved = reply.N
							}
							px.mu.Unlock()
						}
						acceptChannel <- ok && reply.OK
					}()
				}
			}

			// Same as in prepare we wait for a majority of messages.
			allAcceptResponse := 1 // one is from myself
			for {
				accepted := <-acceptChannel
				if accepted {
					acceptOKCount++
				}
				allAcceptResponse++
				if acceptOKCount >= majorityPeerCount {
					break
				}
				if allAcceptResponse >= peerCount {
					break
				}
			}

			if acceptOKCount < majorityPeerCount {
				// failed
				continue
			}

			// Phase 3 Decide:
			DPrintf("Phase 3 Decide: seq %v, proposer: %v, n: %v, v: %v\n", seq, px.me, n, actualV)

			// call self.
			var reply RPCDecideReply
			decideArgs := &RPCDecideArgs{
				Seq: seq,
				V:   actualV,
			}
			px.RPCDecide(decideArgs, &reply)

			for _, name := range px.peers {
				peer := name
				go func() {
					var reply RPCDecideReply
					for {
						ok := call(peer, "Paxos.RPCDecide", decideArgs, &reply)
						if ok && reply.OK == true {
							break // we only need to ensure every peer hears decide.
						}
						time.Sleep(10 * time.Millisecond)
					}
				}()
			}

			// garbage collection after all the phases are complete!.
			px.collectGarbage()
			break
		}
	}()
}

// Done Called by the application when the application on this machine is done with
// all instances <= seq.
// see the comments for Min() for more explanation.
func (px *Paxos) Done(seq int) {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()

	if seq > px.peerDone[px.me] {
		px.peerDone[px.me] = seq
	}
}

// Max
// the application wants to know the
// highest instance sequence known to
// this peer.
func (px *Paxos) Max() int {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	return px.highestSeqSeen
}

// Min should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefore cannot forget these
// instances.
func (px *Paxos) Min() int {
	// You code here.
	px.mu.Lock()
	ret := px.getMin()
	px.mu.Unlock()

	px.collectGarbage()
	return ret
}

// getMin gets the minimum sequence number which is marked as done by each of the peers.
// caller holds the lock
func (px *Paxos) getMin() int {
	var doneByAll = math.MaxInt32
	for _, done := range px.peerDone {
		if done < doneByAll {
			doneByAll = done
		}
	}
	return doneByAll + 1
}

// Status the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
func (px *Paxos) Status(seq int) (bool, interface{}) {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	if px.instances[seq].Decided {
		return true, px.instances[seq].DecidedV
	}
	return false, nil
}

//
// RPC Definitions:

// RPCPrepareArgs we exchange Done only in Prepare (but both in args and reply).
type RPCPrepareArgs struct {
	Seq    int
	N      int
	Sender int
	Done   int
}

type RPCPrepareReply struct {
	OK   bool
	Na   int
	Va   interface{}
	Done int
}

type RPCAcceptArgs struct {
	Seq int
	N   int
	V   interface{}
}

type RPCAcceptReply struct {
	OK bool
	N  int
}

type RPCDecideArgs struct {
	Seq int
	V   interface{}
}

type RPCDecideReply struct {
	OK bool
}

// RPCPrepare This function handles paxos prepare request
// There are two scenarios:
// 1st: Argument N > The Highest Scene N for sequence number "seq".
// 2nd: Argument N <= The Highest Scene N for sequence number "seq".
func (px *Paxos) RPCPrepare(args *RPCPrepareArgs, reply *RPCPrepareReply) error {
	px.mu.Lock()

	// Has this peer already accepted a proposal for this sequence number?
	acc, found := px.acceptorIns[args.Seq]
	if !found {
		acc.HighestSeen = -1
		acc.HighestAcN = -1
	}

	if args.N > acc.HighestSeen {
		// Scenario 1. Argument N > The Highest Scene N
		acc.HighestSeen = args.N
		reply.OK = true
		reply.Na = acc.HighestAcN
		reply.Va = acc.HighestAcV
		px.acceptorIns[args.Seq] = acc
		DPrintfRPC("RPC peer %v Prepare reply: OK, N: %v, na:%v, va:%v\n", px.me, args.N, reply.Na, reply.Va)
	} else {
		// Scenario 2. Argument N <= The Highest Scene N
		reply.OK = false
		DPrintfRPC("RPC peer %v Prepare reply: rejected, N: %v, HighestSeen: %v\n", px.me, args.N, acc.HighestSeen)
	}

	// Piggyback the done value in reply.
	reply.Done = px.peerDone[px.me]

	// Retrieve and update the done value of the sender from args.
	if args.Done > px.peerDone[args.Sender] {
		px.peerDone[args.Sender] = args.Done
	}

	px.mu.Unlock()

	// Finally, check if any memory can be freed at this moment.
	px.collectGarbage()

	return nil
}

// RPCAccept This function handles paxos accept request
// There are two scenarios:
// 1st: Argument N >= The Highest Scene N for sequence number "seq".
// 2nd: Argument N < The Highest Scene N for sequence number "seq".
func (px *Paxos) RPCAccept(args *RPCAcceptArgs, reply *RPCAcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	acc, found := px.acceptorIns[args.Seq]
	if !found {
		acc.HighestSeen = -1
		acc.HighestAcN = -1
	}

	if args.N >= acc.HighestSeen {
		// Scenario 1. Argument N >= The Highest Scene N
		acc.HighestSeen = args.N
		acc.HighestAcN = args.N
		acc.HighestAcV = args.V
		px.acceptorIns[args.Seq] = acc
		reply.OK = true
		reply.N = args.N
		DPrintfRPC("RPC peer %v Accept reply: OK, na:%v, va:%v\n", px.me, args.N, args.V)
	} else {
		// Scenario 2. Argument N < The Highest Scene N
		reply.OK = false
		DPrintfRPC("RPC peer %v Prepare reply: rejected, N: %v, HighestSeen: %v\n", px.me, args.N, acc.HighestSeen)
	}

	return nil
}

// RPCDecide This function handles paxos decide request
// As we are not considering byzantine nodes.
// We mark a sequence number as decided if the peer request says so.
func (px *Paxos) RPCDecide(args *RPCDecideArgs, reply *RPCDecideReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	// mark the sequence number as decided with the decided request.
	inst, _ := px.instances[args.Seq]
	inst.Decided = true
	inst.DecidedV = args.V
	px.instances[args.Seq] = inst
	reply.OK = true

	return nil
}

// Kill
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
func (px *Paxos) Kill() {
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
}

// Make
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.instances = make(map[int]Instance)
	px.acceptorIns = make(map[int]Instance)
	px.highestSeqSeen = -1
	px.peerDone = make(map[int]int)
	for id, _ := range px.peers {
		px.peerDone[id] = -1
	}
	px.doneFreed = 0

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.unreliable && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.unreliable && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						px.rpcCount++
						go rpcs.ServeConn(conn)
					} else {
						px.rpcCount++
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.dead == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
