package pbservice

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"
	"viewservice"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type PBServer struct {
	l          net.Listener
	dead       bool
	unreliable bool
	me         string
	vs         *viewservice.Clerk
	done       sync.WaitGroup
	finish     chan interface{}

	view    viewservice.View
	content map[string]string

	mu sync.Mutex
}

// helper functions
func (pb *PBServer) isPrimary() bool {
	return pb.view.Primary == pb.me
}

func (pb *PBServer) isBackup() bool {
	return pb.view.Backup == pb.me
}

func (pb *PBServer) sendForward(args *ForwardArgs) error {
	var reply ForwardReply
	ok := call(pb.view.Backup, "PBServer.ProcessForward", args, &reply)
	if !ok {
		return errors.New("can't forward put")
	}
	return nil
}

func (pb *PBServer) Forward(args *ForwardArgs) error {
	if len(pb.view.Backup) == 0 {
		return nil
	}
	return pb.sendForward(args)
}

func (pb *PBServer) ProcessForward(args *ForwardArgs, reply *ForwardReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if !pb.isBackup() {
		return errors.New("Server is not a backup")
	}
	for key, value := range args.Content {
		pb.content[key] = value
	}
	return nil
}

func (pb *PBServer) isDuplicateRequest(client string, uid string) bool {
	return pb.content["seen."+client] == uid
}

func (pb *PBServer) applyHash(prevValue, value string, doHash bool) string {
	if doHash {
		return strconv.Itoa(int(hash(prevValue + value)))
	}
	return value
}

func (pb *PBServer) forwardPut(key, value, client, uid, prevValue string) error {
	forwardArgs := &ForwardArgs{
		Content: map[string]string{
			key:                  value,
			"seen." + client:     uid,
			"oldreply." + client: prevValue,
		},
	}
	return pb.Forward(forwardArgs)
}

func (pb *PBServer) commitPut(key, value, client, uid, prevValue string) {
	pb.content[key] = value
	pb.content["seen."+client] = uid
	pb.content["oldreply."+client] = prevValue
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.isPrimary() != true {
		reply.Err = ErrWrongServer
		return errors.New("not primary server")
	}

	client := args.Me
	uid := args.UUID
	key := args.Key
	value := args.Value

	if pb.isDuplicateRequest(client, uid) {
		reply.PreviousValue = pb.content["oldreply."+client]
		return nil
	}

	reply.PreviousValue = pb.content[key]
	value = pb.applyHash(reply.PreviousValue, value, args.DoHash)

	if err := pb.forwardPut(key, value, client, uid, reply.PreviousValue); err != nil {
		return err
	}

	pb.commitPut(key, value, client, uid, reply.PreviousValue)
	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.isPrimary() != true {
		reply.Err = ErrWrongServer
		return errors.New("backup received Get request")
	}
	reply.Value = pb.content[args.Key]
	return nil
}

func (pb *PBServer) tick() {
	pb.mu.Lock()
	view, err := pb.vs.Ping(pb.view.Viewnum)
	if err != nil {
		pb.view = view
		pb.mu.Unlock()
		return
	}

	needForward := view.Backup != "" && view.Backup != pb.view.Backup && pb.isPrimary()

	pb.view = view
	if needForward {
		pb.Forward(&ForwardArgs{Content: pb.content})
	}
	pb.mu.Unlock()
}

func (pb *PBServer) kill() {
	pb.dead = true
	pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.finish = make(chan interface{})
	pb.view = viewservice.View{}
	pb.content = make(map[string]string)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	//fmt.Printf("[StartServer] Server %v started and listening...\n", pb.me)

	go func() {
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63()%1000) < 100 {
					conn.Close()
				} else if pb.unreliable && (rand.Int63()%1000) < 200 {
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				} else {
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.dead == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
		DPrintf("%s: wait until all request are done\n", pb.me)
		pb.done.Wait()
		close(pb.finish)
	}()

	pb.done.Add(1)
	go func() {
		for pb.dead == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
		pb.done.Done()
	}()

	return pb
}
