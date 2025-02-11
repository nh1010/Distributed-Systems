package viewservice

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	// Your declarations here.
	times        map[string]time.Time
	views        map[uint]View
	current_view uint
	ack_primary  uint
}

func (vs *ViewServer) handlePrimaryFailure(view *View) {
	t_primary, primary_exists := vs.times[view.Primary]

	if primary_exists && time.Since(t_primary) > PingInterval*DeadPings {
		newView := View{
			Viewnum: view.Viewnum + 1,
			Primary: view.Backup,
		}

		newView.Backup = findBackup(vs.times, view)

		vs.current_view = newView.Viewnum
		vs.views[vs.current_view] = newView
	}
}

func (vs *ViewServer) handleBackupFailure(view *View) {
	t_backup, backup_existsists := vs.times[view.Backup]

	if backup_existsists && time.Since(t_backup) > PingInterval*DeadPings {
		newView := View{
			Viewnum: view.Viewnum + 1,
			Primary: view.Primary,
		}

		newView.Backup = findBackup(vs.times, view)

		vs.current_view = newView.Viewnum
		vs.views[vs.current_view] = newView
	}
}

func findBackup(times map[string]time.Time, view *View) string {
	for server, last_ping := range times {
		if time.Since(last_ping) < PingInterval*DeadPings && server != view.Primary && server != view.Backup {
			return server
		}
	}
	return ""
}

func UpdateView(view *View, server string, clientnum uint) View {
	updated_view := View{
		Viewnum: view.Viewnum,
		Primary: view.Primary,
		Backup:  view.Backup,
	}

	changed := false
	if clientnum == 0 && view.Viewnum > 1 && server == view.Primary {
		view.Primary = ""
	}

	if len(view.Primary) == 0 {
		updated_view.Primary = view.Backup
		if len(view.Backup) == 0 {
			updated_view.Primary = server
		} else {
			updated_view.Backup = server
		}
		changed = true
	} else if len(view.Backup) == 0 && view.Primary != server {
		updated_view.Backup = server
		changed = true
	}

	if changed {
		updated_view.Viewnum = view.Viewnum + 1
	}

	return updated_view
}

// server Ping RPC handler.
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	vs.times[args.Me] = time.Now()

	view := vs.views[vs.current_view]

	if view.Primary == args.Me && view.Viewnum == args.Viewnum {
		vs.ack_primary = view.Viewnum
	}

	newView := UpdateView(&view, args.Me, args.Viewnum)

	if newView.Viewnum == vs.current_view || newView.Viewnum > vs.ack_primary+1 {
		reply.View = view
	} else {
		vs.current_view = newView.Viewnum
		vs.views[vs.current_view] = newView
		reply.View = newView
	}

	return nil
}

// server Get() RPC handler.
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	reply.View = vs.views[vs.current_view]

	return nil
}

// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
func (vs *ViewServer) tick() {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	view := vs.views[vs.current_view]

	// Skip if primary hasn't acknowledged the current view
	if vs.ack_primary < vs.current_view {
		return
	}

	vs.handlePrimaryFailure(&view)
	vs.handleBackupFailure(&view)
}

// tell the server to shut itself down.
// for testing.
// please don't change this function.
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.times = make(map[string]time.Time)
	vs.views = make(map[uint]View)
	vs.current_view = 0
	vs.views[vs.current_view] = View{}

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
