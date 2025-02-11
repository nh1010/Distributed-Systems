package mapreduce

import (
	"container/list"
	"fmt"
)

type WorkerInfo struct {
	address         string // The network address of the worker
	active          bool   // To indicate if the worker is still active or has failed
	jobsDone        int    // The number of jobs this worker has completed
	JobName         string
	file            string
	nMap            int
	nReduce         int
	Workers         map[string]*WorkerInfo
	registerChannel chan string

	// You can add definitions here.
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	availableWorkers := make(chan string, len(mr.Workers))

	go func() {
		for {
			worker := <-mr.registerChannel
			mr.Workers[worker] = &WorkerInfo{
				address:  worker,
				active:   true,
				jobsDone: 0,
			}
			availableWorkers <- worker
		}
	}()

	mapTasks := make(chan int, mr.nMap)
	for i := 0; i < mr.nMap; i++ {
		mapTasks <- i
	}

	for i := 0; i < mr.nMap; i++ {
		worker := <-availableWorkers

		go func(worker string) {
			for task := range mapTasks {
				args := &DoJobArgs{
					File:          mr.file,
					Operation:     Map,
					JobNumber:     task,
					NumOtherPhase: mr.nReduce,
				}
				if ok := call(worker, "Worker.DoTask", args, nil); !ok {
					fmt.Printf("Map task %d failed on worker %s. Reassigning...\n", task, worker)
					mapTasks <- task
				} else {
					mr.Workers[worker].jobsDone++
					availableWorkers <- worker
				}
			}
		}(worker)
	}

	for i := 0; i < mr.nMap; i++ {
		<-mapTasks
	}

	reduceTasks := make(chan int, mr.nReduce)
	for i := 0; i < mr.nReduce; i++ {
		reduceTasks <- i
	}
	for i := 0; i < mr.nReduce; i++ {
		worker := <-availableWorkers
		go func(worker string) {
			for task := range reduceTasks {
				args := &DoJobArgs{
					File:          "",
					Operation:     Reduce,
					JobNumber:     task,
					NumOtherPhase: mr.nMap,
				}
				if ok := call(worker, "Worker.DoTask", args, nil); !ok {
					fmt.Printf("Reduce task %d failed on worker %s. Reassigning...\n", task, worker)
					reduceTasks <- task
				} else {
					mr.Workers[worker].jobsDone++
					availableWorkers <- worker
				}
			}
		}(worker)
	}

	for i := 0; i < mr.nReduce; i++ {
		<-reduceTasks
	}

	return mr.KillWorkers()
}
