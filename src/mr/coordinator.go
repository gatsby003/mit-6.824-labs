package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Coordinator struct {
	taskTable          TaskTable
	intermediate_files IntermediateFiles
	taskQueue          TaskQueue
	workers            WorkerTable
	reduceJobs         int
	mapJobs            int
	mapphaseover       bool
	reducephaseover    bool
	mapreduceover      Status
}

func initiateCoordinator(mapJobs int, reduceJobs int) *Coordinator {
	// initialize map table
	c := Coordinator{}
	c.workers = WorkerTable{activeworkers: make(map[string]*WorkerStatus)}
	c.taskTable = TaskTable{taskmap: make(map[string]*Task)}
	c.taskQueue = TaskQueue{queue: make([]Task, 0)}
	c.reduceJobs = reduceJobs
	c.mapJobs = mapJobs
	c.mapreduceover = Status{
		initateTearDown: false,
	}
	c.intermediate_files = IntermediateFiles{files: make([][]string, 10)}
	return &c
}

// types : TaskTable, Status
// method : populateTables

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (c *Coordinator) GetTask(request *TaskRequest, response *Response) error {
	// check task queue or fail
	c.taskQueue.mu.Lock()
	if len(c.taskQueue.queue) != 0 {
		c.workers.mu.Lock()
		worker_id := request.WorkerID
		response.TaskAlloted = true
		// deque a task
		task := c.taskQueue.queue[0]
		if task.is_pseudo {
			worker, exists := c.workers.activeworkers[worker_id]
			if exists {
				worker.active = false
			}
			response.PleaseExit = true
		} else {
			c.workers.activeworkers[worker_id] = &WorkerStatus{
				active:        true,
				time_assigned: time.Now(),
			}
			c.taskQueue.queue = c.taskQueue.queue[1:]
			// fill response object
			response.Filename = task.filename
			response.IntermediateFiles = task.intermedite_files
			response.Is_Map = task.is_map
			response.Is_Reduce = task.is_reduce
			response.N_Reduce = task.n_reduce
			response.Map_Task_Number = task.map_task_number
			response.Reduce_Task_Number = task.reduce_task_number

			// assign task time
			var task_key string
			if task.is_map {
				task_key = fmt.Sprintf("maptask-%d", task.map_task_number)
			} else if task.is_reduce {
				task_key = fmt.Sprintf("reducetask-%d", task.reduce_task_number)
			}

			c.taskTable.mu.Lock()
			c.taskTable.taskmap[task_key].time_assigned = time.Now()
			c.taskTable.taskmap[task_key].present_in_queue = false
			c.taskTable.mu.Unlock()
		}
		c.workers.mu.Unlock()
	}
	c.taskQueue.mu.Unlock()
	return nil
}

func (c *Coordinator) PostResult(request *TaskResult, response *Response) error {
	// getting result, store files for reduce
	if request.IsMap {
		c.intermediate_files.mu.Lock()
		for i, file := range request.Intermediate_Files {
			c.intermediate_files.files[i] = append(c.intermediate_files.files[i], file)
		}
		c.intermediate_files.mu.Unlock()

		// mark task as complete
		defer func(request *TaskResult, c *Coordinator) {
			c.taskTable.mu.Lock()
			map_task_number := request.Map_Task_Number
			task_id := fmt.Sprintf("maptask-%d", map_task_number)
			task := c.taskTable.taskmap[task_id]
			task.is_assigned = false
			task.is_finished = true
			c.taskTable.mu.Unlock()
		}(request, c)
	} else if request.IsReduce {
		defer func(request *TaskResult, c *Coordinator) {
			c.taskTable.mu.Lock()
			reduce_task_number := request.Reduce_Task_Number
			task_id := fmt.Sprintf("reducetask-%d", reduce_task_number)
			task := c.taskTable.taskmap[task_id]
			task.is_assigned = false
			task.is_finished = true
			fmt.Println("Reduce Done", task.reduce_task_number)
			c.taskTable.mu.Unlock()
		}(request, c)
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//

func (c *Coordinator) Done() bool {

	if CheckTasksDone(c) {
		// check workers , and deactivate non-responding workers
		done := CheckWorkersAndDeactivate(c)
		return done
	} else {
		return false
	}

}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := initiateCoordinator(len(files), nReduce)

	go Scheduler(c, files, nReduce)

	c.server()

	return c
}

// Scheduler
func Scheduler(c *Coordinator, files []string, nReduce int) {
	// Schedular : periodically loads tasks in task queue,
	// 			   exits when all tasks are either done or have exhausted retries
	//			   changes the mapPhase and reducePhase to True before quitting

	// load map tasks to taskTable
	LoadMapTasks(c, files, nReduce)

	for {
		// schedule tasks from task table into task queue , also manage rescheduling
		done_maps, done_reduces := ScheduleTasks(c)

		if done_maps == c.mapJobs && !c.mapphaseover {
			// map phase done , now load all reduce jobs in table
			LoadReduceTasks(c)
		}

		if c.mapJobs == done_maps && c.reduceJobs == done_reduces && !c.reducephaseover {
			// map jobs and reduce jobs over now load pseudo task to initiate teardown
			Iniate_tearDown(c)
		}

	}
}
