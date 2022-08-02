package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	mapTable        TaskTable
	reduceTable     TaskTable
	mapPhaseOver    bool
	reducePhaseOver bool
	taskQueue       TaskQueue
}

func initiateCoordinator() *Coordinator {
	// initialize map table
	c := Coordinator{}
	c.mapTable = TaskTable{taskmap: make(map[string]Task)}
	c.reduceTable = TaskTable{taskmap: make(map[string]Task)}
	c.mapPhaseOver = false
	c.reducePhaseOver = false
	c.taskQueue = TaskQueue{queue: make([]Task, 0)}

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
		response.TaskAlloted = true
		// deque a task
		task := c.taskQueue.queue[0]
		c.taskQueue.queue = c.taskQueue.queue[1:]
		// fill response object
		response.Filename = task.filename
		response.Is_Map = task.is_map
		response.Is_Reduce = task.is_reduce
		response.N_Reduce = task.n_reduce
		response.Map_Task_Number = task.map_task_number
		response.Reduce_Task_Number = task.reduce_task_number
		fmt.Println("Task Served", task, "Task Queue Length", len(c.taskQueue.queue))
	} else {
		response.TaskAlloted = false
	}
	c.taskQueue.mu.Unlock()
	return nil
}

func (c *Coordinator) PostResult(request *TaskResult, response *Response) error {
	// getting result
	if request.IsMap {
		c.reduceTable.mu.Lock()
		files := request.Intermediate_Files
		for i, file := range files {
			c.reduceTable.taskmap[file] = Task{
				filename:           file,
				is_map:             false,
				is_reduce:          true,
				reduce_task_number: i,
			}
		}
		c.reduceTable.mu.Unlock()
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
	ret := false

	// Your code here.
	// checks the two variables : mapPhaseDone , reducePhaseDone

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := initiateCoordinator()
	// Your code here.
	// Schedular : periodically loads tasks in task queue,
	// 			   exits when all tasks are either done or have exhausted retries
	//			   changes the mapPhase and reducePhase to True before quitting

	// load map tasks in queue
	c.taskQueue.mu.Lock()
	for i := 0; i < len(files); i++ {
		task := Task{
			filename:        files[i],
			is_map:          true,
			n_reduce:        nReduce,
			map_task_number: i,
		}
		c.taskQueue.queue = append(c.taskQueue.queue, task)
	}
	fmt.Println(len(files))
	fmt.Println("Task Queue ready", c.taskQueue.queue)
	c.taskQueue.mu.Unlock()
	c.server()
	return c
}
