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
	mapreduceover      chan bool
}

func initiateCoordinator(mapJobs int, reduceJobs int) *Coordinator {
	// initialize map table
	c := Coordinator{}
	c.workers = WorkerTable{activeworkers: make(map[string]*WorkerStatus)}
	c.taskTable = TaskTable{taskmap: make(map[string]*Task)}
	c.taskQueue = TaskQueue{queue: make([]Task, 0)}
	c.reduceJobs = reduceJobs
	c.mapJobs = mapJobs
	c.mapreduceover = make(chan bool)
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
	fmt.Println("Get task called")
	c.taskQueue.mu.Lock()
	if len(c.taskQueue.queue) != 0 {
		fmt.Println("Inside Get Task")
		c.workers.mu.Lock()
		worker_id := request.WorkerID
		response.TaskAlloted = true
		// deque a task
		task := c.taskQueue.queue[0]
		fmt.Println(task.is_pseudo)
		if task.is_pseudo {
			worker, exists := c.workers.activeworkers[worker_id]
			if exists {
				worker.active = false
			}
			fmt.Println("Worker Deactivate", worker_id)
			response.PleaseExit = true
		} else {
			fmt.Println("assigning task")
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
			fmt.Println("Marked Map Task Finished", task_id)
		}(request, c)
	} else if request.IsReduce {
		defer func(request *TaskResult, c *Coordinator) {
			c.taskTable.mu.Lock()
			reduce_task_number := request.Reduce_Task_Number
			task_id := fmt.Sprintf("reducetask-%d", reduce_task_number)
			task := c.taskTable.taskmap[task_id]
			task.is_assigned = false
			task.is_finished = true
			c.taskTable.mu.Unlock()
			fmt.Println("Marked Reduce Task as finished", task_id)
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
	c.workers.mu.Lock()
	// first run status check
	if len(c.workers.activeworkers) == 0 {
		c.workers.mu.Unlock()
		return false
	}
	for _, worker := range c.workers.activeworkers {
		lapsed := time.Since(worker.time_assigned)
		fmt.Println(lapsed.Seconds(), worker.active)
		if lapsed.Seconds() < 10 && worker.active {
			c.workers.mu.Unlock()
			return false
		} else {
			worker.active = false
		}
	}

	c.workers.mu.Unlock()
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := initiateCoordinator(len(files), nReduce)
	// Your code here.
	// Schedular : periodically loads tasks in task queue,
	// 			   exits when all tasks are either done or have exhausted retries
	//			   changes the mapPhase and reducePhase to True before quitting

	// load map tasks in mapTable
	c.taskTable.mu.Lock()
	for i := 0; i < len(files); i++ {
		task := Task{
			filename:        files[i],
			is_map:          true,
			n_reduce:        nReduce,
			map_task_number: i,
		}
		task_id := fmt.Sprintf("maptask-%d", i)
		c.taskTable.taskmap[task_id] = &task
	}
	fmt.Println("MapTable Loaded")
	c.taskTable.mu.Unlock()

	go Scheduler(c)

	c.server()

	return c
}

// Scheduler
func Scheduler(c *Coordinator) {
	// load tasks to task queue
	// map tasks
	for {
		done_maps := 0
		done_reduces := 0
		zero_value := time.Time{}

		unassigned_tasks := make([]Task, 0)
		// load task queue for map tasks
		c.taskTable.mu.Lock()
		for _, task := range c.taskTable.taskmap {
			elapsed_time := time.Since(task.time_assigned)
			if task.time_assigned != zero_value && !task.is_finished && elapsed_time.Seconds() > 10 {
				fmt.Println("Task Rescheduled", elapsed_time.Seconds())
				unassigned_tasks = append(unassigned_tasks, *task)
				task.time_assigned = time.Now()
			}
			if !task.is_assigned && !task.is_finished {
				fmt.Println("Task Added", task.filename)
				task.is_assigned = true
				unassigned_tasks = append(unassigned_tasks, *task)
			} else if task.is_map && task.is_finished {
				done_maps += 1
			} else if task.is_reduce && task.is_finished {
				done_reduces += 1
			}
		}
		c.taskTable.mu.Unlock()
		if len(unassigned_tasks) > 0 {
			c.taskQueue.mu.Lock()
			fmt.Println("Loading tasks in task queue")
			c.taskQueue.queue = append(c.taskQueue.queue, unassigned_tasks...)
			c.taskQueue.mu.Unlock()
		}

		if c.mapJobs == done_maps && c.reduceJobs == done_reduces && !c.reducephaseover {
			// before adding psudo task check if all reduce and map jobs are done

			if !c.reducephaseover {
				c.reducephaseover = true
				fmt.Println("Map and reduce phase done", done_maps, done_reduces)

				pseudo_task := Task{
					is_pseudo: true,
				}
				c.taskQueue.mu.Lock()
				c.taskQueue.queue = append(c.taskQueue.queue, pseudo_task)
				c.taskQueue.mu.Unlock()
			}
		}

		if done_maps == c.mapJobs && !c.mapphaseover {
			// map phase done , now load all reduce jobs in queue

			if !c.mapphaseover {
				c.mapphaseover = true
				reduce_tasks := make([]Task, 0)
				c.intermediate_files.mu.Lock()
				for i, file_arr := range c.intermediate_files.files {
					task := Task{
						intermedite_files:  file_arr,
						is_reduce:          true,
						is_map:             false,
						reduce_task_number: i,
						n_reduce:           c.reduceJobs,
						is_assigned:        false,
						is_finished:        false,
					}
					reduce_tasks = append(reduce_tasks, task)
				}
				c.intermediate_files.mu.Unlock()

				c.taskTable.mu.Lock()
				for _, task := range reduce_tasks {
					task_id := fmt.Sprintf("reducetask-%d", task.reduce_task_number)
					c.taskTable.taskmap[task_id] = &task
				}
				c.taskTable.mu.Unlock()

				c.taskQueue.mu.Lock()
				c.taskQueue.queue = append(c.taskQueue.queue, reduce_tasks...)
				c.taskQueue.mu.Unlock()
			}

		}

	}
}
