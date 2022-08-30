package mr

import (
	"fmt"
	"time"
)

func LoadMapTasks(c *Coordinator, files []string, nReduce int) {
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
}

func LoadReduceTasks(c *Coordinator) {
	if !c.mapphaseover {
		c.mapphaseover = true
		reduce_tasks := make([]*Task, 0)
		c.intermediate_files.mu.Lock()
		task_num := 0
		for _, file_arr := range c.intermediate_files.files {
			task := Task{
				intermedite_files:  file_arr,
				is_reduce:          true,
				is_map:             false,
				reduce_task_number: task_num,
				n_reduce:           c.reduceJobs,
				is_assigned:        false,
				is_finished:        false,
			}
			reduce_tasks = append(reduce_tasks, &task)
			task_num += 1
		}
		c.intermediate_files.mu.Unlock()

		c.taskTable.mu.Lock()
		for _, task := range reduce_tasks {
			task_id := fmt.Sprintf("reducetask-%d", task.reduce_task_number)
			c.taskTable.taskmap[task_id] = task
		}
		c.taskTable.mu.Unlock()

	}
}

func Iniate_tearDown(c *Coordinator) {
	if !c.reducephaseover {
		c.reducephaseover = true
		c.mapreduceover.mu.Lock()
		c.mapreduceover.initateTearDown = true
		c.mapreduceover.mu.Unlock()
		pseudo_task := Task{
			is_pseudo: true,
		}
		c.taskQueue.mu.Lock()
		c.taskQueue.queue = append(c.taskQueue.queue, pseudo_task)
		c.taskQueue.mu.Unlock()
	}
}

func ScheduleTasks(c *Coordinator) (done_maps int, done_reduces int) {
	done_maps = 0
	done_reduces = 0
	zero_value := time.Time{}

	unassigned_tasks := make([]Task, 0)
	// load task queue for map tasks
	c.taskTable.mu.Lock()
	for _, task := range c.taskTable.taskmap {
		elapsed_time := time.Since(task.time_assigned)
		if task.time_assigned != zero_value && !task.is_finished && elapsed_time.Seconds() > 10 {
			unassigned_tasks = append(unassigned_tasks, *task)
			task.time_assigned = time.Now()
		}
		if !task.is_assigned && !task.is_finished {
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
		c.taskQueue.queue = append(c.taskQueue.queue, unassigned_tasks...)
		c.taskQueue.mu.Unlock()
	}

	return done_maps, done_reduces
}

func CheckTasksDone(c *Coordinator) bool {
	var tasksDone bool
	c.mapreduceover.mu.Lock()
	tasksDone = c.mapreduceover.initateTearDown
	c.mapreduceover.mu.Unlock()
	return tasksDone
}

func CheckWorkersAndDeactivate(c *Coordinator) bool {
	// check if all workers are dead if not deactivate non-responding workers
	// or wait till they reach 10s of non-response
	c.workers.mu.Lock()
	if len(c.workers.activeworkers) == 0 {
		c.workers.mu.Unlock()
		return false
	}
	for _, worker := range c.workers.activeworkers {
		lapsed := time.Since(worker.time_assigned)
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
