package mr

import (
	"sync"
	"time"
)

type Task struct {
	filename           string
	is_map             bool
	is_reduce          bool
	map_task_number    int
	reduce_task_number int
	time               time.Time
	n_reduce           int
}

type TaskTable struct {
	taskmap map[string]Task
	mu      sync.Mutex
}

type TaskQueue struct {
	queue []Task
	mu    sync.Mutex
}
