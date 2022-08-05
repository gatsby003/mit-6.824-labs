package mr

import (
	"sync"
	"time"
)

type Task struct {
	filename           string
	intermedite_files  []string
	is_map             bool
	is_reduce          bool
	is_pseudo          bool
	map_task_number    int
	reduce_task_number int
	time_assigned      time.Time
	n_reduce           int
	is_assigned        bool
	is_finished        bool
}

type TaskTable struct {
	taskmap map[string]*Task
	mu      sync.Mutex
}

type WorkerTable struct {
	activeworkers map[string]bool
	mu            sync.Mutex
}

type TaskQueue struct {
	queue []Task
	mu    sync.Mutex
}

type IntermediateFiles struct {
	files [][]string
	mu    sync.Mutex
}
