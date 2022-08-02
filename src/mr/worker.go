package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		// make rpc , wait for reply
		CallGetTask(mapf, reducef)
		time.Sleep(time.Second * 3)
	}

}

func callMap(response *Response, mapf func(string, string) []KeyValue) {
	data, e := os.ReadFile(response.Filename)
	if e != nil {
		fmt.Println("error in reading file", e)
		return
	}

	kva := mapf(response.Filename, string(data))

	// now store them in nReduce buckets
	nReduce := response.N_Reduce

	buckets := make([][]KeyValue, 10)

	// distributing key values in buckets
	for _, kv := range kva {
		bucket := ihash(kv.Key) % nReduce
		buckets[bucket] = append(buckets[bucket], kv)
	}

	// writing buckets to intermiediate files
	intermediate_files := make([]string, nReduce)
	for i, bucket := range buckets {
		// sort.Sort(ByKey(bucket))
		intermediate_file_name := "mr-%d-%d.json"
		output_file_name := fmt.Sprintf(intermediate_file_name, ihash(response.Filename), i)
		output_file, e := os.Create(filepath.Join("mr-tmp", output_file_name))
		if e != nil {
			fmt.Println("Crashed while writing")
			return
		}
		enc := json.NewEncoder(output_file)

		for _, kv := range bucket {
			enc.Encode(kv)
		}

		intermediate_files = append(intermediate_files, intermediate_file_name)
		output_file.Close()
	}

	// after task is successful make a RPC call to post result
	CallPostResult(&intermediate_files, true, false)

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//

func CallPostResult(intermiediate_files *[]string, isMap bool, isReduce bool) {
	request := TaskResult{}
	response := Response{}

	request.Intermediate_Files = *intermiediate_files
	request.Success = true
	request.IsMap = isMap
	request.IsReduce = isReduce

	ok := call("Coordinator.PostResult", &request, &response)

	if !ok {
		fmt.Println("GetTask RPC recieved no response.")
	} else {
		// callReduce(response, reducef)
		fmt.Println("Result posted.")
	}
}

func CallGetTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	request := TaskRequest{Ready: true}
	response := Response{}

	ok := call("Coordinator.GetTask", &request, &response)

	if !ok {
		fmt.Println("GetTask RPC recieved no response.")
	} else if response.Is_Map {
		// read file
		callMap(&response, mapf)
	} else {
		// callReduce(response, reducef)
		fmt.Println("Reduce time")
	}

}
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
