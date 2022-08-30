package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
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
		exit := CallGetTask(mapf, reducef)
		if exit {
			break
		}
		time.Sleep(time.Second * 3)
	}
	fmt.Println("Exiting")
}

func callReduce(response *Response, reducef func(string, []string) string) {
	files := response.IntermediateFiles
	key_values_map := make(map[string][]string, 0)

	for _, file := range files {
		fname := filepath.Join(file)
		f, err := os.Open(fname)
		if err != nil {
			fmt.Errorf("Error hundi si")
		}
		// decode json to key : value list
		d := json.NewDecoder(f)
		for {
			var v KeyValue
			if err := d.Decode(&v); err == io.EOF {
				break // done decoding file
			} else if err != nil {
				fmt.Println("Intermediate file reading error.")
			}

			if _, ok := key_values_map[v.Key]; ok {
				key_values_map[v.Key] = append(key_values_map[v.Key], v.Value)
			} else {
				key_values_map[v.Key] = make([]string, 0)
				key_values_map[v.Key] = append(key_values_map[v.Key], v.Value)
			}

		}
	}

	result := make([]KeyValue, 0)
	for k, v := range key_values_map {
		count := reducef(k, v)
		result = append(result, KeyValue{
			Key:   k,
			Value: count,
		})
	}
	sort.Sort(ByKey(result))

	output_file_name := fmt.Sprintf("mr-out-%d", response.Reduce_Task_Number)
	output_file_path := filepath.Join(output_file_name)

	// // discard if output file already exists
	// if _, err := os.Stat(output_file_path); err == nil {
	// 	os.Remove(output_file_name)
	// }

	// create tmp file
	tmpFile, err := ioutil.TempFile("", "*_mr-reduce")
	if err != nil {
		log.Fatal(err)
	}

	w := bufio.NewWriter(tmpFile)

	for _, v := range result {
		w.WriteString(v.Key + " " + v.Value + "\n")
	}

	w.Flush()

	// move tmp file to permanent file
	os.Rename(tmpFile.Name(), output_file_path)

	// close it
	tmpFile.Close()

	fmt.Println("Final Output", output_file_name)

	PostReduceResult(output_file_name, response.Reduce_Task_Number)

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
	intermediate_files := make([]string, 0)
	for i, bucket := range buckets {
		// sort.Sort(ByKey(bucket))
		intermediate_file_name := "mr-%d-%d.json"
		output_file_name := fmt.Sprintf(intermediate_file_name, response.Map_Task_Number, i)
		output_file_path := filepath.Join(output_file_name)

		// discard if output file already exists
		// if _, err := os.Stat(output_file_path); err == nil {
		// 	os.Remove(output_file_name)
		// }

		tmpFile, err := ioutil.TempFile("", "*_mr-map.json")
		if err != nil {
			log.Fatal(err)
		}

		if e != nil {
			fmt.Println("Crashed while writing")
			return
		}
		enc := json.NewEncoder(tmpFile)

		for _, kv := range bucket {
			enc.Encode(kv)
		}

		os.Rename(tmpFile.Name(), output_file_path)
		tmpFile.Close()

		intermediate_files = append(intermediate_files, output_file_path)
	}

	// after task is successful make a RPC call to post result
	CallPostResult(&intermediate_files, true, false, response.Map_Task_Number)

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//

func PostReduceResult(outputfile string, taskNumber int) {
	request := TaskResult{
		Success:            true,
		Final_Output:       outputfile,
		Reduce_Task_Number: taskNumber,
		IsReduce:           true,
		IsMap:              false,
	}

	response := Response{}

	ok := call("Coordinator.PostResult", &request, &response)

	if !ok {
		fmt.Println("Post Result received no response.")
	}
}

func CallPostResult(intermiediate_files *[]string, isMap bool, isReduce bool, taskNumber int) {
	// for map tasks
	request := TaskResult{}
	response := Response{}

	request.Intermediate_Files = *intermiediate_files
	request.Success = true
	request.IsMap = isMap
	request.IsReduce = isReduce
	request.Map_Task_Number = taskNumber

	ok := call("Coordinator.PostResult", &request, &response)

	if !ok {
		fmt.Println("Post Result received no response.")
	}
}

func CallGetTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) bool {

	request := TaskRequest{
		Ready:    true,
		WorkerID: strconv.Itoa(os.Getpid()),
	}
	response := Response{}

	ok := call("Coordinator.GetTask", &request, &response)

	if !ok {
		fmt.Println("GetTask RPC recieved no response.")
	} else if response.Is_Map {
		// read file
		callMap(&response, mapf)
	} else if response.Is_Reduce {
		callReduce(&response, reducef)
	} else if response.PleaseExit {
		return true
	}

	return false

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
