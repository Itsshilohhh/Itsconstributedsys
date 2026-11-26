package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.

	for {
		args := GetTaskArgs{}
		reply := GetTaskReply{}
		if !call("Coordinator.GetTask", &args, &reply) {
			return
		}

		switch reply.Kind {
		case MapTask:
			if err := doMap(&reply, mapf); err != nil {
				log.Printf("map task %d failed: %v", reply.TaskID, err)
				return
			}
			report(MapTask, reply.TaskID, reply.Attempt)

		case ReduceTask:
			if err := doReduce(&reply, reducef); err != nil {
				log.Printf("reduce task %d failed: %v", reply.TaskID, err)
				return
			}
			report(ReduceTask, reply.TaskID, reply.Attempt)

		case NoTask:
			time.Sleep(200 * time.Millisecond)

		case ExitTask:
			return
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func doMap(task *GetTaskReply, mapf func(string, string) []KeyValue) error {
	contents, err := os.ReadFile(task.File)
	if err != nil {
		return fmt.Errorf("read %s: %w", task.File, err)
	}

	kva := mapf(task.File, string(contents))

	buckets := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		y := ihash(kv.Key) % task.NReduce
		buckets[y] = append(buckets[y], kv)
	}

	for y := 0; y < task.NReduce; y++ {
		if err := writeKVs(fmt.Sprintf("mr-%d-%d", task.TaskID, y), buckets[y]); err != nil {
			return err
		}
	}
	return nil
}

func doReduce(task *GetTaskReply, reducef func(string, []string) string) error {
	var intermediate []KeyValue
	for x := 0; x < task.NMap; x++ {
		kvs, err := readKVs(fmt.Sprintf("mr-%d-%d", x, task.TaskID))
		if err != nil {
			return err
		}
		intermediate = append(intermediate, kvs...)
	}

	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	tmp, err := os.CreateTemp(".", "mr-tmp-*")
	if err != nil {
		return err
	}
	w := bufio.NewWriter(tmp)

	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := make([]string, 0, j-i)
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		fmt.Fprintf(w, "%v %v\n", intermediate[i].Key, reducef(intermediate[i].Key, values))
		i = j
	}

	if err := w.Flush(); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	return os.Rename(tmp.Name(), fmt.Sprintf("mr-out-%d", task.TaskID))
}

func writeKVs(filename string, kvs []KeyValue) error {
	tmp, err := os.CreateTemp(".", "mr-tmp-*")
	if err != nil {
		return err
	}

	enc := json.NewEncoder(tmp)
	for _, kv := range kvs {
		if err := enc.Encode(&kv); err != nil {
			tmp.Close()
			os.Remove(tmp.Name())
			return err
		}
	}

	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	return os.Rename(tmp.Name(), filename)
}

func readKVs(filename string) ([]KeyValue, error) {
	f, err := os.Open(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	var kvs []KeyValue
	dec := json.NewDecoder(f)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err == io.EOF {
			break
		} else if err != nil {
			return nil, fmt.Errorf("decode %s: %w", filename, err)
		}
		kvs = append(kvs, kv)
	}
	return kvs, nil
}

// report 告诉 coordinator 某个任务已经完成。
func report(kind int, taskID int, attempt int) {
	args := ReportTaskArgs{Kind: kind, TaskID: taskID, Attempt: attempt}
	reply := ReportTaskReply{}
	call("Coordinator.ReportTask", &args, &reply)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
