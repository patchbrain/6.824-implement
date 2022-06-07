package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NumReduce to choose the reduce
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
	nReduce := CallGetnReduce()
	nMap := CallGetnMap()
	for {
		t, ok := CallGetMapTask()
		if ok {
			// 通知coordinator有一个map任务完成了
			doMap(t, mapf, nReduce)
			CallDoneMapWorker(t.TaskID)
			err := renameMapOut(t, nReduce)
			if err != nil {
				log.Fatal(err)
			}
		}

		CallGetMapDoneCount()
		allDone := CallCheckAllMapDone()
		if allDone {
			break
		}

		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("All map tasks done.")
	fmt.Println("Enter reduce phase.")

	for {
		t, ok := CallGetReduceTask()
		if ok {
			doReduce(t, reducef, nMap)
			// 通知coordinator有一个reduce任务完成了
			CallDoneReduceWorker(t.TaskID)
		}

		CallGetReduceDoneCount()
		allDone := CallCheckAllReduceDone()
		if allDone {
			break
		}

		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("All MapReduce tasks done.")
	fmt.Println("worker exits.")
}

// 将临时的map output file名称转换成正确的名称
func renameMapOut(t MapTask, nReduce int) error {
	for i := 0; i < nReduce; i++ {
		index := t.TaskID - 1000
		err := os.Rename(fmt.Sprintf("./map-out-%d-%d-temp", index, i),
			fmt.Sprintf("map-out-%d-%d", index, i))
		if err != nil {
			return err
		}
	}
	return nil
}

func doMap(t MapTask, mapf func(string, string) []KeyValue, num int) {
	inputFile := t.InputFile
	file, err := os.Open(inputFile)
	if err != nil {
		log.Fatalf("cannot open %v", inputFile)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", inputFile)
	}
	file.Close()
	kva := mapf(inputFile, string(content))
	sort.Sort(ByKey(kva))

	// 写入中间文件
	WriteToFile(kva, num, t.TaskID-1000)
}

// 将键值对写入文件
func WriteToFile(kvs []KeyValue, num, id int) {
	// num: 要写入的文件数量
	splitedKvs := make([][]KeyValue, num)
	for i := 0; i < num; i++ {
		splitedKvs[i] = make([]KeyValue, 0)
	}

	nKvs := len(kvs)
	for i := 0; i < nKvs; i++ {
		// 把kvs分别存储到多个中间文件中
		idx := ihash(kvs[i].Key) % num
		splitedKvs[idx] = append(splitedKvs[idx], kvs[i])
	}

	for i := 0; i < num; i++ {
		// 分别将splitedKvs写入文件
		oname := fmt.Sprintf("map-out-%d-%d-temp", id, i)

		file, err := os.Create(oname)
		defer file.Close()
		if err != nil {
			log.Fatal(err)
		}

		data, err := json.Marshal(&splitedKvs[i])
		if err != nil {
			log.Fatal(err)
		}

		file.Write(data)
	}
}

func doReduce(t ReduceTask, reducef func(string, []string) string, nMap int) {
	index := t.ReduceIndex
	kvs := []KeyValue{}
	for i := 0; i < nMap; i++ {
		tempKvs := []KeyValue{}
		filename := fmt.Sprintf("map-out-%d-%d", i, index)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}

		err = json.Unmarshal(content, &tempKvs)
		if err != nil {
			log.Fatalf("cannot unmarshal %v", filename)
		}
		kvs = append(kvs, tempKvs...)
	}

	sort.Sort(ByKey(kvs))

	oname := t.OutputFile
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(kvs) {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kvs[i].Key, output)

		i = j
	}

	ofile.Close()
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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
		//fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// 获取任务
func CallGetMapTask() (MapTask, bool) {
	args := GetMapTaskArgs{}
	reply := GetMapTaskReply{}

	ok := call("Coordinator.OfferMapTask", &args, &reply)
	if ok {
		//fmt.Printf("reply.Task %v\n", reply.Task)
	} else {
		fmt.Printf("call failed!\n")
	}

	return reply.Task, ok
}

func CallGetReduceTask() (ReduceTask, bool) {
	args := GetReduceTaskArgs{}
	reply := GetReduceTaskReply{}

	ok := call("Coordinator.OfferReduceTask", &args, &reply)
	if ok {
		//fmt.Printf("reply.Task %v\n", reply.Task)
	} else {
		fmt.Printf("call failed!\n")
	}

	return reply.Task, ok
}

// 通知有一个map worker已完成
func CallDoneReduceWorker(id int) bool {
	args := ReduceWorkerDoneArgs{Id: id}
	reply := ReduceWorkerDoneReply{}

	ok := call("Coordinator.ReduceWorkerDone", &args, &reply)
	if ok {
		//fmt.Printf("reply.Isok %v\n", reply.Isok)
	}

	return reply.Isok
}

// 通知有一个map worker已完成
func CallDoneMapWorker(id int) bool {
	args := MapWorkerDoneArgs{Id: id}
	reply := MapWorkerDoneReply{}

	ok := call("Coordinator.MapWorkerDone", &args, &reply)
	if ok {
		//fmt.Printf("reply.Isok %v\n", reply.Isok)
	}

	return reply.Isok
}

// 查询是否所有Map任务都已完成
func CallCheckAllMapDone() bool {
	args := AllMapDoneArgs{}
	reply := AllMapDoneReply{}

	ok := call("Coordinator.MapIsAllDone", &args, &reply)
	if ok {
		//fmt.Printf("reply.Isok %v\n", reply.Isok)
	}

	return reply.Isok
}

// 查询是否所有Map任务都已完成
func CallCheckAllReduceDone() bool {
	args := AllReduceDoneArgs{}
	reply := AllReduceDoneReply{}

	ok := call("Coordinator.ReduceIsAllDone", &args, &reply)
	if ok {
		//fmt.Printf("reply.Isok %v\n", reply.Isok)
	}

	return reply.Isok
}

// 查询Map完成数量
func CallGetMapDoneCount() int {
	args := GetMapDoneCountArgs{}
	reply := GetMapDoneCountReply{}

	ok := call("Coordinator.OfferMapDoneCount", &args, &reply)
	if ok {
		//fmt.Printf("reply.num %v\n", reply.Num)
	}

	return reply.Num
}

// 查询Reduce完成数量
func CallGetReduceDoneCount() int {
	args := GetReduceDoneCountArgs{}
	reply := GetReduceDoneCountReply{}

	ok := call("Coordinator.OfferReduceDoneCount", &args, &reply)
	if ok {
		//fmt.Printf("OfferReduceDoneCount reply.num %v\n", reply.Num)
	}

	return reply.Num
}

func CallGetnReduce() int {
	args := GetnReduceArgs{}
	reply := GetnReduceReply{}

	ok := call("Coordinator.OffernReduce", &args, &reply)
	if ok {
		//fmt.Printf("reply.NumReduce %v\n", reply.NumReduce)
	} else {
		fmt.Printf("call failed!\n")
	}

	return reply.NumReduce
}

func CallGetnMap() int {
	args := GetnMapArgs{}
	reply := GetnMapReply{}

	ok := call("Coordinator.OffernMap", &args, &reply)
	if ok {
		//fmt.Printf("reply.NumMap %v\n", reply.NumMap)
	} else {
		fmt.Printf("call failed!\n")
	}

	return reply.NumMap
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
