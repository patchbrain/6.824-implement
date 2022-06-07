package mr

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	NumReduce          int
	InputFilePaths     []string // 传入map worker的文件路径
	Phase              string
	MapTasks           []MapTask
	ReduceTasks        []ReduceTask
	InitDone           bool
	MapDone            bool
	ReduceDone         bool
	MapWorkerCount     int
	MapDoneCount       int
	ReduceWorkerCount  int
	ReduceDoneCount    int
	PubMapTaskCount    int // 发布的map任务数量
	PubReduceTaskCount int // 发布的reduce任务数量
	rwMutex            *sync.RWMutex
}

type MapTask struct {
	TaskID    int       // task标识
	InputFile string    // input文件地址
	Done      bool      // 该任务是否完成
	StartTime time.Time // 记录任务分发的时间
	InProcess bool      // 是否正在执行
}

type ReduceTask struct {
	TaskID      int       // task标识
	ReduceIndex int       // reduce worker要读的文件编号
	OutputFile  string    // reduce output文件地址
	Done        bool      // 该任务是否完成
	StartTime   time.Time // 记录任务分发的时间
	InProcess   bool      // 是否正在执行
}

// Your code here -- RPC handlers for the worker to call.
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// map任务是否全部完成
func (c *Coordinator) MapIsAllDone(args *AllMapDoneArgs, reply *AllMapDoneReply) error {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()
	c.MapDone = true
	for _, task := range c.MapTasks {
		if task.Done == false {
			c.MapDone = false
			break
		}
	}

	reply.Isok = c.MapDone
	return nil
}

// reduce任务是否全部完成
func (c *Coordinator) ReduceIsAllDone(args *AllReduceDoneArgs, reply *AllReduceDoneReply) error {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()
	c.ReduceDone = true
	for _, task := range c.ReduceTasks {
		if task.Done == false {
			c.ReduceDone = false
			break
		}
	}

	reply.Isok = c.ReduceDone
	return nil
}

// 一个map worker完成了任务
func (c *Coordinator) MapWorkerDone(args *MapWorkerDoneArgs, reply *MapWorkerDoneReply) error {
	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()

	index := args.Id - 1000
	c.MapDoneCount++
	c.MapTasks[index].Done = true
	c.MapTasks[index].InProcess = false

	reply.Isok = true
	return nil
}

// 一个reduce worker完成了任务
func (c *Coordinator) ReduceWorkerDone(args *ReduceWorkerDoneArgs, reply *ReduceWorkerDoneReply) error {
	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()

	index := args.Id - 2000
	c.ReduceDoneCount++
	c.ReduceTasks[index].Done = true
	c.ReduceTasks[index].InProcess = false

	reply.Isok = true
	return nil
}

// 获取完成的map数量
func (c *Coordinator) OfferMapDoneCount(args *GetMapDoneCountArgs, reply *GetMapDoneCountReply) error {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()

	reply.Num = c.MapDoneCount
	return nil
}

// 获取完成的reduce数量
func (c *Coordinator) OfferReduceDoneCount(args *GetMapDoneCountArgs, reply *GetMapDoneCountReply) error {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()

	reply.Num = c.ReduceDoneCount
	return nil
}

// 获取nReduce
func (c *Coordinator) OffernReduce(args *GetnReduceArgs, reply *GetnReduceReply) error {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()

	reply.NumReduce = c.NumReduce
	return nil
}

// 获取nReduce
func (c *Coordinator) OffernMap(args *GetnReduceArgs, reply *GetnMapReply) error {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()

	reply.NumMap = len(c.InputFilePaths)
	return nil
}

// 发布一个map task
func (c *Coordinator) OfferMapTask(args *GetMapTaskArgs, reply *GetMapTaskReply) error {
	// 发布map任务
	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()

	if c.MapDone {
		return errors.New("map tasks are all done")
	}

	if c.PubMapTaskCount >= len(c.MapTasks) {
		return errors.New("coordinator has no map Task to offer")
	}
	var t *MapTask
	// 找一个任务
	for i := range c.MapTasks {
		if !c.MapTasks[i].Done && !c.MapTasks[i].InProcess {
			t = &c.MapTasks[i]
			break
		}
	}

	if t != nil {
		t.InProcess = true
		t.StartTime = time.Now()
		c.PubMapTaskCount++ // 发布数量+1
	}

	reply.Task = *t
	return nil
}

// 发布一个reduce task
func (c *Coordinator) OfferReduceTask(args *GetReduceTaskArgs, reply *GetReduceTaskReply) error {
	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()

	if c.ReduceDone {
		return errors.New("reduce tasks are all done")
	}

	// 发布reduce任务
	if c.PubReduceTaskCount >= c.NumReduce {
		return errors.New("coordinator has no reduce Task to offer")
	}
	var t *ReduceTask
	// 找一个任务
	for i := range c.ReduceTasks {
		if !c.ReduceTasks[i].Done && !c.ReduceTasks[i].InProcess {
			t = &c.ReduceTasks[i]
			break
		}
	}

	if t != nil {
		t.StartTime = time.Now()
		t.InProcess = true
		c.PubReduceTaskCount++ // 发布数量+1
	}

	reply.Task = *t
	return nil
}

// internal function

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
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()
	ret := c.MapDone && c.ReduceDone

	// todo: 判断任务是否已经完成
	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NumReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.InputFilePaths = make([]string, len(files))
	copy(c.InputFilePaths, files)
	c.NumReduce = nReduce
	c.rwMutex = &sync.RWMutex{}
	initTask(&c)
	c.InitDone = true // 代表初始化完成

	go checkTimeout(&c)
	c.server()
	return &c
}

func initTask(c *Coordinator) {
	c.MapTasks = make([]MapTask, len(c.InputFilePaths))
	c.ReduceTasks = make([]ReduceTask, c.NumReduce)

	for i := 0; i < len(c.MapTasks); i++ {
		c.MapTasks[i].TaskID = 1000 + i
		c.MapTasks[i].InputFile = c.InputFilePaths[i]
	}

	for i := 0; i < c.NumReduce; i++ {
		c.ReduceTasks[i].TaskID = 2000 + i
		c.ReduceTasks[i].ReduceIndex = i
		c.ReduceTasks[i].OutputFile = fmt.Sprintf("mr-out-%d", i)
	}
}

func checkTimeout(c *Coordinator) {
	for {
		c.rwMutex.Lock()
		if !c.MapDone {
			for i := range c.MapTasks {
				if c.MapTasks[i].Done || !c.MapTasks[i].InProcess {
					continue
				}

				duration := time.Now().Second() - c.MapTasks[i].StartTime.Second()
				// 超过10s，认为任务超时
				if duration > 10 {
					c.PubMapTaskCount--
					c.MapDone = false
					c.MapTasks[i].InProcess = false
				}
			}
		}

		if !c.ReduceDone {
			for i := range c.ReduceTasks {
				if c.ReduceTasks[i].Done || !c.ReduceTasks[i].InProcess {
					continue
				}

				duration := time.Now().Second() - c.ReduceTasks[i].StartTime.Second()
				// 超过10s，认为任务超时
				if duration > 10 {
					c.PubReduceTaskCount--
					c.ReduceDone = false
					c.ReduceTasks[i].InProcess = false
				}
			}
		}
		c.rwMutex.Unlock()

	}
}
