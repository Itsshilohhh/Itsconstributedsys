package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// 一个任务多久没汇报就认为它的 worker 挂了，把任务回收重新派发。
// 必须大于任何正常任务的最长耗时：
//   - jobcount 的 Map 会 sleep 2~5 秒
//   - early_exit 的 Reduce 会 sleep 3 秒
//   - crash 的 Map/Reduce 最长会延迟 10 秒
const taskTimeout = 10 * time.Second

// 任务状态。
type TaskState int

const (
	Idle     TaskState = iota // 还没派发
	Running                   // 已经派给某个 worker，正在跑
	Finished                  // worker 汇报完成了
)

// 一个任务。map 和 reduce 共用这个结构体。
type Task struct {
	ID      int       // map: 0..nMap-1；reduce: 0..nReduce-1
	File    string    // 只有 map 用：输入文件名
	State   TaskState // 当前状态
	Start   time.Time // 派发时刻，用于判断是否超时
	Attempt int       // 第几次被派发，用来丢弃过期的汇报
}

type Coordinator struct {
	// Your definitions here.
	mu       sync.Mutex
	mapTasks []Task
	redTasks []Task
	nReduce  int
}

// Your code here -- RPC handlers for the worker to call.

// 一个 worker 来要任务。这是整个实验的核心：先发 map，map 全做完才发 reduce。
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.reclaimTimedOut()

	// 1. map 阶段：找一个还没派发的 map 任务。
	for i := range c.mapTasks {
		if c.mapTasks[i].State == Idle {
			return c.assign(&c.mapTasks[i], MapTask, reply)
		}
	}

	// 2. 还有 map 在跑：让 worker 等一会儿再来。
	for i := range c.mapTasks {
		if c.mapTasks[i].State == Running {
			reply.Kind = NoTask
			return nil
		}
	}

	// 3. 所有 map 都完成了，进入 reduce 阶段。
	for i := range c.redTasks {
		if c.redTasks[i].State == Idle {
			return c.assign(&c.redTasks[i], ReduceTask, reply)
		}
	}

	// 4. 还有 reduce 在跑：等。
	for i := range c.redTasks {
		if c.redTasks[i].State == Running {
			reply.Kind = NoTask
			return nil
		}
	}

	// 5. 全部完成。
	reply.Kind = ExitTask
	return nil
}

// assign 把任务 t 标记为正在执行，并填好给 worker 的回复。
// 调用时必须已经持有 c.mu。
func (c *Coordinator) assign(t *Task, kind int, reply *GetTaskReply) error {
	t.State = Running
	t.Start = time.Now()
	t.Attempt++

	reply.Kind = kind
	reply.TaskID = t.ID
	reply.File = t.File
	reply.NReduce = c.nReduce
	reply.NMap = len(c.mapTasks)
	reply.Attempt = t.Attempt
	return nil
}

// reclaimTimedOut 把长时间没汇报的任务打回 Idle，让别的 worker 重新做。
// 调用时必须已经持有 c.mu。
func (c *Coordinator) reclaimTimedOut() {
	now := time.Now()
	reclaim := func(tasks []Task) {
		for i := range tasks {
			if tasks[i].State == Running && now.Sub(tasks[i].Start) > taskTimeout {
				tasks[i].State = Idle
			}
		}
	}
	reclaim(c.mapTasks)
	reclaim(c.redTasks)
}

// worker 干完活来汇报。
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var tasks []Task
	switch args.Kind {
	case MapTask:
		tasks = c.mapTasks
	case ReduceTask:
		tasks = c.redTasks
	default:
		return nil
	}

	for i := range tasks {
		if tasks[i].ID != args.TaskID {
			continue
		}
		// 只有"正在跑"且"是同一轮派发"的汇报才算数。
		// 否则可能是某个被回收的慢 worker 迟到的汇报，不能采信。
		if tasks[i].State == Running && tasks[i].Attempt == args.Attempt {
			tasks[i].State = Finished
		}
		break
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, t := range c.mapTasks {
		if t.State != Finished {
			return false
		}
	}
	for _, t := range c.redTasks {
		if t.State != Finished {
			return false
		}
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{nReduce: nReduce}

	for i, file := range files {
		c.mapTasks = append(c.mapTasks, Task{
			ID:    i,
			File:  file,
			State: Idle,
		})
	}

	for i := 0; i < nReduce; i++ {
		c.redTasks = append(c.redTasks, Task{
			ID:    i,
			State: Idle,
		})
	}

	c.server(sockname)
	return &c
}
