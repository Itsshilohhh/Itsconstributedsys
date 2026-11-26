package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// 任务类型。GetTaskReply.Kind 用这个表示"这一轮给你什么"。
const (
	NoTask     = 0 // 暂时没有可发的任务，worker 等一会儿再来
	MapTask    = 1
	ReduceTask = 2
	ExitTask   = 3 // 全部完成，worker 可以退出
)

// GetTask 不需要参数，但 RPC 要求有 args。
type GetTaskArgs struct{}

type GetTaskReply struct {
	Kind    int
	TaskID  int
	File    string // map 任务：要读的输入文件
	NReduce int    // map 任务：要分成几个中间文件
	NMap    int    // reduce 任务：要读 0..NMap-1 这些 map 的输出
	Attempt int    // 第几轮派发，汇报时要原样带回去
}

type ReportTaskArgs struct {
	Kind    int
	TaskID  int
	Attempt int // 汇报的是第几轮派发，防止过期汇报被误信
}

type ReportTaskReply struct{}
