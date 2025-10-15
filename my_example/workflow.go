package my_example

import (
	"sync/atomic"
	"time"

	"go.temporal.io/sdk/workflow"
)

// SayHelloWorldWorkflow hello world流水线，作为第一个测试
func SayHelloWorldWorkflow(ctx workflow.Context, name string) (string, error) {
	ao := workflow.ActivityOptions{
		StartToCloseTimeout:    time.Minute * 10,
		HeartbeatTimeout:       time.Minute,      // 心跳超时
		ScheduleToCloseTimeout: time.Minute * 15, // 调度到完成超时
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	var result string
	err := workflow.ExecuteActivity(ctx, Greet, name).Get(ctx, &result)
	if err != nil {
		return "", err
	}
	err = workflow.ExecuteActivity(ctx, Greet2, name).Get(ctx, &result)
	if err != nil {
		return "", err
	}
	return result, nil
}

// 以下为ASM Demo流水线示例
// DemoAsmRequst 请求示例
type DemoAsmRequst struct {
	TargetName []string
}

// TODO DemoAsmResponse 响应示例
type DemoAsmResponse struct {
	Data interface{}
}

// DemoAsmWorkflow ASM流水线逻辑
func DemoAsmWorkflow(ctx workflow.Context, req DemoAsmRequst) (*DemoAsmResponse, error) {
	acOpts := workflow.ActivityOptions{
		StartToCloseTimeout:    time.Minute * 10,
		HeartbeatTimeout:       time.Minute,      // 心跳超时
		ScheduleToCloseTimeout: time.Minute * 15, // 调度到完成超时
	}
	ctx = workflow.WithActivityOptions(ctx, acOpts)
	var result *DemoAsmResponse
	// todo 增加activity逻辑
	return result, nil
}

// ==================== 以下是新的5个Activity的Workflow ====================
var totalTask atomic.Int64

// Signal名称定义
const (
	SignalA1 = "signal-a1"
	SignalA2 = "signal-a2"
	SignalA3 = "signal-a3"
	SignalA4 = "signal-a4"
	SignalA5 = "signal-a5"

	// 状态更新信号
	StatusUpdateSignal = "status-update"

	// 取余number
	qyNumber = 9
)

// Signal数据结构
type ActivityInput struct {
	Value int `json:"value"`
}

type StatusUpdate struct {
	ActivityName string
	Executed     bool
	Executing    bool
}

type DataFlowUpdate struct {
	IsActive bool
}

// ActivityOutput 存储每个Activity的输出结果
type ActivityOutput struct {
	ActivityName string `json:"activity_name"`
	Outputs      []int  `json:"outputs"`
}

// 节点类型定义
type NodeType string

const (
	NodeA1 NodeType = "A1"
	NodeA2 NodeType = "A2"
	NodeA3 NodeType = "A3"
	NodeA4 NodeType = "A4"
	NodeA5 NodeType = "A5"
)

// 统一的输出处理参数
type OutputProcessParams struct {
	Outputs      []int
	CurrentNode  NodeType
	StatusSignal string           // 当前节点的信道
	PrevSignal   workflow.Channel // 上一个节点的信道
	NextSignal   workflow.Channel // 下一个节点的信道（信号通道）
	FinalResults *[]int           // 最终结果存储（仅A5使用）
}

// FiveActivityWorkflow 5个Activity的异步Workflow
func FiveActivityWorkflow(ctx workflow.Context, initialInput int) ([]int, error) {
	// 设置Activity选项
	ao := workflow.ActivityOptions{
		StartToCloseTimeout:    time.Minute * 10,
		HeartbeatTimeout:       time.Second * 30,
		ScheduleToCloseTimeout: time.Minute * 15,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)
	logger := workflow.GetLogger(ctx)

	// 初始化信号通道 todo 这里的初始大小要考虑清楚。或者考虑做一个全局优先级队列？（分布式问题怎么办）
	signalA1 := workflow.NewBufferedChannel(ctx, 10000)
	signalA2 := workflow.NewBufferedChannel(ctx, 10000)
	signalA3 := workflow.NewBufferedChannel(ctx, 10000)
	signalA4 := workflow.NewBufferedChannel(ctx, 10000)
	signalA5 := workflow.NewBufferedChannel(ctx, 10000)

	// 最终结果存储
	finalResults := []int{}
	// 创建可取消的上下文用于协程管理
	childCtx, cancelFunc := workflow.WithCancel(ctx)
	defer cancelFunc()

	// 异步信号监听器
	workflow.Go(childCtx, func(gCtx workflow.Context) {
		for {
			selector := workflow.NewSelector(gCtx)
			// 监听所有信号
			selector.AddReceive(signalA1, func(c workflow.ReceiveChannel, more bool) {
				logger.Info("A1收到")
				if !more {
					return
				}
				var input ActivityInput
				c.Receive(gCtx, &input)
				logger.Info("A1收到信号", "value", input.Value)
				// 触发A1执行
				workflow.Go(gCtx, func(ctx workflow.Context) {
					executeActivityA1(ctx, input.Value, StatusUpdateSignal, signalA2, &finalResults)
				})
			})

			selector.AddReceive(signalA2, func(c workflow.ReceiveChannel, more bool) {
				if !more {
					return
				}
				var input ActivityInput
				c.Receive(gCtx, &input)
				logger.Info("A2收到信号", "value", input.Value)
				// 触发A2执行
				workflow.Go(gCtx, func(ctx workflow.Context) {
					executeActivityA2(ctx, input.Value, StatusUpdateSignal, signalA1, signalA3, &finalResults)
				})
			})

			selector.AddReceive(signalA3, func(c workflow.ReceiveChannel, more bool) {
				if !more {
					return
				}
				var input ActivityInput
				c.Receive(gCtx, &input)
				logger.Info("A3收到信号", "value", input.Value)
				// 触发A3执行
				workflow.Go(gCtx, func(ctx workflow.Context) {
					executeActivityA3(ctx, input.Value, StatusUpdateSignal, signalA2, signalA4, &finalResults)
				})
			})

			selector.AddReceive(signalA4, func(c workflow.ReceiveChannel, more bool) {
				if !more {
					return
				}
				var input ActivityInput
				c.Receive(gCtx, &input)
				logger.Info("A4收到信号", "value", input.Value)
				// 触发A4执行
				workflow.Go(gCtx, func(ctx workflow.Context) {
					executeActivityA4(ctx, input.Value, StatusUpdateSignal, signalA3, signalA5, &finalResults)
				})
			})

			selector.AddReceive(signalA5, func(c workflow.ReceiveChannel, more bool) {
				if !more {
					return
				}
				var input ActivityInput
				c.Receive(gCtx, &input)
				logger.Info("A5收到信号", "value", input.Value)
				// 触发A5执行
				workflow.Go(gCtx, func(ctx workflow.Context) {
					executeActivityA5(ctx, input.Value, StatusUpdateSignal, signalA4, &finalResults)
				})
			})

			selector.Select(gCtx)

			// 检查是否需要退出
			if gCtx.Err() != nil {
				logger.Info("信号监听器退出")
				break
			}
		}
	})

	// 设置当前待完成任务数
	totalTask.Add(1)
	// 异步发送初始数据给到信道A1
	logger.Info("开始异步发送信号完成")
	signalA1.Send(ctx, ActivityInput{Value: initialInput})
	logger.Info("初始异步发送信号完成")

	// 等待所有Activity完成
	for {
		workflow.Sleep(ctx, time.Second*5)
		// 检查是否所有Activity都已执行
		if totalTask.Load() > 0 {
			logger.Info("totalTask not fninish", "任务数", totalTask.Load())
			continue
		}
		logger.Info("所有Activity执行完成，工作流退出")
		break
	}

	// 取消所有子协程
	cancelFunc()
	return finalResults, nil
}

// 统一的输出处理方法
func processActivityOutputs(ctx workflow.Context, params OutputProcessParams) {
	logger := workflow.GetLogger(ctx)
	logger.Info("开始处理节点输出", "node", params.CurrentNode, "outputs", params.Outputs)

	switch params.CurrentNode {
	case NodeA1:
		totalTask.Add(int64(len(params.Outputs)))
		// A1节点：所有输出传给下一个节点(A2)
		for _, output := range params.Outputs {
			params.NextSignal.SendAsync(ActivityInput{Value: output})
			// workflow.SignalExternalWorkflow(ctx, workflowID, runID, params.NextSignal)
		}
	case NodeA2:
		totalTask.Add(int64(len(params.Outputs)))
		// A2节点：能被3整除的传回上一个节点(A1)，其他传给下一个节点(A3)
		for _, output := range params.Outputs {
			if output%qyNumber == 0 {
				params.PrevSignal.SendAsync(ActivityInput{Value: output})
				// workflow.SignalExternalWorkflow(ctx, workflowID, runID, params.PrevSignal, ActivityInput{Value: output})
			} else {
				params.NextSignal.SendAsync(ActivityInput{Value: output})
				// workflow.SignalExternalWorkflow(ctx, workflowID, runID, params.NextSignal, ActivityInput{Value: output})
			}
		}
	case NodeA3:
		totalTask.Add(int64(len(params.Outputs)))
		// A3节点：能被3整除的传回上一个节点(A2)，其他传给下一个节点(A4)
		for _, output := range params.Outputs {
			if output%qyNumber == 0 {
				params.PrevSignal.SendAsync(ActivityInput{Value: output})
				// workflow.SignalExternalWorkflow(ctx, workflowID, runID, params.PrevSignal, ActivityInput{Value: output})
			} else {
				params.NextSignal.SendAsync(ActivityInput{Value: output})
				// workflow.SignalExternalWorkflow(ctx, workflowID, runID, params.NextSignal, ActivityInput{Value: output})
			}
		}
	case NodeA4:
		totalTask.Add(int64(len(params.Outputs)))
		// A4节点：能被3整除的传回上一个节点(A3)，其他传给下一个节点(A5)
		for _, output := range params.Outputs {
			if output%qyNumber == 0 {
				params.PrevSignal.SendAsync(ActivityInput{Value: output})
				// workflow.SignalExternalWorkflow(ctx, workflowID, runID, params.PrevSignal, ActivityInput{Value: output})
			} else {
				params.NextSignal.SendAsync(ActivityInput{Value: output})
				// workflow.SignalExternalWorkflow(ctx, workflowID, runID, params.NextSignal, ActivityInput{Value: output})
			}
		}
	case NodeA5:

		// A5节点：能被3整除的传回上一个节点(A4)，其他存储为最终结果
		for _, output := range params.Outputs {
			if output%qyNumber == 0 {
				totalTask.Add(1)
				params.PrevSignal.SendAsync(ActivityInput{Value: output})
				// workflow.SignalExternalWorkflow(ctx, workflowID, runID, params.PrevSignal, ActivityInput{Value: output})
			} else {
				// 存储为最终结果
				*params.FinalResults = append(*params.FinalResults, output)
			}
		}
	default:
		logger.Error("未知节点类型", "node", params.CurrentNode)
	}
	// 每次调用processActivityOutputs都可以视为一个任务完成
	totalTask.Add(-1)
	logger.Info("节点输出处理完成", "node", params.CurrentNode)
}

// executeActivityA1 执行A1 Activity
func executeActivityA1(ctx workflow.Context, input int, statusSignal string, nextSignal workflow.Channel, finalResults *[]int) {
	logger := workflow.GetLogger(ctx)

	logger.Info("开始执行A1", "input", input)

	// 执行Activity
	var outputs []int
	err := workflow.ExecuteActivity(ctx, ActivityA1, input).Get(ctx, &outputs)
	if err != nil {
		logger.Error("A1执行失败", "error", err)
		return
	}

	logger.Info("A1执行完成", "outputs", outputs)

	// 统一处理输出
	processParams := OutputProcessParams{
		Outputs:      outputs,
		CurrentNode:  NodeA1,
		StatusSignal: statusSignal,
		NextSignal:   nextSignal,
		FinalResults: finalResults,
	}
	processActivityOutputs(ctx, processParams)
}

// executeActivityA2 执行A2 Activity
func executeActivityA2(ctx workflow.Context, input int, statusSignal string, prevSignal, nextSignal workflow.Channel, finalResults *[]int) {
	logger := workflow.GetLogger(ctx)

	logger.Info("开始执行A2", "input", input)

	// 执行Activity
	var outputs []int
	err := workflow.ExecuteActivity(ctx, ActivityA2, input).Get(ctx, &outputs)
	if err != nil {
		logger.Error("A2执行失败", "error", err)
		return
	}

	logger.Info("A2执行完成", "outputs", outputs)

	// 统一处理输出
	processParams := OutputProcessParams{
		Outputs:      outputs,
		CurrentNode:  NodeA2,
		StatusSignal: statusSignal,
		PrevSignal:   prevSignal,
		NextSignal:   nextSignal,
		FinalResults: finalResults,
	}
	processActivityOutputs(ctx, processParams)
}

// executeActivityA3 执行A3 Activity
func executeActivityA3(ctx workflow.Context, input int, statusSignal string, prevSignal, nextSignal workflow.Channel, finalResults *[]int) {
	logger := workflow.GetLogger(ctx)

	logger.Info("开始执行A3", "input", input)

	// 执行Activity
	var outputs []int
	err := workflow.ExecuteActivity(ctx, ActivityA3, input).Get(ctx, &outputs)
	if err != nil {
		logger.Error("A3执行失败", "error", err)
		return
	}

	logger.Info("A3执行完成", "outputs", outputs)

	// 统一处理输出
	processParams := OutputProcessParams{
		Outputs:      outputs,
		CurrentNode:  NodeA3,
		StatusSignal: statusSignal,
		PrevSignal:   prevSignal,
		NextSignal:   nextSignal,
		FinalResults: finalResults,
	}
	processActivityOutputs(ctx, processParams)
}

// executeActivityA4 执行A4 Activity
func executeActivityA4(ctx workflow.Context, input int, statusSignal string, prevSignal, nextSignal workflow.Channel, finalResults *[]int) {
	logger := workflow.GetLogger(ctx)

	logger.Info("开始执行A4", "input", input)

	// 执行Activity
	var outputs []int
	err := workflow.ExecuteActivity(ctx, ActivityA4, input).Get(ctx, &outputs)
	if err != nil {
		logger.Error("A4执行失败", "error", err)
		return
	}

	logger.Info("A4执行完成", "outputs", outputs)

	// 统一处理输出
	processParams := OutputProcessParams{
		Outputs:      outputs,
		CurrentNode:  NodeA4,
		StatusSignal: statusSignal,
		PrevSignal:   prevSignal,
		NextSignal:   nextSignal,
		FinalResults: finalResults,
	}
	processActivityOutputs(ctx, processParams)
}

// executeActivityA5 执行A5 Activity
func executeActivityA5(ctx workflow.Context, input int, statusSignal string, prevSignal workflow.Channel, finalResults *[]int) {
	logger := workflow.GetLogger(ctx)

	logger.Info("开始执行A5", "input", input)

	// 执行�行Activity
	var outputs []int
	err := workflow.ExecuteActivity(ctx, ActivityA5, input).Get(ctx, &outputs)
	if err != nil {
		logger.Error("A5执行失败", "error", err)
		return
	}

	logger.Info("A5执行完成", "outputs", outputs)

	// 统一处理输出
	processParams := OutputProcessParams{
		Outputs:      outputs,
		CurrentNode:  NodeA5,
		StatusSignal: statusSignal,
		PrevSignal:   prevSignal,
		FinalResults: finalResults,
	}
	processActivityOutputs(ctx, processParams)
}

type Otel struct {
	TraceId      string // 链路ID
	TaskId       string // 任务ID
	Target       string // 目标ID
	ParentTarget string // 父目标

}
