package node

// 暂弃，后面应该用不到了
//import (
//	"context"
//	"crypto/md5"
//	"errors"
//	"fmt"
//	taskv2 "github.acme.red/backendhub/idl/gen/go/mapper/task/v2"
//	workflowv2 "github.acme.red/backendhub/idl/gen/go/mapper/workflow/v2"
//	taskargsv1 "github.acme.red/mapper/idl/gen/go/mapper/taskargs/v1"
//	"github.com/bytedance/sonic"
//	"go.temporal.io/sdk/activity"
//	"go.temporal.io/sdk/client"
//	"google.golang.org/protobuf/types/known/anypb"
//	"math/rand"
//	"n8n2temporal/notify"
//	"n8n2temporal/pkg/util"
//	"strings"
//	"time"
//)
//
//var _ Activity = (*DomainResolveActivity)(nil)
//
//// DomainResolveActivity 域名解析 Activity
//type DomainResolveActivity struct {
//	grpcCli taskv2.TaskManagerServiceClient // 调度链接
//	tempCli client.Client                   // temporal cli 链接
//	*BaseActivity
//	data []*taskargsv1.DomainResolveTaskData // 参数
//}
//
//// NewDomainResolveActivity 创建新的域名解析节点
//func NewDomainResolveActivity(taskCli taskv2.TaskManagerServiceClient, tmCli client.Client) *DomainResolveActivity {
//	return &DomainResolveActivity{
//		grpcCli: taskCli,
//		tempCli: tmCli,
//	}
//}
//
//// DomainResolveActivity 注册域名解析作为activity节点
//func (a *DomainResolveActivity) DomainResolveActivity(ctx context.Context, input *ActivityInput,
//	express *ExpressionEvaluator, node WkFLowNode) (*ActivityOutput, error) {
//	if a.grpcCli == nil {
//		return nil, fmt.Errorf("DomainResolveActivity Error, grpc client not initialized")
//	}
//	if a.tempCli == nil {
//		return nil, fmt.Errorf("DomainResolveActivity Error, temporal client not initialized")
//	}
//	a.BaseActivity = &BaseActivity{
//		NodeInfo:            &node,
//		expressionEvaluator: express,
//	}
//	return a.Execute(ctx, input)
//}
//
//// GetNodeInfo 获取当前节点信息
//func (a *DomainResolveActivity) GetNodeInfo() *WkFLowNode {
//	return a.NodeInfo
//}
//
//// Execute 执行节点逻辑（实现NodeActivity接口）
//func (a *DomainResolveActivity) Execute(ctx context.Context, input *ActivityInput) (*ActivityOutput, error) {
//	return a.ExecuteWithExecuteTiming(ctx, input, a.executeDomainResolve)
//}
//
//// ValidateInput 验证输入参数（实现NodeActivity接口）
//func (a *DomainResolveActivity) ValidateInput(input *ActivityInput) error {
//	// 基础验证
//	if err := a.BaseActivity.ValidateInput(input); err != nil {
//		return err
//	}
//	// 获取变量节点的值
//	nodeData, err := sonic.Marshal(input.Parameters)
//	if err != nil {
//		return err
//	}
//	var taskResolve = make([]*taskargsv1.DomainResolveTaskData, 0)
//	err = sonic.Unmarshal(nodeData, &taskResolve)
//	if err != nil {
//		return err
//	}
//	a.data = taskResolve
//	return nil
//}
//
//// executeDomainResolve 内部域名解析逻辑
//func (a *DomainResolveActivity) executeDomainResolve(ctx context.Context, input *ActivityInput) (*ExecNodeFuncResult, error) {
//	logger := a.GetLogger(ctx)
//	// 验证参数
//	if err := a.ValidateInput(input); err != nil {
//		return nil, err
//	}
//	var taskUnqIds []string
//	var hb []string
//	if activity.HasHeartbeatDetails(ctx) {
//		_ = activity.GetHeartbeatDetails(ctx, &hb)
//	}
//	if len(hb) > 0 {
//		taskUnqIds = hb
//	} else {
//		taskIds, err := a.CreateTask(ctx, input)
//		if err != nil {
//			logger.Error("CreateScheduleTask Error", "err", err)
//			return nil, err
//		}
//		taskUnqIds = taskIds
//	}
//	// 获取调度结果（如果是阻塞，那么Data的数量和taskUnqIds相等；如果是流式，那么最终流式返回的时候，也和阻塞的数据式一样的）
//	if !input.StreamRsp {
//		return a.output(ctx, taskUnqIds)
//	}
//	return a.streamOutput(ctx, taskUnqIds, input)
//}
//
//// CreateTask 创建调度任务
//func (a *DomainResolveActivity) CreateTask(ctx context.Context, input *ActivityInput) ([]string, error) {
//	logger := a.GetLogger(ctx)
//	// 元数据标签
//	metaData, _ := sonic.Marshal(input.SignalInput.Metadata)
//	var label = map[string]string{}
//	_ = sonic.Unmarshal(metaData, &label)
//	// 遍历所有响应数据，组装tasks请求
//	tasks := make([]*taskv2.Task, 0, len(input.SignalInput.Data))
//	taskUnqIdMap := make(map[string]struct{})
//
//	for _, data := range a.data {
//		args, err := anypb.New(data)
//		if err != nil {
//			return nil, err
//		}
//		// 请求参数构建
//		task := &taskv2.Task{
//			Kind: workflowv2.TaskKind_TASK_KIND_DOMAIN_RESOLVE.String(), // 任务类型，对应POC、爬虫等枚举
//			//ParentUniqueId: input.SignalData.NodeName,                            // 父ID，上一个节点的ID（信号节点接收到的就是上一个节点的执行结果）
//			GroupId:        input.WorkflowID,    // 组ID用于区分流水线,当前正在运行的流水线RunID
//			Args:           args,                // 参数，节点执行的参数
//			Labels:         label,               // 标签，透传
//			WorkerSelector: map[string]string{}, // 工作节点选择，暂时为空
//		}
//		// 计算唯一ID
//		var strBuilder strings.Builder
//		strBuilder.WriteString(task.Kind)
//		//strBuilder.WriteString(task.ParentUniqueId)
//		strBuilder.WriteString(task.GroupId)
//		strBuilder.Write(task.Args.GetValue())
//		//labelBytes, _ := sonic.Marshal(task.Labels)
//		//strBuilder.Write(labelBytes)
//		task.UniqueId = fmt.Sprintf("%x", md5.Sum([]byte(strBuilder.String())))
//		// 表示任务重复了
//		if _, ok := taskUnqIdMap[task.UniqueId]; ok {
//			continue
//		}
//		taskUnqIdMap[task.UniqueId] = struct{}{}
//		tasks = append(tasks, task)
//	}
//	// 统计任务id列表
//	var taskUnqIds = make([]string, 0, len(tasks))
//	for _, task := range tasks {
//		taskUnqIds = append(taskUnqIds, task.UniqueId)
//	}
//	logger.Info("调度任务列表", "taskUnqIds", strings.Join(taskUnqIds, ","))
//	// 发送到调度，执行任务（不要获取结果，因为结果表示的是任务的增量，但我们只关心创建和丢失的任务数量）
//	_, err := a.grpcCli.CreateTasks(ctx, &taskv2.CreateTasksRequest{Tasks: tasks})
//	if err != nil {
//		return nil, err
//	}
//	return taskUnqIds, nil
//}
//
//// 阻塞输出
//func (a *DomainResolveActivity) output(ctx context.Context, taskUnqIds []string) (*ExecNodeFuncResult, error) {
//	var (
//		// 响应结果
//		res = &ExecNodeFuncResult{
//			Data: make([]map[string]interface{}, 0, len(taskUnqIds)),
//		}
//		logger = a.GetLogger(ctx) // 日志信息
//		i      = 0                // 记录连续接口错误次数
//	)
//	delay := 100 * time.Millisecond
//	maxDelay := 2 * time.Second
//	for {
//		if i > 5 {
//			return nil, errors.New("获取节点执行结果失败")
//		}
//		resp, err := a.grpcCli.ListTasks(ctx, &taskv2.ListTasksRequest{
//			UniqueIds:  taskUnqIds,
//			StatusList: []taskv2.TaskStatus{taskv2.TaskStatus_TASK_STATUS_COMPLETED, taskv2.TaskStatus_TASK_STATUS_DISCARDED},
//		})
//		if err != nil {
//			i++
//			logger.Info("调度任务结果获取失败", "err", err, "i", i)
//			d := withJitter(delay)
//			if d > maxDelay {
//				d = maxDelay
//			}
//			if err := wait(ctx, d); err != nil {
//				return nil, err
//			}
//			delay = time.Duration(float64(delay) * 2)
//			activity.RecordHeartbeat(ctx, taskUnqIds)
//			continue
//		}
//		i = 0 // 将错误数置空，只有连续错误才会停止
//		delay = 100 * time.Millisecond
//
//		// 阻塞响应的退出条件就是查询结果数量等于创建任务数量
//		if len(resp.GetItems()) != len(taskUnqIds) {
//			activity.RecordHeartbeat(ctx, taskUnqIds)
//			if err := wait(ctx, 200*time.Millisecond); err != nil {
//				return nil, err
//			}
//			continue
//		}
//		for _, r := range resp.GetItems() {
//			var domainResolve = &taskargsv1.DomainResolveTaskResult{}
//			if r.Result == nil {
//				continue
//			}
//			if !r.Result.MessageIs(domainResolve) {
//				return nil, errors.New("请求和响应的协议不一致")
//			}
//			err := r.Result.UnmarshalTo(domainResolve)
//			if err != nil {
//				return nil, errors.New("数据序列化失败")
//			}
//			dataVal, err := util.PbToMap(domainResolve)
//			if err != nil {
//				return nil, err
//			}
//			res.Data = append(res.Data, dataVal)
//		}
//		return res, nil
//	}
//}
//
//// 流式输出
//func (a *DomainResolveActivity) streamOutput(ctx context.Context, taskUnqIds []string, input *ActivityInput) (*ExecNodeFuncResult, error) {
//	var (
//		res = &ExecNodeFuncResult{ // 最终响应结果
//			Data: make([]map[string]interface{}, 0, len(taskUnqIds)),
//		}
//		logger = a.GetLogger(ctx)      // 日志信息
//		info   = activity.GetInfo(ctx) // activity信息
//		i      = 0                     // 记录连续接口错误次数
//	)
//	for {
//		if i > 5 { // 错误次数达到上限
//			return nil, errors.New("获取节点执行结果失败")
//		}
//		// 完成所有任务时，退出循环，返回数据
//		if len(taskUnqIds) == 0 {
//			break
//		}
//		resp, err := a.grpcCli.ListTasks(ctx, &taskv2.ListTasksRequest{
//			UniqueIds:  taskUnqIds,
//			StatusList: []taskv2.TaskStatus{taskv2.TaskStatus_TASK_STATUS_COMPLETED, taskv2.TaskStatus_TASK_STATUS_DISCARDED},
//		})
//		// 收到响应结果就发送心跳，无论下面逻辑怎么执行，都不印象
//		if err != nil {
//			i++
//			logger.Info("调度任务结果获取失败", "err", err, "i", i)
//			activity.RecordHeartbeat(ctx, taskUnqIds)
//			time.Sleep(time.Second)
//			continue
//		}
//
//		i = 0 // 将错误数置空，只有连续错误才会停止
//		if len(resp.GetItems()) < 1 {
//			activity.RecordHeartbeat(ctx, taskUnqIds)
//			if err := wait(ctx, 200*time.Millisecond); err != nil {
//				return nil, err
//			}
//			continue
//		}
//		// 遍历每次查询的数据
//		var rspIds = make([]string, 0, len(resp.GetItems()))
//		var tmpRes = make([]map[string]interface{}, 0, len(resp.GetItems()))
//		var temResIds = make([]string, 0, len(resp.GetItems()))
//		for _, r := range resp.GetItems() {
//			rspIds = append(rspIds, r.UniqueId)
//			var domainResolve = &taskargsv1.DomainResolveTaskResult{}
//			if len(r.Result.GetValue()) == 0 { // 爬出结果为空的数据
//				continue
//			}
//			if !r.Result.MessageIs(domainResolve) {
//				return nil, errors.New("请求和响应的协议不一致")
//			}
//			err := r.Result.UnmarshalTo(domainResolve)
//			if err != nil {
//				return nil, errors.New("数据序列化失败")
//			}
//			dataVal, err := util.PbToMap(domainResolve)
//			if err != nil {
//				return nil, err
//			}
//			temResIds = append(temResIds, r.UniqueId)
//			tmpRes = append(tmpRes, dataVal)
//			res.Data = append(res.Data, dataVal)
//		}
//		// 没有拿到结果的前提下，没必要发送信号
//		if len(tmpRes) < 1 || len(temResIds) < 1 {
//			activity.RecordHeartbeat(ctx, taskUnqIds)
//			continue
//		}
//		// 更新taskUnqId的值
//		taskUnqIds = util.ArrRemoveItems(taskUnqIds, rspIds)
//		// 发送信号 - 充值branch
//		input.SignalInput.Metadata.NextBranch = "main"
//		var signalData = notify.SignalData{
//			ActivityExecId: input.ExecID,
//			NodeName:       a.BaseActivity.NodeInfo.Name,
//			Data:           tmpRes,
//			DataId:         temResIds,
//			Metadata:       input.SignalInput.Metadata,
//		}
//		err = a.tempCli.SignalWorkflow(ctx, info.WorkflowExecution.ID, "", a.BaseActivity.NodeInfo.Name, signalData)
//		if err != nil {
//			return nil, err
//		}
//		activity.RecordHeartbeat(ctx, taskUnqIds)
//	}
//	return res, nil
//}
//
//func withJitter(d time.Duration) time.Duration {
//	n := (rand.Float64()*2 - 1) * 0.2
//	return time.Duration(float64(d) * (1 + n))
//}
//
//func wait(ctx context.Context, d time.Duration) error {
//	timer := time.NewTimer(d)
//	defer timer.Stop()
//	select {
//	case <-timer.C:
//		return nil
//	case <-ctx.Done():
//		if !timer.Stop() {
//			<-timer.C
//		}
//		return ctx.Err()
//	}
//}
