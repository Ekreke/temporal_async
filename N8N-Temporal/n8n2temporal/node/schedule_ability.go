package node

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	taskv2 "github.acme.red/backendhub/idl/gen/go/mapper/task/v2"
	workflowv2 "github.acme.red/backendhub/idl/gen/go/mapper/workflow/v2"
	"github.com/bytedance/sonic"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"n8n2temporal/notify"
	"n8n2temporal/pkg/convert"
	"n8n2temporal/pkg/util"
	"strings"
	"time"
)

var (
	githubToken = "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" // todo 待移出去
)

// 能力调度节点（通用）
var _ Activity = (*AbilitySchedule)(nil)

// AbilitySchedule 通用能力调度节点
type AbilitySchedule struct {
	grpcCli taskv2.TaskManagerServiceClient // 调度链接
	tempCli client.Client                   // temporal cli 链接
	*BaseActivity
	parameters *AbilityScheduleParameters // 节点参数
	conv       *convert.JsonPB            // 参数转换入口
}

// AbilityScheduleParameters 定义节点的Parameters格式
type AbilityScheduleParameters struct {
	PbFile     string `json:"pb_file"`     // PB文件,必传
	ReqMessage string `json:"req_message"` // 请求参数message,必传
	RspMessage string `json:"rsp_message"` // 响应参数message,必传
}

// NewAbilitySchedule 创建新的域名解析节点
func NewAbilitySchedule(taskCli taskv2.TaskManagerServiceClient, tmCli client.Client) *AbilitySchedule {
	return &AbilitySchedule{
		grpcCli: taskCli,
		tempCli: tmCli,
	}
}

// AbilitySchedule 注册节点方法,将作为节点的执行入口
func (a *AbilitySchedule) AbilitySchedule(ctx context.Context, input *ActivityInput, express *ExpressionEvaluator, node WkFLowNode) (*ActivityOutput, error) {
	if a.grpcCli == nil {
		return nil, fmt.Errorf("AbilitySchedule Error, grpc client not initialized")
	}
	if a.tempCli == nil {
		return nil, fmt.Errorf("AbilitySchedule Error, temporal client not initialized")
	}
	a.BaseActivity = &BaseActivity{
		NodeInfo:            &node,
		expressionEvaluator: express,
	}
	// 验证参数，补全结构体数据
	if err := a.ValidateInput(input); err != nil {
		return nil, err
	}
	return a.ExecuteWithExecuteTiming(ctx, input, a.executeDomainResolve)
}

// GetNodeInfo 获取当前节点信息
func (a *AbilitySchedule) GetNodeInfo() *WkFLowNode {
	return a.NodeInfo
}

// ValidateInput 验证输入参数（实现NodeActivity接口）
func (a *AbilitySchedule) ValidateInput(input *ActivityInput) error {
	// 基础验证
	if err := a.BaseActivity.ValidateInput(input); err != nil {
		return err
	}
	// 验证三个必穿参数
	params, err := sonic.Marshal(input.Parameters)
	if err != nil {
		return err
	}
	var asParams = &AbilityScheduleParameters{}
	err = sonic.Unmarshal(params, &asParams)
	if err != nil {
		return err
	}
	if asParams.PbFile == "" || asParams.ReqMessage == "" || asParams.RspMessage == "" {
		return fmt.Errorf("AbilitySchedule Params Error, pbFile or ReqMessage or RspMessage is empty")
	}
	a.parameters = asParams
	a.conv = convert.NewJsonPB(asParams.PbFile, githubToken)
	return nil
}

// Execute todo 等待改造
func (a *AbilitySchedule) Execute(ctx context.Context, input *ActivityInput) (*ActivityOutput, error) {
	return nil, nil
}

func (a *AbilitySchedule) executeDomainResolve(ctx context.Context, input *ActivityInput) (*ExecNodeFuncResult, error) {
	var taskUnqIds = make([]string, 0)
	var heartBeat = make([]string, 0)
	if activity.HasHeartbeatDetails(ctx) {
		_ = activity.GetHeartbeatDetails(ctx, &heartBeat)
	}
	if len(heartBeat) > 0 { // 有心跳数据则直接获取即可
		taskUnqIds = heartBeat
	} else { // 无心跳数据说明则重新建任务执行
		taskIds, err := a.CreateScheduleTask(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("CreateScheduleTask Error: %v", err)
		}
		taskUnqIds = taskIds
	}
	// 获取调度结果（如果是阻塞，那么Data的数量和taskUnqIds相等；如果是流式，那么最终流式返回的时候，也和阻塞的数据式一样的）
	if !input.StreamRsp {
		return a.output(ctx, taskUnqIds)
	}
	return a.streamOutput(ctx, taskUnqIds, input)
}

// CreateScheduleTask 创建调度任务
func (a *AbilitySchedule) CreateScheduleTask(ctx context.Context, input *ActivityInput) ([]string, error) {
	logger := a.GetLogger(ctx)
	// 元数据标签
	metaData, err := sonic.Marshal(input.SignalInput.Metadata)
	if err != nil {
		return nil, err
	}
	var labelMap = map[string]string{}
	err = sonic.Unmarshal(metaData, &labelMap)
	if err != nil {
		return nil, err
	}
	// 遍历所有响应数据，组装tasks请求
	tasks := make([]*taskv2.Task, 0, len(input.InputData))
	taskUnqIdMap := make(map[string]struct{})
	for _, inputData := range input.InputData {
		bytesData, err := sonic.Marshal(inputData)
		if err != nil {
			logger.Warn("CreateScheduleTask Marshal Error", "error", err)
			continue
		}
		args, err := a.conv.JSONToAnyPB(ctx, a.parameters.ReqMessage, bytesData)
		if err != nil {
			logger.Warn("CreateScheduleTask Convert PB Error", "error", err)
			continue
		}
		// 请求参数构建
		task := &taskv2.Task{
			Kind: workflowv2.TaskKind_TASK_KIND_DOMAIN_RESOLVE.String(), // 任务类型，对应POC、爬虫等枚举
			//ParentUniqueId: input.SignalData.NodeName,                            // 父ID，上一个节点的ID（信号节点接收到的就是上一个节点的执行结果）
			GroupId:        input.WorkflowID,    // 组ID用于区分流水线,当前正在运行的流水线RunID
			Args:           args,                // 参数，节点执行的参数
			Labels:         labelMap,            // 标签，透传
			WorkerSelector: map[string]string{}, // 工作节点选择，暂时为空
		}
		// 计算唯一ID
		var strBuilder strings.Builder
		strBuilder.WriteString(task.Kind)
		strBuilder.WriteString(task.GroupId)
		strBuilder.Write(task.Args.GetValue())
		task.UniqueId = fmt.Sprintf("%x", md5.Sum([]byte(strBuilder.String())))
		// 表示任务重复了
		if _, ok := taskUnqIdMap[task.UniqueId]; ok {
			logger.Warn("重复任务参数", "UniqueId", task.UniqueId)
			continue
		}
		taskUnqIdMap[task.UniqueId] = struct{}{}
		tasks = append(tasks, task)
	}
	if len(tasks) < 1 {
		return nil, fmt.Errorf("CreateScheduleTask Error, no tasks found")
	}
	// 统计任务id列表
	var taskUnqIds = make([]string, 0, len(tasks))
	for _, task := range tasks {
		taskUnqIds = append(taskUnqIds, task.UniqueId)
	}
	logger.Info("调度任务列表", "taskUnqIds", strings.Join(taskUnqIds, ","))
	// 发送到调度，执行任务（不要获取结果，因为结果表示的是任务的增量，但我们只关心创建和丢失的任务数量）
	_, err = a.grpcCli.CreateTasks(ctx, &taskv2.CreateTasksRequest{Tasks: tasks})
	if err != nil {
		return nil, err
	}
	return taskUnqIds, nil
}

// 阻塞输出
func (a *AbilitySchedule) output(ctx context.Context, taskUnqIds []string) (*ExecNodeFuncResult, error) {
	var (
		res = &ExecNodeFuncResult{ // 响应结果
			Data: make([]map[string]interface{}, 0, len(taskUnqIds)),
		}
		i = 0 // 记录连续接口错误次数
	)
	for {
		// 轮训间隔
		time.Sleep(10 * time.Second)
		// 发送心跳
		activity.RecordHeartbeat(ctx, taskUnqIds)
		// 达到最大连续错误次数上限停止
		if i > 5 {
			return nil, errors.New("获取节点执行结果失败")
		}
		// 查询list结构
		resp, err := a.grpcCli.ListTasks(ctx, &taskv2.ListTasksRequest{
			UniqueIds:  taskUnqIds,
			StatusList: []taskv2.TaskStatus{taskv2.TaskStatus_TASK_STATUS_COMPLETED, taskv2.TaskStatus_TASK_STATUS_DISCARDED},
		})
		if err != nil {
			i++
			continue
		}
		i = 0 // 将错误数置空，只有连续错误才会停止
		// 阻塞响应的退出条件就是查询结果数量等于创建任务数量
		if len(resp.GetItems()) != len(taskUnqIds) {
			continue
		}
		// 处理结果
		for _, r := range resp.GetItems() {
			if len(r.Result.GetValue()) == 0 { // 结果为空的数据
				continue
			}
			bytesData, err := a.conv.AnyPBToJSON(ctx, a.parameters.RspMessage, r.Result)
			if err != nil {
				return nil, errors.New("anyPBToJSON Error:" + err.Error())
			}
			var dataVal = map[string]interface{}{}
			err = sonic.Unmarshal(bytesData, &dataVal)
			if err != nil {
				return nil, errors.New("Unmarshal mapVal Error:" + err.Error())
			}
			res.Data = append(res.Data, dataVal)
		}
		return res, nil
	}
}

// 流式输出
func (a *AbilitySchedule) streamOutput(ctx context.Context, taskUnqIds []string, input *ActivityInput) (*ExecNodeFuncResult, error) {
	var (
		res = &ExecNodeFuncResult{ // 最终响应结果
			Data: make([]map[string]interface{}, 0, len(taskUnqIds)),
		}
		logger = a.GetLogger(ctx)      // 日志信息
		info   = activity.GetInfo(ctx) // activity信息
		i      = 0                     // 记录连续接口错误次数
	)
	for {
		time.Sleep(5 * time.Second)
		if i > 5 { // 错误次数达到上限
			return nil, errors.New("获取节点执行结果失败")
		}
		// 完成所有任务时，退出循环，返回数据
		if len(taskUnqIds) == 0 {
			break
		}
		resp, err := a.grpcCli.ListTasks(ctx, &taskv2.ListTasksRequest{
			UniqueIds:  taskUnqIds,
			StatusList: []taskv2.TaskStatus{taskv2.TaskStatus_TASK_STATUS_COMPLETED, taskv2.TaskStatus_TASK_STATUS_DISCARDED},
		})
		// 收到响应结果就发送心跳，无论下面逻辑怎么执行，都不印象
		if err != nil {
			i++
			logger.Info("调度任务结果获取失败", "err", err, "i", i)
			activity.RecordHeartbeat(ctx, taskUnqIds)
			time.Sleep(time.Second)
			continue
		}

		i = 0 // 将错误数置空，只有连续错误才会停止
		if len(resp.GetItems()) < 1 {
			activity.RecordHeartbeat(ctx, taskUnqIds)
			continue
		}
		// 遍历每次查询的数据
		var rspIds = make([]string, 0, len(resp.GetItems()))                 // 存储已经收到的响应信号ID
		var tmpRes = make([]map[string]interface{}, 0, len(resp.GetItems())) // 存储结果数据
		var temResIds = make([]string, 0, len(resp.GetItems()))              // 存储信号返回数据的结果id
		for _, r := range resp.GetItems() {
			rspIds = append(rspIds, r.UniqueId)
			if len(r.Result.GetValue()) == 0 { // 结果为空的数据
				continue
			}
			bytesData, err := a.conv.AnyPBToJSON(ctx, a.parameters.RspMessage, r.Result)
			if err != nil {
				return nil, err
			}
			var dataVal = map[string]interface{}{}
			err = sonic.Unmarshal(bytesData, &dataVal)
			if err != nil {
				return nil, err
			}
			temResIds = append(temResIds, r.UniqueId)
			tmpRes = append(tmpRes, dataVal)
			res.Data = append(res.Data, dataVal)
		}
		// 没有拿到结果的前提下，没必要发送信号
		if len(tmpRes) < 1 || len(temResIds) < 1 {
			activity.RecordHeartbeat(ctx, taskUnqIds)
			continue
		}
		// 更新taskUnqId的值
		taskUnqIds = util.ArrRemoveItems(taskUnqIds, rspIds)
		// 发送信号 - 重置下一跳的branch
		input.SignalInput.Metadata.NextBranch = "main"
		var signalData = notify.SignalData{
			ActivityExecId: input.ExecID,
			NodeName:       a.BaseActivity.NodeInfo.Name,
			Data:           tmpRes,
			DataId:         temResIds,
			Metadata:       input.SignalInput.Metadata,
		}
		err = a.tempCli.SignalWorkflow(ctx, info.WorkflowExecution.ID, "", a.BaseActivity.NodeInfo.Name, signalData)
		if err != nil {
			return nil, err
		}
		activity.RecordHeartbeat(ctx, taskUnqIds)
	}
	return res, nil
}
