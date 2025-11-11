package node

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"n8n2temporal/node/assemble"
	"strings"
	"time"

	taskv2 "github.acme.red/backendhub/idl/gen/go/mapper/task/v2"
	workflowv2 "github.acme.red/backendhub/idl/gen/go/mapper/workflow/v2"
	taskargsv1 "github.acme.red/mapper/idl/gen/go/mapper/taskargs/v1"
	"github.com/bytedance/sonic"
	"google.golang.org/protobuf/types/known/anypb"
)

var _ Activity = (*SpiderActivity)(nil)

// SpiderActivity 爬虫节点 Activity
type SpiderActivity struct {
	grpcCli taskv2.TaskManagerServiceClient
	*BaseActivity
}

// NewSpiderActivity 创建新的爬虫节点
func NewSpiderActivity(grpcClient taskv2.TaskManagerServiceClient) *SpiderActivity {
	activity := &SpiderActivity{
		grpcCli: grpcClient,
	}
	return activity
}

// RegisterSpider 注册节点到worker
func (a *SpiderActivity) RegisterSpider(ctx context.Context, input *ActivityInput,
	express *ExpressionEvaluator, node WkFLowNode) (*ActivityOutput, error) {
	if a.grpcCli == nil {
		return nil, fmt.Errorf("RegisterSpider Error, grpc client not initialized")
	}
	a.BaseActivity = &BaseActivity{
		NodeInfo:            &node,
		expressionEvaluator: express,
	}
	return a.Execute(ctx, input)
}

// GetNodeInfo 获取当前节点信息
func (a *SpiderActivity) GetNodeInfo() *WkFLowNode {
	return a.NodeInfo
}

// Execute 执行节点逻辑
func (a *SpiderActivity) Execute(ctx context.Context, input *ActivityInput) (*ActivityOutput, error) {
	return a.ExecuteWithExecuteTiming(ctx, input, a.executeSpider)
}

// ValidateInput 验证输入参数
func (a *SpiderActivity) ValidateInput(input *ActivityInput) error {
	// 统一基础验证
	if err := a.BaseActivity.ValidateInput(input); err != nil {
		return err
	}
	// 参数检查
	return nil
}

// executeSpider 执行实际逻辑
func (a *SpiderActivity) executeSpider(input *ActivityInput) (*ExecNodeFuncResult, error) {
	// 验证参数
	if err := a.ValidateInput(input); err != nil {
		return nil, err
	}
	// 元数据标签
	metaData, _ := sonic.Marshal(input.SignalInput.Metadata)
	var label = map[string]string{}
	_ = sonic.Unmarshal(metaData, &label)
	// 遍历所有响应数据，组装tasks请求
	tasks := make([]*taskv2.Task, 0, len(input.SignalInput.Data))
	taskUnqIdMap := make(map[string]struct{})

	for _, signalData := range input.SignalInput.Data {
		// 解析parameters中的各个变量值，进行正确赋值
		nodeParam := make(map[string]interface{})
		for k, v := range input.Parameters {
			val, ok := v.(string)
			if !ok {
				nodeParam[k] = v
				continue
			}
			evalData, err := a.GetExpressionEvaluator().EvaluateExpression(val, signalData)
			if err != nil {
				return nil, err
			}
			nodeParam[k] = evalData
		}
		// 将入参转换成anyPb
		nodeData, err := sonic.Marshal(nodeParam)
		if err != nil {
			return nil, err
		}

		data := &taskargsv1.SpiderTaskData{}
		err = sonic.Unmarshal(nodeData, &data)
		if err != nil {
			return nil, err
		}

		args, err := anypb.New(data)
		if err != nil {
			return nil, err
		}
		// 请求参数构建
		task := &taskv2.Task{
			Kind:           workflowv2.TaskKind_TASK_KIND_DOMAIN_RESOLVE.String(), // 任务类型，对应POC、爬虫等枚举
			ParentUniqueId: input.SignalInput.NodeName,                            // 父ID，上一个节点的ID（信号节点接收到的就是上一个节点的执行结果）
			GroupId:        input.WorkflowID,                                      // 组ID用于区分流水线,当前正在运行的流水线RunID
			Args:           args,                                                  // 参数，节点执行的参数
			Labels:         label,                                                 // 标签，透传
			WorkerSelector: map[string]string{},                                   // 工作节点选择，暂时为空
		}
		var strBuilder strings.Builder
		strBuilder.WriteString(task.Kind)
		strBuilder.WriteString(task.ParentUniqueId)
		strBuilder.WriteString(task.GroupId)
		strBuilder.WriteString(task.Args.String())
		labelBytes, _ := sonic.Marshal(task.Labels)
		strBuilder.Write(labelBytes)
		task.UniqueId = fmt.Sprintf("%x", md5.Sum([]byte(strBuilder.String())))
		// 表示任务重复了
		if _, ok := taskUnqIdMap[task.UniqueId]; ok {
			continue
		}
		taskUnqIdMap[task.UniqueId] = struct{}{}
		tasks = append(tasks, task)
	}

	// 统计任务id列表
	var taskUnqIds = make([]string, 0, len(tasks))
	for _, task := range tasks {
		taskUnqIds = append(taskUnqIds, task.UniqueId)
	}
	// 发送到调度，执行任务
	_, err := a.grpcCli.CreateTasks(context.Background(), &taskv2.CreateTasksRequest{Tasks: tasks})
	if err != nil {
		return nil, err
	}
	// 这里调用调度获取结果接口
	var (
		i   = 0
		res = make([]*taskv2.ListTasksResponseItem, 0)
	)
	for {
		if i > 5 {
			return nil, errors.New("获取节点执行结果失败")
		}
		var status = taskv2.TaskStatus_TASK_STATUS_COMPLETED
		resp, err := a.grpcCli.ListTasks(context.Background(), &taskv2.ListTasksRequest{
			UniqueIds: taskUnqIds,
			GroupId:   &input.WorkflowID,
			Status:    &status,
		})
		if err != nil {
			i++
			continue
		}
		i = 0
		if len(resp.GetItems()) != len(taskUnqIds) {
			time.Sleep(5 * time.Second)
			continue
		}
		res = resp.GetItems()
		break
	}
	// 查询调度执行结果,
	var rs = &ExecNodeFuncResult{
		Data:          make([]map[string]interface{}, 0),
		AddTaskNum:    len(tasks),
		FinishTaskNum: 1,
	}
	for _, r := range res {
		var spiderResovle = &taskargsv1.SpiderTaskResult{}
		if !r.Result.MessageIs(spiderResovle) {
			return nil, errors.New("请求和响应的协议不一致！")
		}
		err = r.Result.UnmarshalTo(spiderResovle)
		if err != nil {
			return nil, fmt.Errorf("结果反序列化失败:%v", err)
		}

		resultMap := assemble.ConvertSpiderTaskResult(spiderResovle)
		rs.Data = append(rs.Data, resultMap)
	}
	return rs, nil
}
