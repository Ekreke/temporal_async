package node

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"github.com/bytedance/sonic"
	"github.com/spf13/cast"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
	"reflect"
)

// SopNode sop节点（子工作流），负责拉起一个子工作流
type SopNode struct {
	*BaseActivity
	cli client.Client
}

// SopParameters sop参数定义(这个定义要注意和workflow的GenericWorkflowInput对其，本质是对其赋值)
type SopParameters struct {
	WorkflowJSON string                 `json:"workflow_json"` // SOP的DSL信息
	InitialData  map[string]interface{} `json:"initial_data"`  // 子工作流启动所需要的信息
	MaxStep      int64                  `json:"max_step"`      // 最大步数，无环可设置为0，有环则必须设置(必须为string，他可能需要变量赋值)
}

// NewSopNode 创建SOP节点实例
func NewSopNode(tprCli client.Client) *SopNode {
	return &SopNode{
		cli: tprCli,
	}
}

// SOP 注册执行节点
func (s *SopNode) SOP(ctx context.Context, input *ActivityInput) (*ActivityOutput, error) {
	sop := NewSopNodeActivity(input)
	sop.cli = s.cli
	if err := sop.ValidateInput(input); err != nil {
		return nil, err
	}
	return sop.ExecuteWithExecuteTiming(ctx, input, sop.executeBatchSopNode)
}

var _ Activity = (*SopNodeActivity)(nil)

// SopNodeActivity sop节点（子工作流），实际逻辑执行实例
type SopNodeActivity struct {
	*BaseActivity
	cli client.Client
}

func NewSopNodeActivity(input *ActivityInput) *SopNodeActivity {
	return &SopNodeActivity{
		BaseActivity: &BaseActivity{
			NodeInfo:            input.Node,
			expressionEvaluator: input.Express,
		},
	}
}

// GetNodeInfo 获取当前节点信息
func (s *SopNodeActivity) GetNodeInfo() *WkFLowNode {
	return s.NodeInfo
}

// 批量参数SOP，最终返回所有sopId(WorkflowId)
func (s *SopNodeActivity) executeBatchSopNode(ctx context.Context, input *ActivityInput) (*ExecNodeFuncResult, error) {
	var resData = &ExecNodeFuncResult{
		Data: make([]map[string]interface{}, 0, len(input.InputData)),
	}
	// 组装参数
	for _, inputData := range input.InputData {
		res, err := s.executeSopNode(ctx, input, inputData)
		if err != nil {
			return nil, err
		}
		resData.Data = append(resData.Data, res)
	}
	return resData, nil
}

// executeStartNode 开始节点的具体执行逻辑
func (s *SopNodeActivity) executeSopNode(ctx context.Context, input *ActivityInput, inputData map[string]interface{}) (map[string]interface{}, error) {
	// 1、组装参数
	var params = SopParameters{}
	if err := s.parseParameters(input.Node.Parameters, &params, inputData); err != nil {
		return nil, errors.New("节点参数有误：" + err.Error())
	}
	// 2、发送信号（子工作流需要在workflow中开启）
	var data = make(map[string]interface{})
	pData, err := sonic.Marshal(params)
	if err != nil {
		return nil, errors.New("节点参数序列化失败：" + err.Error())
	}
	err = sonic.Unmarshal(pData, &data)
	if err != nil {
		return nil, errors.New("节点参数反序列化失败：" + err.Error())
	}
	// 尝试创建唯一ID
	data["hash_id"] = fmt.Sprintf("%x", md5.Sum(pData))
	return data, nil
}

// Unmarshal 解析params到sop属性中
func (s *SopNodeActivity) parseParameters(params map[string]interface{}, sop *SopParameters, inputData map[string]interface{}) error {
	var err error
	// 节点赋值 & 变量进行转换
	if val, ok := params["max_step"]; ok {
		var maxStep interface{}
		// 类型兼容
		if v, ok := val.(string); ok {
			maxStep, err = s.BaseActivity.GetExpressionEvaluator().EvaluateExpression(v, inputData)
			if err != nil {
				return err
			}
		} else {
			maxStep = v
		}
		// 这里再去转换maxStep类型
		sop.MaxStep, err = cast.ToInt64E(maxStep)
		if err != nil {
			return errors.New("max_step参数类型有误")
		}
	}
	// workflow的DSL
	if val, ok := params["workflow_json"].(string); ok {
		data, err := s.BaseActivity.GetExpressionEvaluator().EvaluateExpression(val, inputData)
		if err != nil {
			return err
		}
		sop.WorkflowJSON, err = cast.ToStringE(data)
		if err != nil {
			return errors.New("workflow_json 参数有误")
		}
	} else {
		return errors.New("workflow_json 参数类型有误")
	}
	// 入参
	switch val := params["initial_data"].(type) {
	case string:
		data, err := s.BaseActivity.GetExpressionEvaluator().EvaluateExpression(val, inputData)
		if err != nil {
			return err
		}
		dataVal, ok := data.(map[string]interface{})
		if !ok {
			return errors.New("initData非map[string]interface{}类型")
		}
		sop.InitialData = dataVal
	case map[string]interface{}:
		sop.InitialData, err = s.BaseActivity.GetExpressionEvaluator().EvaluateMapExpression(val, inputData)
		if err != nil {
			return err
		}
	default:
		return errors.New("initial_data 参数类型有误," + fmt.Sprintf("类型为: %v\n", reflect.TypeOf(params["initial_data"])))
	}
	return nil
}

// GetLogger 获取logger
func (s *SopNodeActivity) GetLogger(ctx context.Context) log.Logger {
	return s.BaseActivity.GetLogger(ctx)
}

// ValidateInput 验证输入参数
func (s *SopNodeActivity) ValidateInput(input *ActivityInput) error {
	if err := s.BaseActivity.ValidateInput(input); err != nil {
		return err
	}
	if len(input.Node.Parameters) < 1 {
		return errors.New("missing parameters")
	}
	if input.Node.Parameters["workflow_json"] == "" {
		return errors.New("missing workflow_json")
	}
	//if input.Node.Parameters["sop_name"] == nil {
	//	return errors.New("missing sop_name")
	//}
	// 当前节点必须是远程节点
	if !input.Node.IsRemote {
		return errors.New("sop node cannot be local")
	}
	return nil
}
