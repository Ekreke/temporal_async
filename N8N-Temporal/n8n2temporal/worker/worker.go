package main

import (
	"context"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"log"
	"n8n2temporal/node"
	"n8n2temporal/workflow"
)

// Worker 实现
func main() {
	// 创建 Temporal 客户端
	c, err := client.Dial(client.Options{
		HostPort: "localhost:7233",
	})
	if err != nil {
		log.Fatalln("无法创建 Temporal 客户端:", err)
	}
	defer c.Close()

	// 创建 Worker
	w := worker.New(c, "n8n-conversion-queue", worker.Options{})

	// 注册工作流和活动
	//w.RegisterWorkflow(workflow.N8NConvertedWorkflow)
	w.RegisterWorkflow(workflow.GenericWorkflowWithMaxStep)

	// 注册所有活动
	w.RegisterActivity(node.NewDomainResolveActivity().ExecuteDomainResolve)
	w.RegisterActivity(node.NewCustomNodeActivity().ExecuteCustomNode)

	// 注册v2.0.0节点（创建独立的执行函数）
	w.RegisterActivity(executeStartNode)
	w.RegisterActivity(executeEndNode)
	w.RegisterActivity(executeVariableNode)
	w.RegisterActivity(executeConditionalNode)
	w.RegisterActivity(executePythonDockerNode)

	// 启动 Worker
	log.Println("启动 n8n 转换 Worker...")
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalln("Worker 运行失败:", err)
	}
}

// v2.0.0节点执行包装函数
func executeStartNode(ctx context.Context, input *node.ActivityInput) (*node.ActivityOutput, error) {
	return node.NewStartNodeActivity().Execute(ctx, input)
}

func executeEndNode(ctx context.Context, input *node.ActivityInput) (*node.ActivityOutput, error) {
	return node.NewEndNodeActivity().Execute(ctx, input)
}

func executeVariableNode(ctx context.Context, input *node.ActivityInput) (*node.ActivityOutput, error) {
	return node.NewVariableNodeActivity().Execute(ctx, input)
}

func executeConditionalNode(ctx context.Context, input *node.ActivityInput) (*node.ActivityOutput, error) {
	return node.NewConditionalNodeActivity().Execute(ctx, input)
}

func executePythonDockerNode(ctx context.Context, input *node.ActivityInput) (*node.ActivityOutput, error) {
	return node.NewPythonDockerNodeActivity().Execute(ctx, input)
}
