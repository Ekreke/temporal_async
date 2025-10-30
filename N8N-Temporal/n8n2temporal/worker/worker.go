package main

import (
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"log"
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
	w := worker.New(c, "n8n-conversion-queue-new", worker.Options{})

	// 注册工作流和活动
	w.RegisterWorkflow(workflow.GenericWorkflowWithMaxStep)

	// 注册所有活动
	w.RegisterActivity(workflow.ExecuteDomainResolve)
	w.RegisterActivity(workflow.ExecuteCustomNode)

	// 注册v2.0.0节点活动函数
	w.RegisterActivity(workflow.ExecuteStartNode)
	w.RegisterActivity(workflow.ExecuteEndNode)
	w.RegisterActivity(workflow.ExecuteVariableNode)
	w.RegisterActivity(workflow.ExecuteConditionalNode)
	w.RegisterActivity(workflow.ExecutePythonDockerNode)

	// 启动 Worker
	log.Println("启动 n8n 转换 Worker...")
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalln("Worker 运行失败:", err)
	}
}
