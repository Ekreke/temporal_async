package main

import (
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"log"
	"n8n2temporal/activity"
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
	w.RegisterWorkflow(workflow.GenericWorkflow)

	// 注册所有活动
	w.RegisterActivity((&activity.DomainResolveActivity{}).ExecuteDomainResolve)
	w.RegisterActivity((&activity.PythonCodeActivity{}).ExecutePythonCode)
	w.RegisterActivity((&activity.CustomNodeActivity{}).ExecuteCustomNode)
	w.RegisterActivity((&activity.ConditionCheckActivity{}).ExecuteIf)
	w.RegisterActivity((&activity.SwitchNodeActivity{}).ExecuteSwitchNode)

	// 启动 Worker
	log.Println("启动 n8n 转换 Worker...")
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalln("Worker 运行失败:", err)
	}
}
