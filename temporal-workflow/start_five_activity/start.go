// 启动5个Activity的Workflow
package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"test1/temporal-workflow"
	"test1/temporal-workflow/consts"

	"go.temporal.io/sdk/client"
)

func main() {
	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalln("Unable to create client", err)
	}
	defer c.Close()

	// 解析初始输入值
	initialInput := 10
	if len(os.Args) > 1 {
		if val, err := strconv.Atoi(os.Args[1]); err == nil {
			initialInput = val
		}
	}

	options := client.StartWorkflowOptions{
		ID:        "five-activity-workflow",
		TaskQueue: string(consts.Worker2),
	}

	we, err := c.ExecuteWorkflow(context.Background(), options, temporal_workflow.FiveActivityWorkflow, initialInput)
	if err != nil {
		log.Fatalln("Unable to execute workflow", err)
	}
	// log.Println("Started workflow", "WorkflowID", we.GetID(), "RunID", we.GetRunID())
	// c.SignalWorkflow(context.Background(), we.GetID(), we.GetRunID(), "signal-a1", my_example.ActivityInput{Value: initialInput})
	var result []int
	err = we.Get(context.Background(), &result)
	if err != nil {
		log.Fatalln("Unable get workflow result", err)
	}
	log.Println("Workflow result:", result)
}
