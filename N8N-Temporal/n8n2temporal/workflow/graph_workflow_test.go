package workflow

import (
	"context"
	_ "embed"
	"github.com/bytedance/sonic"
	"go.temporal.io/sdk/client"
	"log"
	"n8n2temporal/pkg/util"
	"testing"
)

// workflow流程
var (
	//go:embed examples/workflow.json
	workflow     string
	workflowData = map[string]interface{}{
		"sop_domain_resolve_dsl": sopDomainResolve,
		"initial_data": map[string]interface{}{
			"a_record":     []map[string]interface{}{{"domain": "example.com", "query_type": "A"}, {"domain": "baidu.com", "query_type": "A"}},
			"mx_record":    []map[string]interface{}{{"domain": "google.ca", "query_type": "MX"}, {"domain": "baidu.com", "query_type": "MX"}},
			"cname_record": []map[string]interface{}{{"domain": "m.mediawiki.org", "query_type": "CNAME"}, {"domain": "baidu.com", "query_type": "CNAME"}},
			"out_nodes":    []string{"A记录", "MX记录", "CNAME记录"},
		},
		"out_nodes": []string{"sop_domain_resolve"},
	}
)

// 执行入口： go test -v -run "^TestWorkflowSOP$"
func TestWorkflowSOP(t *testing.T) {
	// 连接
	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalln("Unable to create client", err)
	}
	defer c.Close()
	// 解析初始输入值
	options := client.StartWorkflowOptions{
		ID:        util.UUID(),
		TaskQueue: "n8n-conversion-queue-new",
	}
	args := GenericWorkflowInput{
		WorkflowJSON: workflow,
		InitialData:  workflowData,
	}
	we, err := c.ExecuteWorkflow(context.Background(), options, GenericWorkflowWithMaxStep, args)
	if err != nil {
		log.Fatalln("Unable to execute workflow", err)
	}
	var ires interface{}
	err = we.Get(context.Background(), &ires)
	if err != nil {
		log.Println("Unable get workflow ires", err)
	}
	println("Workflow ires:")
	println(sonic.MarshalString(ires))
}
