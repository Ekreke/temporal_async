import json

import requests

N8N_BASE_URL= "http://localhost:5678/"
API_KEY="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJjMWM0ZDk3Ni0xNGZlLTQ1N2YtODM1Yy00NmYzNGJkZjYxNjkiLCJpc3MiOiJuOG4iLCJhdWQiOiJwdWJsaWMtYXBpIiwiaWF0IjoxNzU5MTEzNTY1fQ.m9UqLvhZVkhL-fJAGf9RxB-foPWV64ZZ4jc1DyaKsJA"

# 获取工作流JSON结构表示
def get_workflow_json(workflow_id: str):
    url = f"{N8N_BASE_URL}/api/v1/workflows/{workflow_id}"
    headers = {"X-N8N-API-KEY": API_KEY, "Content-Type": "application/json"}
    try:
        resp = requests.get(url=url, headers=headers).json()
        print(json.dumps(resp))
        return resp
    except requests.exceptions.HTTPError as e:
        print("exception {}".format(e))
        return []


if __name__== "__main__":
    # 1、先获取工作流json，确保存在
    get_workflow_json("ciQFzLxpGwEMwAoH")
    # todo 2、检测每个节点的参数是否都有传递（直接执行一遍工作流、观测是否有报错）
    # 3、在N8N上配置自己的工作流节点
