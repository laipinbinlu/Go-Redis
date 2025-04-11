package cluster

import "go_redis/interface/resp"

// 根据用户的指令确定模式  --->创建一个map: 指令对应的方法（该方法已经明确了模式的具体执行）----->只是负责转发到对应的节点
func makeRouter() map[string]CmdFunc {
	routerMap := make(map[string]CmdFunc)
	// 方法表  --路由表
	routerMap["exists"] = deafaultFunc
	routerMap["type"] = deafaultFunc
	routerMap["set"] = deafaultFunc
	routerMap["setnx"] = deafaultFunc
	routerMap["get"] = deafaultFunc
	routerMap["getset"] = deafaultFunc
	routerMap["ping"] = ping
	routerMap["rename"] = Rename
	routerMap["renamenx"] = Rename
	routerMap["flushdb"] = flushdb
	routerMap["del"] = Del
	routerMap["select"] = execSelect

	return routerMap
}

// 默认方法   GET K  SET K1 V1  转发模式
func deafaultFunc(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply {
	key := string(cmdArgs[1])
	peer := cluster.peerPicker.PickNode(key)
	return cluster.relay(peer, c, cmdArgs)
}
