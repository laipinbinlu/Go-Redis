package cluster

import (
	"go_redis/interface/resp"
	"go_redis/resp/reply"
)

// 当前系统功能不全，所以只有当在本节点的key才可以rename

// rename k1 k2
func Rename(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply {
	if len(cmdArgs) != 3 {
		return reply.MakeErrReply("Err Wrong number args")
	}
	src := string(cmdArgs[1])  // k1
	dest := string(cmdArgs[2]) // k2
	// 查找不同key所对因的节点
	srcpeer := cluster.peerPicker.PickNode(src)
	destpeer := cluster.peerPicker.PickNode(dest)

	if srcpeer != destpeer {
		return reply.MakeErrReply("Err rename must within on the same peer")
	}

	// 直接转发给对应的peer执行
	return cluster.relay(srcpeer, c, cmdArgs)
}
