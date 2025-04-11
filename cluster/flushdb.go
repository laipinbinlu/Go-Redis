package cluster

import (
	"go_redis/interface/resp"
	"go_redis/resp/reply"
)

func flushdb(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply {
	// 广播模式
	replies := cluster.broadcast(c, cmdArgs)
	var errReply reply.ErrorReply
	for _, r := range replies {
		if reply.IsErrReply(r) {
			errReply = r.(reply.ErrorReply)
			break
		}
	}
	if errReply == nil {
		return reply.MakeOkReply()
	}
	return reply.MakeErrReply("error: " + errReply.Error())
}
