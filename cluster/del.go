package cluster

import (
	"go_redis/interface/resp"
	"go_redis/resp/reply"
)

// del k1 k2 k3 k4 k5   -> 删除的个数 3
func Del(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply {
	// 广播模式
	replies := cluster.broadcast(c, cmdArgs)
	var errReply reply.ErrorReply
	var deleted int64 = 0

	for _, r := range replies {
		if reply.IsErrReply(r) {
			errReply = r.(reply.ErrorReply)
			break
		}
		// 处理结果
		intreply, ok := r.(*reply.IntReply)
		if !ok {
			errReply = reply.MakeErrReply("error: cannot change to intReply")
		}
		deleted += intreply.Code // 加上返回的结果
	}

	if errReply == nil {
		return reply.MakeIntReply(deleted)
	}
	return reply.MakeErrReply("error: " + errReply.Error())
}
