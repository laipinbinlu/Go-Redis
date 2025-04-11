package cluster

import "go_redis/interface/resp"

// 本地命令
func execSelect(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply {
	return cluster.db.Exec(c, cmdArgs)
}
