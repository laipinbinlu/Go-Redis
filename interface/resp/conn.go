package resp

type Connection interface { // resp通信协议   客户端的相关操作
	Write([]byte) error
	GetDBIndex() int
	SelectDB(int)
}
