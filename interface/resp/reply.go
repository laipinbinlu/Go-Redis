package resp

type Reply interface {
	ToBytes() []byte // 转化为[]byte字节数   基于tcp之上的通信
}
