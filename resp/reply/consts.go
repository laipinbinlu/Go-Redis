package reply

// 一些固定的redis对客户端的正常回复  --- 5种reply

// -------------------PONG---------------------
type PongReply struct{}

var pongbytes = []byte("+PONG\r\n") //客户端ping   那么redis就是回复pong (遵循回复的协议)

func (r PongReply) ToBytes() []byte {
	return pongbytes
}

func MakePongReply() *PongReply {
	return &PongReply{}
}

// ------------------------OK ---------------
type OkReply struct{}

var okBytes = []byte("+OK\r\n")

func (r OkReply) ToBytes() []byte {
	return okBytes
}

var theOkReply = new(OkReply)

func MakeOkReply() *OkReply { // 节约内存，就是单例模式
	return theOkReply
}

// ------------NULL---------------------
type NullBulkReply struct{}

var nullBulkBytes = []byte("$-1\r\n")

func (r NullBulkReply) ToBytes() []byte {
	return nullBulkBytes
}

func MakeNullBulkReply() *NullBulkReply {
	return &NullBulkReply{}
}

// --------------------空字符串----------------
type EmptyMultiBulkReply struct{}

var emptyMultiBulkBytes = []byte("*0\r\n")

func (r EmptyMultiBulkReply) ToBytes() []byte {
	return emptyMultiBulkBytes
}

func MakeEmptyMultiBulkReply() *EmptyMultiBulkReply {
	return &EmptyMultiBulkReply{}
}

// ----------------空回复------------------
type NoReply struct{}

var noBytes = []byte("")

func (r NoReply) ToBytes() []byte {
	return noBytes
}

func MakeNoRply() *NoReply {
	return &NoReply{}
}
