package reply

// redis固定的错误回复

// ---------------未知错误---------------------
type UnknownErrReply struct{}

var unknownErrBytes = []byte("-Err unknown\r\n")

func (r *UnknownErrReply) Error() string { // 回复给该系统
	return "Err unknown"
}

func (r *UnknownErrReply) ToBytes() []byte { // 回复给客户端
	return unknownErrBytes
}

// ------ 某个指令的参数错误------
type ArgNumErrReply struct {
	Cmd string
}

func (r *ArgNumErrReply) Error() string {
	return "ERR wrong number of arguments for'" + r.Cmd + "' command"
}

func (r *ArgNumErrReply) ToBytes() []byte {
	return []byte("-ERR wrong number of arguments for'" + r.Cmd + "' command\r\n")
}

func MakeArgNumErrReply(cmd string) *ArgNumErrReply {
	return &ArgNumErrReply{
		Cmd: cmd,
	}
}

// --------语法错误------------
type SyntaxErrReply struct{}

var syntaxErrBytes = []byte("-Err syntax error\r\n")
var theSyntaxErrReply = &SyntaxErrReply{}

func (r *SyntaxErrReply) Error() string {
	return "Err syntax error"
}

func (r *SyntaxErrReply) ToBytes() []byte {
	return syntaxErrBytes
}

// MakeSyntaxErrReply creates syntax error
func MakeSyntaxErrReply() *SyntaxErrReply {
	return theSyntaxErrReply
}

// ----------数据类型错误---------------
type WrongTypeErrReply struct{}

var wrongTypeErrBytes = []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")

func (r *WrongTypeErrReply) Error() string {
	return "WRONGTYPE Operation against a key holding the wrong kind of value"
}

// ToBytes marshals redis.Reply
func (r *WrongTypeErrReply) ToBytes() []byte {
	return wrongTypeErrBytes
}

// ---------用户发送的消息不满足协议规范->协议错误----------
type ProtocolErrReply struct {
	Msg string
}

func (r *ProtocolErrReply) Error() string {
	return "ERR Protocol error '" + r.Msg + "' command"
}

// ToBytes marshals redis.Reply
func (r *ProtocolErrReply) ToBytes() []byte {
	return []byte("-ERR Protocol error: '" + r.Msg + "'\r\n")
}
