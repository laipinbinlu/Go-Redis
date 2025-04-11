package reply

import (
	"bytes"
	"go_redis/interface/resp"
	"strconv"
)

// redis的一些动态的回复
var (
	CRLF = "\r\n"
)

// ------字符串回复------------  遵守resp协议  实现reply接口
type BulkReply struct {
	Arg []byte // 标准的回复 "$8\r\nwangbiao\r\n"
}

func (r *BulkReply) ToBytes() []byte {
	if r.Arg == nil {
		return nullBulkBytes
	}
	return []byte("$" + strconv.Itoa(len(r.Arg)) + CRLF + string(r.Arg) + CRLF)
}

func MakeBulkReply(arg []byte) *BulkReply {
	return &BulkReply{Arg: arg}
}

// ---------多行字符串回复---------------
type MultiBulkReply struct {
	Args [][]byte
}

func (r *MultiBulkReply) ToBytes() []byte { // 遍历二维数组依次z转换为对应的byte格式
	var buf bytes.Buffer
	argLen := len(r.Args)
	buf.WriteString("*" + strconv.Itoa(argLen) + CRLF)
	for _, arg := range r.Args {
		if arg == nil {
			buf.WriteString(string(nullBulkBytes))
		} else {
			buf.WriteString("$" + strconv.Itoa(len(arg)) + CRLF + string(arg) + CRLF)
		}
	}
	return buf.Bytes()
}

func MakeMultiBulkReply(args [][]byte) *MultiBulkReply {
	return &MultiBulkReply{Args: args}
}

// ------------状态回复-----------
type StatusReply struct {
	Status string
}

func (r *StatusReply) ToBytes() []byte {
	return []byte("+" + r.Status + CRLF)
}

func MakeStatusReply(s string) *StatusReply {
	return &StatusReply{Status: s}
}

// ------------数字回复----------
type IntReply struct {
	Code int64
}

func (r *IntReply) ToBytes() []byte {
	return []byte(":" + strconv.FormatInt(r.Code, 10) + CRLF)
}

func MakeIntReply(code int64) *IntReply {
	return &IntReply{Code: code}
}

// 固定错误reply接口
type ErrorReply interface {
	Error() string
	ToBytes() []byte
}

// -----------------标准状态的错误回复---------------
type StandardErrReply struct {
	Status string
}

// MakeErrReply creates StandardErrReply
func MakeErrReply(status string) *StandardErrReply {
	return &StandardErrReply{
		Status: status,
	}
}

// ToBytes marshal redis.Reply
func (r *StandardErrReply) ToBytes() []byte {
	return []byte("-" + r.Status + CRLF)
}

func (r *StandardErrReply) Error() string {
	return r.Status
}

// 判断当前的回复是否为错误的回复
func IsErrReply(reply resp.Reply) bool {
	return reply.ToBytes()[0] == '-'
}
