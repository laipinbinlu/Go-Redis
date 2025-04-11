package parser

// 解析器

import (
	"bufio"
	"errors"
	"go_redis/interface/resp"
	"go_redis/lib/logger"
	"go_redis/resp/reply"
	"io"
	"runtime/debug"
	"strconv"
	"strings"
)

// 用户解析之后的数据结构
type Payload struct {
	Data resp.Reply
	Err  error
}

// 解析器的解析指令的状态
type readState struct {
	readingMultiLine  bool     // 是否读取的是多行数据
	expectedArgsCount int      //希望读取到的参数的数目
	msgType           byte     //消息的类型
	args              [][]byte //解析参数的具体内容
	bulkLen           int64    //字符串的长度
}

func (s *readState) finished() bool { // 当前的是否解析完成。
	return s.expectedArgsCount > 0 && len(s.args) == s.expectedArgsCount
}

// 异步解析数据 调用redis对客户的命令进行解析   并发执行对于每个连接使用gorutine 进行解析
func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch
}

// 读取用户传入的内容, 将取得的内容放入到chan中
func parse0(reader io.Reader, ch chan<- *Payload) {
	defer func() { // 防止发生pannic错误，导致系统崩溃
		if err := recover(); err != nil {
			logger.Error(string(debug.Stack()))
		}
	}()

	bufReader := bufio.NewReader(reader) // 设置读取缓冲区，因为传入的数据都是多行数据（resp协议）
	var state readState                  // 解析器的状态
	var err error
	var msg []byte
	for {
		var ioErr bool
		msg, ioErr, err = readLine(bufReader, &state) // 不断地循环读取一行数据，并且进行数据校验
		if err != nil {
			if ioErr { // 发生了io错误
				ch <- &Payload{
					Err: err,
				}
				close(ch)
				return // 发生io错误，直接终止连接，关闭通道
			}
			// 就是协议错误，直接输出错误即可，继续接受用户的输入
			ch <- &Payload{
				Err: err,
			}
			state = readState{} // 初始化状态
			continue
		}
		// 正常情况 ->数据校验无误
		// 判断当前是否为多行解析模式
		if !state.readingMultiLine { // *3\r\n ------
			if msg[0] == '*' {
				err := parseMultiBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: err,
					}
					state = readState{}
					continue
				}
				if state.expectedArgsCount == 0 {
					ch <- &Payload{
						Data: &reply.EmptyMultiBulkReply{},
					}
					state = readState{}
					continue
				}
			} else if msg[0] == '$' { //$4\r\nPONG\r\n   这个也是多行模式，2行模式
				err := parseBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: err,
					}
					state = readState{}
					continue
				}
				// 特殊情况  $-1\r\n     空指令
				if state.bulkLen == -1 {
					ch <- &Payload{
						Data: &reply.NullBulkReply{},
					}
					state = readState{}
					continue
				}
			} else { // + - : 这三种情况   单行模式
				result, err := parsrseSingLineReply(msg)
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{}
				continue
			}

		} else { // 多行模式下
			err := readBody(msg, &state)
			if err != nil {
				ch <- &Payload{
					Err: err,
				}
				state = readState{}
				continue
			}
			if state.finished() { // 判断读取是否完成
				// 已经完成了，那么就是返回结果了
				var result resp.Reply
				if state.msgType == '*' {
					result = reply.MakeMultiBulkReply(state.args)
				} else if state.msgType == '$' { // 同样还是多行，将取到的[][]byte 命令封装
					//result = reply.MakeBulkReply(state.args)   // 最初的代码
					result = reply.MakeMultiBulkReply(state.args)
				}
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{} // 读取完数据之后，重置状态
			}
		}
	}
}

// *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
// 读一行数据   -->返回的是 数据，io错误，具体的错误
func readLine(bufReader *bufio.Reader, state *readState) ([]byte, bool, error) { //\r\n换行符
	var msg []byte
	var err error
	if state.bulkLen == 0 { // 情况1：没有字节数，那么直接按照\r\n进行分割，初始状态   *3\r\n    $3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
		msg, err = bufReader.ReadBytes('\n')
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg)-2] != '\r' { // 为空，并且倒数第二个不是'\r'
			return nil, false, errors.New("prorocol error: " + string(msg))
		}

	} else { // 情况2 已经读到了$数字， 则按照数字指定的字节数读取，其实就是对字节数进行校验
		msg = make([]byte, state.bulkLen+2)
		_, err := io.ReadFull(bufReader, msg)
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg)-1] != '\n' || msg[len(msg)-2] != '\r' {
			return nil, false, errors.New("prorocol error: " + string(msg))
		}
		state.bulkLen = 0 // 读取完毕之后，重置为0

	}
	return msg, false, nil // 都没有出现上述错误，则返回正确的结果。
}

// readliine 只是读取一行的数据，parseMultiBulkHeader处理读取数据的消息-->多行数据
// *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n     *开头  的头部数据 并且设置解析器的状态
func parseMultiBulkHeader(msg []byte, state *readState) error {
	var err error
	var expectedLine uint64
	expectedLine, err = strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if expectedLine == 0 { // 为0
		state.expectedArgsCount = 0
		return nil
	} else if expectedLine > 0 { // 读取到多行数据大于0
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = int(expectedLine)
		state.args = make([][]byte, 0, expectedLine) // 数据大小
		return nil
	} else { // 小于0 的情况，显然是不对的
		return errors.New("protocol error: " + string(msg))
	}
}

// $4\r\nPING\r\n    $单行数据 解析头部
func parseBulkHeader(msg []byte, state *readState) error {
	var err error
	state.bulkLen, err = strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if state.bulkLen == -1 {
		return nil
	} else if state.bulkLen > 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = 1
		state.args = make([][]byte, 0, 1)
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}

// TODO:这里在解析数据时会存在类型转化的问题
// +OK\r\n  -err\r\n  :5\r\n
func parsrseSingLineReply(msg []byte) (resp.Reply, error) { // 解析用户传入的单行数据
	str := strings.TrimSuffix(string(msg), "\r\n") // 去掉末尾的\r\n
	var result resp.Reply                          // 用户传入的数据和回复的数据结构其实是一模一样的
	switch msg[0] {
	case '+':
		result = reply.MakeStatusReply(str[1:])
	case '-':
		result = reply.MakeErrReply(str[1:])
	case ':':
		val, err := strconv.ParseInt(string(str[1:]), 10, 64) // 将字符串转化为对应进制的位数的int
		if err != nil {
			return nil, errors.New("protocol error: " + string(msg)) // 返回协议错误
		}
		result = reply.MakeIntReply(val)
	}
	return result, nil
}

// 读取数据部分   在用户解析完头部数据之后， 解析器的状态已经设置好了
// (*3\r\n)  $3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
//
// （$4\r\n)   PING\r\n
func readBody(msg []byte, state *readState) error {
	//读取单行的body  , 去除后面\r\n
	line := msg[0 : len(msg)-2]
	var err error
	if line[0] == '$' { //1. $数字的情况
		state.bulkLen, err = strconv.ParseInt(string(line[1:]), 10, 64) // 转化为数字， 设置当前的数据的长度
		if err != nil {
			return errors.New("protocol error: " + string(msg))
		}
		if state.bulkLen <= 0 { // $0\r\n 的场景
			state.args = append(state.args, []byte{})
			state.bulkLen = 0
		}
	} else { // 正常的数据部分(具体的内容)  -->直接合并
		state.args = append(state.args, line)
	}
	return nil
}
