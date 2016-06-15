// Copyright 2014 beego Author. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Usage:
//
// import "github.com/astaxie/beego/logs"
//
//	log := NewLogger(10000)
//	log.SetLogger("console", "")
//
//	> the first params stand for how many channel
//
// Use it like this:
//
//	log.Trace("trace")
//	log.Info("info")
//	log.Warn("warning")
//	log.Debug("debug")
//	log.Critical("critical")
//
//  more docs http://beego.me/docs/module/logs.md
package logs

import (
	"fmt"
	"path"
	"runtime"
	"sync"
)

const (
	_ = iota
	LevelInfo
	LevelError
	LevelWarn
	LevelDebug
)

// Legacy loglevel constants to ensure backwards compatibility.

type loggerType func() LoggerInterface

// LoggerInterface defines the behavior of a log provider.
type LoggerInterface interface {
	Init(config string) error
	WriteMsg(msg string, level int) error
	Destroy()
	Flush()
}

var adapters = make(map[string]loggerType)

// Register makes a log provide available by the provided name.
// If Register is called twice with the same name or if driver is nil,
// it panics.
func Register(name string, log loggerType) {
	if log == nil {
		panic("logs: Register provide is nil")
	}
	if _, dup := adapters[name]; dup {
		panic("logs: Register called twice for provider " + name)
	}
	adapters[name] = log
}

// BeeLogger is default logger in beego application.
// it can contain several providers and log message into all providers.
type BeeLogger struct {
	lock                sync.Mutex
	level               int
	enableFuncCallDepth bool
	loggerFuncCallDepth int
	asynchronous        bool
	msg                 chan *logMsg
	outputs             map[string]LoggerInterface
}

type logMsg struct {
	level int
	msg   string
}

// NewLogger returns a new BeeLogger.
// channellen means the number of messages in chan.
// if the buffering chan is full, logger adapters write to file or other way.
func NewLogger(channellen int64) *BeeLogger {
	bl := new(BeeLogger)
	bl.level = LevelDebug
	bl.loggerFuncCallDepth = 2
	bl.msg = make(chan *logMsg, channellen)
	bl.outputs = make(map[string]LoggerInterface)
	return bl
}

func (bl *BeeLogger) Async() *BeeLogger {
	bl.asynchronous = true
	go bl.startLogger()
	return bl
}

// SetLogger provides a given logger adapter into BeeLogger with config string.
// config need to be correct JSON as string: {"interval":360}.
func (bl *BeeLogger) SetLogger(adaptername string, config string) error {
	bl.lock.Lock()
	defer bl.lock.Unlock()
	if log, ok := adapters[adaptername]; ok {
		lg := log()
		err := lg.Init(config)
		bl.outputs[adaptername] = lg
		if err != nil {
			fmt.Println("logs.BeeLogger.SetLogger: " + err.Error())
			return err
		}
	} else {
		return fmt.Errorf("logs: unknown adaptername %q (forgotten Register?)", adaptername)
	}
	return nil
}

// remove a logger adapter in BeeLogger.
func (bl *BeeLogger) DelLogger(adaptername string) error {
	bl.lock.Lock()
	defer bl.lock.Unlock()
	if lg, ok := bl.outputs[adaptername]; ok {
		lg.Destroy()
		delete(bl.outputs, adaptername)
		return nil
	} else {
		return fmt.Errorf("logs: unknown adaptername %q (forgotten Register?)", adaptername)
	}
}

func (bl *BeeLogger) writerMsg(loglevel int, msg string) error {
	lm := new(logMsg)
	lm.level = loglevel
	if bl.enableFuncCallDepth {
		_, file, line, ok := runtime.Caller(bl.loggerFuncCallDepth)
		if !ok {
			file = "???"
			line = 0
		}
		_, filename := path.Split(file)
		lm.msg = fmt.Sprintf("[%s:%d] %s", filename, line, msg)
	} else {
		lm.msg = msg
	}
	if bl.asynchronous {
		bl.msg <- lm
	} else {
		for name, l := range bl.outputs {
			err := l.WriteMsg(lm.msg, lm.level)
			if err != nil {
				fmt.Println("unable to WriteMsg to adapter:", name, err)
				return err
			}
		}
	}
	return nil
}

// Set log message level.
//
// If message level (such as LevelDebug) is higher than logger level (such as LevelWarning),
// log providers will not even be sent the message.
func (bl *BeeLogger) SetLevel(l int) {
	bl.level = l
}

// set log funcCallDepth
func (bl *BeeLogger) SetLogFuncCallDepth(d int) {
	bl.loggerFuncCallDepth = d
}

// get log funcCallDepth for wrapper
func (bl *BeeLogger) GetLogFuncCallDepth() int {
	return bl.loggerFuncCallDepth
}

// enable log funcCallDepth
func (bl *BeeLogger) EnableFuncCallDepth(b bool) {
	bl.enableFuncCallDepth = b
}

// start logger chan reading.
// when chan is not empty, write logs.
func (bl *BeeLogger) startLogger() {
	for {
		select {
		case bm := <-bl.msg:
			for _, l := range bl.outputs {
				err := l.WriteMsg(bm.msg, bm.level)
				if err != nil {
					fmt.Println("ERROR, unable to WriteMsg:", err)
				}
			}
		}
	}
}

// Log INFORMATIONAL level message.
func (bl *BeeLogger) Info(format string, v ...interface{}) {
	if LevelInfo > bl.level {
		return
	}
	msg := fmt.Sprintf(format, v...)
	bl.writerMsg(LevelInfo, msg)
}

// Log ERROR level message.
func (bl *BeeLogger) Error(format string, v ...interface{}) {
	if LevelError > bl.level {
		return
	}
	msg := fmt.Sprintf(format, v...)
	bl.writerMsg(LevelError, msg)
}

// Log WARNING level message.
func (bl *BeeLogger) Warn(format string, v ...interface{}) {
	if LevelWarn > bl.level {
		return
	}
	msg := fmt.Sprintf(format, v...)
	bl.writerMsg(LevelWarn, msg)
}

// Log DEBUG level message.
func (bl *BeeLogger) Debug(format string, v ...interface{}) {
	if LevelDebug > bl.level {
		return
	}
	msg := fmt.Sprintf(format, v...)
	bl.writerMsg(LevelDebug, msg)
}

// flush all chan data.
func (bl *BeeLogger) Flush() {
	for _, l := range bl.outputs {
		l.Flush()
	}
}

// close logger, flush all chan data and destroy all adapters in BeeLogger.
func (bl *BeeLogger) Close() {
	for {
		if len(bl.msg) > 0 {
			bm := <-bl.msg
			for _, l := range bl.outputs {
				err := l.WriteMsg(bm.msg, bm.level)
				if err != nil {
					fmt.Println("ERROR, unable to WriteMsg (while closing logger):", err)
				}
			}
			continue
		}
		break
	}
	for _, l := range bl.outputs {
		l.Flush()
		l.Destroy()
	}
}
