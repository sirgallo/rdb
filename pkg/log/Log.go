package clog

import "fmt"
import "strings"

import "github.com/sirgallo/raft/pkg/utils"


func NewCustomLog(name string) *CustomLog {
	return &CustomLog{
			Name: name,
	}
}

func (cLog *CustomLog) Debug(msg ...interface{}) {
	cLog.formatOutput(Debug, msg)
} 

func (cLog *CustomLog) Error(msg ...interface{}) {
	cLog.formatOutput(Info, msg)
} 

func (cLog *CustomLog) Info(msg ...interface{}) {
	cLog.formatOutput(Info, msg)
} 

func (cLog *CustomLog) Warn(msg ...interface{}) {
	cLog.formatOutput(Warn, msg)
}

func (cLog *CustomLog) formatOutput(level LogLevel, msg []interface{}) {
	encodedMsg := func () string {
		encodeTransform := func(chunk interface{}) string {
			encoded, _ := utils.EncodeStructToString[interface{}](chunk)
			return encoded
		}
	
		encodedChunks := utils.Map[interface{}, string](msg, encodeTransform)
		return strings.Join(encodedChunks, " ")
	}()

	color := func () LogColor {
			if level == Debug { 
					return DebugColor 
			} else if level == Error { 
					return ErrorColor
			} else if level == Info {
					return InfoColor
			} else{
					return WarnColor
			}
	}()

	fmt.Printf("%s[%s] %s: %s\n", color, cLog.Name, level, encodedMsg)
}