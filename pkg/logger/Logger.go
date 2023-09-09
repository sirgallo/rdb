package clog

import "fmt"
import "strings"
import "time"

import "github.com/sirgallo/raft/pkg/utils"


//=========================================== Logger


func NewCustomLog(name string) *CustomLog {
	return &CustomLog{
		Name: name,
	}
}

/*
	Debug, Error, Info, Warn:
		different log levels
*/

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

/*
	Format Output:
		helper method for each of the log levels
		output is
			(formatted time) [name] Log level: encoded message
*/

func (cLog *CustomLog) formatOutput(level LogLevel, msg []interface{}) {
	currTime := time.Now()
	formattedTime := currTime.Format(TimeFormat)

	encodedMsg := func() string {
		encodeTransform := func(chunk interface{}) string {
			encoded, _ := utils.EncodeStructToString[interface{}](chunk)
			return encoded
		}
	
		encodedChunks := utils.Map[interface{}, string](msg, encodeTransform)
		return strings.Join(encodedChunks, " ")
	}()

	color := func() LogColor {
		if level == Debug { 
			return DebugColor 
		} else if level == Error { 
			return ErrorColor
		} else if level == Info {
			return InfoColor
		} else { return WarnColor }
	}()

	fmt.Printf("%s(%s) [%s] %s: %s\n", color, formattedTime, cLog.Name, Bold + level, Reset + encodedMsg)
}