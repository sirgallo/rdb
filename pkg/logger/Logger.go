package clog

import "fmt"
import "os"
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
	go cLog.formatOutput(Debug, msg)
} 

func (cLog *CustomLog) Error(msg ...interface{}) {
	go cLog.formatOutput(Error, msg)
} 

func (cLog *CustomLog) Info(msg ...interface{}) {
	go cLog.formatOutput(Info, msg)
} 

func (cLog *CustomLog) Warn(msg ...interface{}) {
	go cLog.formatOutput(Warn, msg)
}

func (cLog *CustomLog) Fatal(msg ...interface{}) {
	cLog.formatOutput(Error, msg)
	os.Exit(1)
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
			encoded, _ := utils.EncodeStructToJSONString[interface{}](chunk)
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