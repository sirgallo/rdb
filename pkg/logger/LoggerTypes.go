package clog


type CustomLog struct {
    Name string
}

type LogLevel string
type LogColor string 

const (
    Debug LogLevel = "Debug"
    Error LogLevel = "Error"
    Info LogLevel = "Info"
    Warn LogLevel = "Warn"
)

const Reset = "\033[0m"
const Bold = "\033[1m"

// ANSI escape codes for text colors
const (
    DebugColor LogColor = "\033[34m"
    ErrorColor LogColor = "\033[31m"
    InfoColor LogColor = "\033[32m"
    WarnColor LogColor = "\033[33m"
)