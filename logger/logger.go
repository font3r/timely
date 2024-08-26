package logger

import (
	"log"
	"os"
)

var Logger = log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile)
