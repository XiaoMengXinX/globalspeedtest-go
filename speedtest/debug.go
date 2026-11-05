package speedtest

import (
	"log"
	"os"
	"sync/atomic"
)

type Debug struct {
	dbg  *log.Logger
	flag atomic.Bool
}

func NewDebug() *Debug {
	return &Debug{dbg: log.New(os.Stderr, "[DBG]", log.Ldate|log.Ltime)}
}

func (d *Debug) Enable() {
	d.flag.Store(true)
}

func (d *Debug) Println(v ...any) {
	if d.flag.Load() {
		d.dbg.Println(v...)
	}
}

func (d *Debug) Printf(format string, v ...any) {
	if d.flag.Load() {
		d.dbg.Printf(format, v...)
	}
}

var dbg = NewDebug()
