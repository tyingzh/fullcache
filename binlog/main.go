package main

import (
	"context"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"os"
	"time"
)

func main() {
	SyncerRun()
	 //CanalRun()
}

func SyncerRun() {
	// demo mysql binlog parser
	/*
	1. local myslq server binlog ON and row
	2.
	 */
	//tables := make(map[replication.EventType]string)
	cfg := replication.BinlogSyncerConfig {
		ServerID: 100,
		Flavor:   "mysql",
		Host:     "127.0.0.1",
		Port:     3306,
		User:     "root",
		Password: "123456",
	}
	syncer := replication.NewBinlogSyncer(cfg)
	// Start sync with specified binlog file and position
	streamer, _ := syncer.StartSync(mysql.Position{"mysql-bin.000003", 154})
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		ev, err := streamer.GetEvent(ctx)
		cancel()

		if err == context.DeadlineExceeded {
			// meet timeout
			continue
		}
		switch ev.Header.EventType {
		case replication.TABLE_MAP_EVENT:
			e, err := ev.Event.(*replication.TableMapEvent)
			fmt.Println(string(e.Table),e.TableID, err)

		}
		ev.Dump(os.Stdout)
		fmt.Println("============")
	}
}

//
func CanalRun() {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = "127.0.0.1:3306"
	cfg.User = "root"
	// We only care table canal_test in test db
	cfg.Dump.TableDB = "supplier"

	c, err := canal.NewCanal(cfg)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	// Register a handler to handle RowsEvent
	c.SetEventHandler(&MyEventHandler{})

	// Start canal
	c.Run()
	//c.Start()
}

type MyEventHandler struct {
	canal.DummyEventHandler
}

func (h *MyEventHandler) OnRow(e *canal.RowsEvent) error {

	fmt.Printf("%s %v\n", e.Action, e.Rows)
	return nil
}

func (h *MyEventHandler) String() string {
	return "MyEventHandler"
}