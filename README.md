# Binlog Fullcache

## Feature Overview
- 支持MySQL-Redis触发缓存更新
- 支持MySQL-Elasticsearch触发缓存更新

##Example
````
func main() {
	SyncerRun()
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

````

- 
## License
[MIT](https://github.com/tyingzh/fullcache/blob/master/LICENSE)
