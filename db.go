package fullcache

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"fmt"
)

func dbConn(database, host string, port int, user, password string) (db *sql.DB, err error) {
	return sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8", user, password, host, port, database))
}
