package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	DatabaseName = "db"
	TableSQL     = "CREATE TABLE `%s` (" +
		"  `id` int(11) NOT NULL AUTO_INCREMENT," +
		"  `k` int(11) NOT NULL DEFAULT '0'," +
		"  `c` char(120) COLLATE utf8mb4_general_ci NOT NULL DEFAULT ''," +
		"  `pad` char(60) COLLATE utf8mb4_general_ci NOT NULL DEFAULT ''," +
		"  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */," +
		"  KEY `k_613` (`k`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci /*T![auto_id_cache] AUTO_ID_CACHE=1 */"
)

func prepare(host string, port, databaseCnt int) {
	db, err := sql.Open("mysql", fmt.Sprintf("root@tcp(%s:%d)/", host, port))
	if err != nil {
		fmt.Printf("Failed to connect to MySQL database: %v\n", err)
		return
	}
	defer db.Close()

	for i := 0; i < databaseCnt; i++ {
		_, err = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s_%d", DatabaseName, i))
		if err != nil {
			fmt.Printf("Failed to drop database %s_%d: %v\n", DatabaseName, i, err)
		} else {
			fmt.Printf("Dropped database %s_%d\n", DatabaseName, i)
		}
		_, err = db.Exec(fmt.Sprintf("CREATE DATABASE %s_%d", DatabaseName, i))
		if err != nil {
			fmt.Printf("Failed to create database %s_%d: %v\n", DatabaseName, i, err)
		} else {
			fmt.Printf("Created database %s_%d\n", DatabaseName, i)
		}
		_, err = db.Exec(fmt.Sprintf("SET GLOBAL tidb_enable_fast_create_table=ON"))
		if err != nil {
			fmt.Printf("Failed to set fast create table, %v", err)
		}
	}

}

func cleanUp(host string, port, databaseCnt int) {
	db, err := sql.Open("mysql", fmt.Sprintf("root@tcp(%s:%d)/", host, port))
	if err != nil {
		fmt.Printf("Failed to connect to MySQL database: %v\n", err)
		return
	}
	defer db.Close()
	for i := 0; i < databaseCnt; i++ {
		_, err = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s_%d", DatabaseName, i))
		if err != nil {
			fmt.Printf("Failed to drop database %s_%d: %v\n", DatabaseName, i, err)
		} else {
			fmt.Printf("Dropped database %s_%d\n", DatabaseName, i)
		}
	}
}

func main() {
	host := flag.String("host", "10.2.6.51", "host")
	port := flag.Int("port", 4000, "port")
	thread := flag.Int("thread", 8, "thread")
	databaseCnt := flag.Int("database", 1, "database")
	tableCnt := flag.Int("table", 1, "table")
	username := flag.String("username", "root", "username")

	flag.Parse()
	fmt.Printf("host: %s, port: %d, thread: %d, database: %d, table: %d\n", *host, *port, *thread, *databaseCnt, *tableCnt)

	prepare(*host, *port, *databaseCnt)

	start := time.Now()
	for i := 0; i < *databaseCnt; i++ {
		startDB := time.Now()
		db, err := sql.Open("mysql", fmt.Sprintf("%s@tcp(%s:%d)/%s_%d", *username, *host, *port, DatabaseName, i))
		if err != nil {
			fmt.Printf("Failed to connect to MySQL database: %v\n", err)
			return
		}
		dbconns := make([]*sql.Conn, 0, *thread)

		for i := 0; i < *thread; i++ {
			conn, err := db.Conn(context.Background())
			if err != nil {
				fmt.Printf("Failed to connect to MySQL database: %v\n", err)
				db.Close()
				return
			}
			dbconns = append(dbconns, conn)
		}

		var wg sync.WaitGroup
		for i := 0; i < *thread; i++ {
			wg.Add(1)
			go createTable(dbconns[i], &wg, i, *tableCnt)
		}
		wg.Wait()
		totalTimeDB := time.Since(startDB)
		fmt.Printf("Created %d tables in database %s_%d, time %v\n", *tableCnt * *thread, DatabaseName, i, totalTimeDB)
		for _, conn := range dbconns {
			conn.Close()
		}
		db.Close()
	}
	totalTime := time.Since(start)
	fmt.Printf("Total execution time: %v\n", totalTime)
	//cleanUp()
}

func createTable(db *sql.Conn, wg *sync.WaitGroup, idx int, tableCnt int) {
	for i := 0; i < tableCnt; i++ {
		tableName := fmt.Sprintf("tb_%d_%d", idx, i)
		tableCreateSQL := fmt.Sprintf(TableSQL, tableName)
		_, err := db.ExecContext(context.Background(), tableCreateSQL)
		if err != nil {
			fmt.Printf("Error creating table %s: %s\n", tableName, err.Error())
		}
	}
	wg.Done()
}
