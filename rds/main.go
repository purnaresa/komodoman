package main

import (
	"database/sql"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	_ "github.com/go-sql-driver/mysql"
)

func init() {
	log.SetLevel(log.DebugLevel)
	log.WithField("status", "starting").Debug("initialize")

	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")

	errConfig := viper.ReadInConfig()
	if errConfig != nil {
		log.Fatalln(errConfig)
	}
	log.WithField("status", "success").Debug("initialize")
}

func main() {
	log.Println("test")
	for i := 0; i < 10; i++ {
		timer := time.Now()
		db := connectRDS()
		testQuery(db)
		log.Debug(time.Since(timer).Milliseconds())
	}

}

func connectRDS() (db *sql.DB) {
	log.WithField("status", "starting").Info("connectRDS")
	dbConn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		viper.GetString("RDSUSER"),
		viper.GetString("RDSPASSWORD"),
		viper.GetString("RDSHOST"),
		viper.GetString("RDSPORT"),
		viper.GetString("DBNAME"))
	db, err := sql.Open("mysql", dbConn)
	if err != nil {
		panic(err)
	}
	err = db.Ping()
	if err != nil {
		panic(err)
	}

	log.WithField("status", "success").Info("connectRDS")
	return
}
func testQuery(db *sql.DB) {
	type movieType struct {
		ID    int
		Email string
	}

	rows, err := db.Query("select * from authors")
	if err != nil {
		log.Fatalln(err)
	}

	defer rows.Close()
	for rows.Next() {
		m := movieType{}
		err := rows.Scan(&m.ID, &m.Email)
		if err != nil {
			log.Fatalln(err)
		}
		log.Debug(m)
	}
}
