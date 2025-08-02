package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/goccy/go-yaml"
	_ "github.com/godror/godror"
)

type oracle_config struct {
	Username     string `yaml:"username"`
	Password     string `yaml:"password"`
	Host         string `yaml:"host"`
	Port         int    `yaml:"port"`
	Service_name string `yaml:"service_name"`
}

type config struct {
	Oracle_connection oracle_config `yaml:"oracle_connection"`
}

func load_config(path string) (*config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func main() {
	cfg, err := load_config("sysdba.yaml")
	if err != nil {
		log.Fatalf("❌ Failed to load config: %v", err)
	}

	oracle := cfg.Oracle_connection
	dsn := fmt.Sprintf(
		`user="%s" password="%s" connectString="%s:%d/%s" adminRole=SYSDBA`,
		oracle.Username, oracle.Password, oracle.Host, oracle.Port, oracle.Service_name,
	)

	db, err := sql.Open("godror", dsn)
	if err != nil {
		log.Fatalf("❌ Failed to open connection: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	rows, err := db.QueryContext(ctx, "SELECT name FROM v$datafile")
	if err != nil {
		log.Fatalf("❌ Query failed: %v", err)
	}
	defer rows.Close()

	fmt.Println("📁 Datafiles:")
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			log.Fatalf("❌ Row scan failed: %v", err)
		}
		fmt.Printf(" - %s\n", name)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("❌ Rows error: %v", err)
	}
}