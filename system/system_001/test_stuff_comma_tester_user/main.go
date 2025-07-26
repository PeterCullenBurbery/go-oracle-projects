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
	cfg, err := load_config("tester.yaml")
	if err != nil {
		log.Fatalf("❌ Failed to load config: %v", err)
	}

	oracle := cfg.Oracle_connection
	dsn := fmt.Sprintf(
		`user="%s" password="%s" connectString="%s:%d/%s"`,
		oracle.Username, oracle.Password, oracle.Host, oracle.Port, oracle.Service_name,
	)

	db, err := sql.Open("godror", dsn)
	if err != nil {
		log.Fatalf("❌ Failed to open connection: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	var result string
	err = db.QueryRowContext(ctx, `
        SELECT 
            TO_CHAR(num1) || ' × ' || TO_CHAR(num2) || ' = ' || TO_CHAR(num1 * num2) AS result
        FROM (
            SELECT 
                TRUNC(DBMS_RANDOM.VALUE(1000000, 1000000000)) AS num1,
                TRUNC(DBMS_RANDOM.VALUE(1000000, 1000000000)) AS num2
            FROM dual
        )
    `).Scan(&result)
	if err != nil {
		log.Fatalf("❌ Query failed: %v", err)
	}

	fmt.Printf("🎲 Random multiplication: %s\n", result)
}