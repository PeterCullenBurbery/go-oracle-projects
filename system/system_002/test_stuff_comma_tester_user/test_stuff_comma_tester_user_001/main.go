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
	// Load config from YAML
	cfg, err := load_config("tester.yaml")
	if err != nil {
		log.Fatalf("❌ Failed to load config: %v", err)
	}
	oracle := cfg.Oracle_connection

	// Build DSN
	dsn := fmt.Sprintf(
		`user="%s" password="%s" connectString="%s:%d/%s"`,
		oracle.Username, oracle.Password, oracle.Host, oracle.Port, oracle.Service_name,
	)

	// Open connection
	db, err := sql.Open("godror", dsn)
	if err != nil {
		log.Fatalf("❌ Failed to open connection: %v", err)
	}
	defer db.Close()

	// Run query to get current user
	ctx := context.Background()
	var currentUser string
	err = db.QueryRowContext(ctx, "SELECT USER FROM dual").Scan(&currentUser)
	if err != nil {
		log.Fatalf("❌ Query failed: %v", err)
	}

	// Output
	fmt.Println()
	fmt.Println("SELECT USER FROM dual;")
	fmt.Printf("\n👤 Current Oracle user: %s\n", currentUser)
}