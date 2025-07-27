package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

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
	// Generate random integers
	rand.Seed(time.Now().UnixNano())
	lower := 1_000_000
	upper := 999_999_999
	randomint1 := rand.Intn(upper-lower+1) + lower
	randomint2 := rand.Intn(upper-lower+1) + lower

	// Load config
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

	// Query Oracle with values
	var product string
	query := `
        SELECT TO_CHAR(:1 * :2) FROM dual
    `
	err = db.QueryRowContext(ctx, query, randomint1, randomint2).Scan(&product)
	if err != nil {
		log.Fatalf("❌ Query failed: %v", err)
	}

	// Print everything
	fmt.Printf("randomint1 = %d\n", randomint1)
	fmt.Printf("randomint2 = %d\n", randomint2)
	fmt.Println("\nSELECT")
	fmt.Printf("  %d * %d AS product\n", randomint1, randomint2)
	fmt.Println("FROM dual;")
	fmt.Printf("\n🎲 Result: %s\n", product)
}