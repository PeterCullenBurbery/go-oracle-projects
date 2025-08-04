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
	const targetSchema = "pdb_2025_008_004_010_033_019"

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

	// Quick sanity check like your original program
	var dbname string
	if err := db.QueryRowContext(ctx, "SELECT name FROM v$database").Scan(&dbname); err != nil {
		log.Fatalf("❌ Query failed: %v", err)
	}
	fmt.Printf("✅ Database name: %s\n", dbname)

	var sysdate string
	if err := db.QueryRowContext(ctx, "SELECT TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') FROM dual").Scan(&sysdate); err != nil {
		log.Fatalf("❌ Query failed: %v", err)
	}
	fmt.Printf("📅 SYSDATE: %s\n", sysdate)

	// 1) Set current schema
	if _, err := db.ExecContext(ctx, "ALTER SESSION SET CURRENT_SCHEMA = :1", targetSchema); err != nil {
		log.Fatalf("❌ Failed to set CURRENT_SCHEMA: %v", err)
	}
	fmt.Printf("🔧 CURRENT_SCHEMA set to: %s\n", targetSchema)

	// 2) Create or replace the function
	createFunc := `
CREATE OR REPLACE FUNCTION get_timestamp
   RETURN TIMESTAMP WITH TIME ZONE
AS
BEGIN
   RETURN CURRENT_TIMESTAMP;
END get_timestamp`
	if _, err := db.ExecContext(ctx, createFunc); err != nil {
		log.Fatalf("❌ Failed to create function: %v", err)
	}
	fmt.Println("🛠️  Function get_timestamp created/replaced successfully.")

	// 3) Verify current schema
	var currentSchema string
	if err := db.QueryRowContext(ctx, "SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') FROM dual").Scan(&currentSchema); err != nil {
		log.Fatalf("❌ Failed to read CURRENT_SCHEMA: %v", err)
	}
	fmt.Printf("🧭 Active schema: %s\n", currentSchema)

	// 4) Call the function and show result
	var ts string
	// Format the timestamp with timezone for a clean string result
	if err := db.QueryRowContext(
		ctx,
		"SELECT TO_CHAR(get_timestamp, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF TZH:TZM') FROM dual",
	).Scan(&ts); err != nil {
		log.Fatalf("❌ Failed to call get_timestamp: %v", err)
	}
	fmt.Printf("⏱️  get_timestamp(): %s\n", ts)
}
