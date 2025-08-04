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
	// >>> change only this if you want a different PDB <<<
	const target_container = "pdb_2025_008_004_010_033_019"

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

	// Show CDB name (root)
	var dbname string
	if err := db.QueryRowContext(ctx, "SELECT name FROM v$database").Scan(&dbname); err != nil {
		log.Fatalf("❌ Query failed (v$database): %v", err)
	}
	fmt.Printf("✅ CDB name: %s\n", dbname)

	// Switch container
	alter := fmt.Sprintf("ALTER SESSION SET CONTAINER = %s", target_container)
	if _, err := db.ExecContext(ctx, alter); err != nil {
		log.Fatalf("❌ Failed to alter session container to %s: %v", target_container, err)
	}
	fmt.Printf("🔁 Switched to PDB: %s\n", target_container)

	// Confirm container
	var con_name string
	if err := db.QueryRowContext(ctx, "SELECT SYS_CONTEXT('USERENV','CON_NAME') FROM dual").Scan(&con_name); err != nil {
		log.Fatalf("❌ Could not confirm container: %v", err)
	}
	fmt.Printf("📦 Current container: %s\n", con_name)

	// List datafiles in this PDB
	rows, err := db.QueryContext(ctx, "SELECT name FROM v$datafile ORDER BY name")
	if err != nil {
		log.Fatalf("❌ Query failed (v$datafile): %v", err)
	}
	defer rows.Close()

	fmt.Println("📄 Datafiles:")
	i := 0
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			log.Fatalf("❌ Row scan failed: %v", err)
		}
		i++
		fmt.Printf("  %2d) %s\n", i, name)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("❌ Rows error: %v", err)
	}
}