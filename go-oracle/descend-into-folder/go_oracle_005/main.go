package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"strings"

	"github.com/PeterCullenBurbery/go_functions_002/v5/date_time_functions"
	"github.com/goccy/go-yaml"
	_ "github.com/godror/godror"

	"os"
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
	const target_container = "pdb_2025_008_004_010_033_019" // change if needed
	const password = "f"

	// 1) generate username with helper
	gen, err := date_time_functions.Generate_prefixed_timestamp("user_slash_schema")
	if err != nil {
		log.Fatalf("❌ failed to generate timestamped username: %v", err)
	}
	username := sanitize_oracle_identifier(gen)

	// 2) connect as sysdba
	cfg, err := load_config("sysdba.yaml")
	if err != nil {
		log.Fatalf("❌ failed to load config: %v", err)
	}
	oc := cfg.Oracle_connection
	dsn := fmt.Sprintf(`user="%s" password="%s" connectString="%s:%d/%s" adminRole=SYSDBA`,
		oc.Username, oc.Password, oc.Host, oc.Port, oc.Service_name)

	db, err := sql.Open("godror", dsn)
	if err != nil {
		log.Fatalf("❌ failed to open connection: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	// show db name
	var cdb string
	if err := db.QueryRowContext(ctx, "SELECT name FROM v$database").Scan(&cdb); err != nil {
		log.Fatalf("❌ query failed (v$database): %v", err)
	}
	fmt.Printf("✅ CDB: %s\n", cdb)

	// 3) switch to pdb
	if _, err := db.ExecContext(ctx, "ALTER SESSION SET CONTAINER = "+target_container); err != nil {
		log.Fatalf("❌ failed to alter session container to %s: %v", target_container, err)
	}
	var con string
	if err := db.QueryRowContext(ctx, "SELECT SYS_CONTEXT('USERENV','CON_NAME') FROM dual").Scan(&con); err != nil {
		log.Fatalf("❌ could not confirm container: %v", err)
	}
	fmt.Printf("📦 Current container: %s\n", con)

	// 4) try CREATE USER; if ORA-00972 (identifier too long), retry with truncated name
	create_stmt := fmt.Sprintf("CREATE USER %s IDENTIFIED BY %s", username, password)
	if _, err := db.ExecContext(ctx, create_stmt); err != nil {
		if is_identifier_too_long(err) {
			short := truncate_identifier(username, 30) // safe floor if DB enforces 30
			fmt.Printf("⚠️ identifier too long; retrying with: %s\n", short)
			username = short
			if _, err2 := db.ExecContext(ctx, fmt.Sprintf("CREATE USER %s IDENTIFIED BY %s", username, password)); err2 != nil {
				log.Fatalf("❌ CREATE USER failed even after truncation: %v", err2)
			}
		} else {
			log.Fatalf("❌ CREATE USER failed: %v", err)
		}
	}

	// optional: allow logon
	if _, err := db.ExecContext(ctx, "GRANT CREATE SESSION TO "+username); err != nil {
		log.Fatalf("❌ grant failed: %v", err)
	}

	fmt.Printf("🎉 Created user: %s (password: %s)\n", username, password)
}

// ---- helpers (snake_case) ----

// oracle unquoted identifiers must start with a letter and contain only letters, digits, _, $, #.
// we uppercase to be conventional for unquoted identifiers.
func sanitize_oracle_identifier(s string) string {
	s = strings.ToUpper(s)
	// replace illegal characters with underscores
	re := regexp.MustCompile(`[^A-Z0-9_\$#]`)
	s = re.ReplaceAllString(s, "_")
	// ensure it starts with a letter
	if len(s) > 0 && (s[0] < 'A' || s[0] > 'Z') {
		s = "U_" + s
	}
	return s
}

func truncate_identifier(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max]
}

func is_identifier_too_long(err error) bool {
	// ORA-00972: identifier is too long
	return strings.Contains(strings.ToUpper(err.Error()), "ORA-00972")
}

// optional utility if you want to detect “name already used by an existing user”
// func is_user_exists(err error) bool {
// 	return strings.Contains(strings.ToUpper(err.Error()), "ORA-01920")
// }
