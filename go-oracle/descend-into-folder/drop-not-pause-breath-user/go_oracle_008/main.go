package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"

	"github.com/PeterCullenBurbery/go_functions_002/v5/date_time_functions"
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

type roles_yaml struct {
	Granted_roles []string `yaml:"granted_roles"`
}

type sys_privs_yaml struct {
	System_privileges []string `yaml:"system_privileges"`
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

func load_roles(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var r roles_yaml
	if err := yaml.Unmarshal(data, &r); err != nil {
		return nil, err
	}
	return r.Granted_roles, nil
}

func load_sys_privs(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var p sys_privs_yaml
	if err := yaml.Unmarshal(data, &p); err != nil {
		return nil, err
	}
	return p.System_privileges, nil
}

func main() {
	const target_container = "pdb_2025_008_004_010_033_019" // change if needed
	const password = "f"
	const roles_yaml_path = "granted-roles.yaml"
	const sys_privs_yaml_path = "system-privileges-without-sysdba-et-al.yaml"

	// 1) generate username
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

	// 4) create user (with fallback on long identifier)
	create_stmt := fmt.Sprintf("CREATE USER %s IDENTIFIED BY %s", username, password)
	if _, err := db.ExecContext(ctx, create_stmt); err != nil {
		if is_identifier_too_long(err) {
			short := truncate_identifier(username, 30)
			fmt.Printf("⚠️ identifier too long; retrying with: %s\n", short)
			username = short
			if _, err2 := db.ExecContext(ctx, fmt.Sprintf("CREATE USER %s IDENTIFIED BY %s", username, password)); err2 != nil {
				log.Fatalf("❌ CREATE USER failed even after truncation: %v", err2)
			}
		} else {
			log.Fatalf("❌ CREATE USER failed: %v", err)
		}
	}
	// minimal logon privilege (can be redundant if CONNECT role is granted later)
	if _, err := db.ExecContext(ctx, "GRANT CREATE SESSION TO "+username+" CONTAINER=CURRENT"); err != nil {
		fmt.Printf("⚠️ grant CREATE SESSION failed (continuing): %v\n", err)
	}
	fmt.Printf("🎉 Created user: %s (password: %s)\n", username, password)

	// 5) load YAML lists
	roles, err := load_roles(roles_yaml_path)
	if err != nil {
		log.Fatalf("❌ could not load roles YAML: %v", err)
	}
	sys_privs, err := load_sys_privs(sys_privs_yaml_path)
	if err != nil {
		log.Fatalf("❌ could not load system privileges YAML: %v", err)
	}

	// 6) grant roles
	ok_count := 0
	fail_count := 0
	for _, role := range roles {
		role = strings.TrimSpace(role)
		if role == "" {
			continue
		}
		stmt := fmt.Sprintf("GRANT %s TO %s CONTAINER=CURRENT", role, username)
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			fmt.Printf("❌ grant role %-35s -> %s (error: %v)\n", role, username, err)
			fail_count++
			continue
		}
		fmt.Printf("✅ grant role %-35s -> %s\n", role, username)
		ok_count++
	}
	fmt.Printf("📊 roles granted OK=%d, failed=%d\n", ok_count, fail_count)

	// 7) grant system privileges
	ok_count = 0
	fail_count = 0
	for _, priv := range sys_privs {
		priv = strings.TrimSpace(priv)
		if priv == "" {
			continue
		}
		stmt := fmt.Sprintf("GRANT %s TO %s CONTAINER=CURRENT", priv, username)
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			fmt.Printf("❌ grant sys priv %-35s -> %s (error: %v)\n", priv, username, err)
			fail_count++
			continue
		}
		fmt.Printf("✅ grant sys priv %-35s -> %s\n", priv, username)
		ok_count++
	}
	fmt.Printf("📊 system privileges granted OK=%d, failed=%d\n", ok_count, fail_count)

	if err := create_java_source(ctx, db, username); err != nil {
		log.Fatalf("❌ %v", err)
	}

	// program exits
}

// ---- helpers (snake_case) ----

func sanitize_oracle_identifier(s string) string {
	s = strings.ToUpper(s)
	re := regexp.MustCompile(`[^A-Z0-9_\$#]`)
	s = re.ReplaceAllString(s, "_")
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
	return strings.Contains(strings.ToUpper(err.Error()), "ORA-00972")
}

func create_java_source(ctx context.Context, db *sql.DB, owner string) error {
	// Compile the Java source in the target user's schema
	if _, err := db.ExecContext(ctx, "ALTER SESSION SET CURRENT_SCHEMA = "+owner); err != nil {
		return fmt.Errorf("set current_schema failed: %w", err)
	}

	// Unquoted object name -> stored as uppercase GET_LOWER_CASE_VALUE
	ddl := `CREATE OR REPLACE AND COMPILE JAVA SOURCE NAMED get_lower_case_value AS
public class get_lower_case_value {
    /**
     * Return the input string in lowercase, or null if input is null.
     */
    public static String get_lower_case_value(String s) {
        if (s == null) return null;
        return s.toLowerCase();
    }
}`

	if _, err := db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("compile Java source failed: %w", err)
	}

	// Verify compile status (we're in CURRENT_SCHEMA, so USER_OBJECTS is simplest)
	var status string
	err := db.QueryRowContext(ctx, `
		SELECT status
		FROM   user_objects
		WHERE  object_type = 'JAVA SOURCE'
		AND    object_name = 'GET_LOWER_CASE_VALUE'`,
	).Scan(&status)
	if err != nil {
		return fmt.Errorf("verification query failed: %w", err)
	}
	fmt.Printf("🧩 Java source get_lower_case_value status: %s\n", status)

	// If INVALID, print compiler errors to help diagnose
	if status == "INVALID" {
		rows, err := db.QueryContext(ctx, `
			SELECT line, position, text
			FROM   user_errors
			WHERE  type = 'JAVA SOURCE'
			AND    name = 'GET_LOWER_CASE_VALUE'
			ORDER  BY sequence`)
		if err != nil {
			return fmt.Errorf("failed to fetch compile errors: %w", err)
		}
		defer rows.Close()

		had := false
		for rows.Next() {
			var line, pos int
			var text string
			if err := rows.Scan(&line, &pos, &text); err != nil {
				return err
			}
			fmt.Printf("❌ [%d:%d] %s\n", line, pos, text)
			had = true
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if had {
			return fmt.Errorf("java source GET_LOWER_CASE_VALUE is INVALID")
		}
	}
	return nil
}