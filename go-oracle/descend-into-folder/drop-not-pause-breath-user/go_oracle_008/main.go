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

const (
	MAX_IDENTIFIER_LEN    = 128 // Oracle 12.2+
	LEGACY_IDENTIFIER_LEN = 30  // pre-12.2 fallback
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
	createStmt := fmt.Sprintf("CREATE USER %s IDENTIFIED BY %s", username, password)
	if _, err := db.ExecContext(ctx, createStmt); err != nil {
		if is_identifier_too_long(err) {
			// First retry at 128
			short := truncate_identifier(username, MAX_IDENTIFIER_LEN)
			if short != username {
				fmt.Printf("⚠️ identifier too long; retrying with: %s\n", short)
				username = short
				if _, err2 := db.ExecContext(ctx, fmt.Sprintf("CREATE USER %s IDENTIFIED BY %s", username, password)); err2 != nil {
					// Optional legacy fallback (30) for pre-12.2 databases
					if is_identifier_too_long(err2) {
						legacy := truncate_identifier(username, LEGACY_IDENTIFIER_LEN)
						if legacy != username {
							fmt.Printf("⚠️ still too long; legacy retry with: %s\n", legacy)
							username = legacy
							if _, err3 := db.ExecContext(ctx, fmt.Sprintf("CREATE USER %s IDENTIFIED BY %s", username, password)); err3 != nil {
								log.Fatalf("❌ CREATE USER failed after legacy fallback: %v", err3)
							}
						} else {
							log.Fatalf("❌ CREATE USER failed after 128 and legacy attempts: %v", err2)
						}
					} else {
						log.Fatalf("❌ CREATE USER failed after 128 attempt: %v", err2)
					}
				}
			} else {
				log.Fatalf("❌ CREATE USER failed (length unchanged): %v", err)
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

	if err := create_plsql_wrapper(ctx, db, username); err != nil {
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
	if _, err := db.ExecContext(ctx, "ALTER SESSION SET CURRENT_SCHEMA = "+owner); err != nil {
		return fmt.Errorf("set current_schema failed: %w", err)
	}

	ddl := `CREATE OR REPLACE AND COMPILE JAVA SOURCE NAMED get_lower_case_value AS
public class get_lower_case_value {
    public static String get_lower_case_value(String s) {
        if (s == null) return null;
        return s.toLowerCase();
    }
}`
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("compile Java source failed: %w", err)
	}

	ownerUpper := strings.ToUpper(owner)

	// Verify using ALL_OBJECTS (not USER_OBJECTS)
	var status string
	q := `
	  SELECT status
	  FROM   all_objects
	  WHERE  owner = :1
	    AND  object_type = 'JAVA SOURCE'
	    AND  UPPER(object_name) = 'GET_LOWER_CASE_VALUE'`
	if err := db.QueryRowContext(ctx, q, ownerUpper).Scan(&status); err != nil {
		return fmt.Errorf("verification query failed: %w", err)
	}
	fmt.Printf("🧩 Java source get_lower_case_value status: %s\n", status)

	// If INVALID, dump compiler errors from ALL_ERRORS
	if status == "INVALID" {
		rows, err := db.QueryContext(ctx, `
		  SELECT line, position, text
		  FROM   all_errors
		  WHERE  owner = :1
		    AND  type  = 'JAVA SOURCE'
		    AND  name  = 'GET_LOWER_CASE_VALUE'
		  ORDER BY sequence`, ownerUpper)
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

func create_plsql_wrapper(ctx context.Context, db *sql.DB, owner string) error {
	// Compile the PL/SQL wrapper in the target schema
	if _, err := db.ExecContext(ctx, "ALTER SESSION SET CURRENT_SCHEMA = "+owner); err != nil {
		return fmt.Errorf("set current_schema failed: %w", err)
	}

	ddl := `
CREATE OR REPLACE FUNCTION get_lower_case_value_pl(p_in VARCHAR2)
  RETURN VARCHAR2 DETERMINISTIC
AS LANGUAGE JAVA
NAME 'get_lower_case_value.get_lower_case_value(java.lang.String) return java.lang.String';` // <-- semicolon added

	if _, err := db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("create wrapper failed: %w", err)
	}

	// Verify creation
	var status string
	if err := db.QueryRowContext(ctx, `
		SELECT status
		FROM   all_objects
		WHERE  owner = :1
		  AND  object_type = 'FUNCTION'
		  AND  object_name = 'GET_LOWER_CASE_VALUE_PL'`,
		strings.ToUpper(owner),
	).Scan(&status); err != nil {
		return fmt.Errorf("verify wrapper failed: %w", err)
	}
	fmt.Printf("🧩 PL/SQL wrapper GET_LOWER_CASE_VALUE_PL status: %s\n", status)

	// If INVALID, show errors
	if status == "INVALID" {
		rows, err := db.QueryContext(ctx, `
			SELECT line, position, text
			FROM   all_errors
			WHERE  owner = :1
			  AND  type  = 'FUNCTION'
			  AND  name  = 'GET_LOWER_CASE_VALUE_PL'
			ORDER BY sequence`, strings.ToUpper(owner))
		if err != nil {
			return fmt.Errorf("failed to fetch wrapper errors: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var line, pos int
			var text string
			if err := rows.Scan(&line, &pos, &text); err != nil {
				return err
			}
			fmt.Printf("❌ [%d:%d] %s\n", line, pos, text)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return fmt.Errorf("wrapper is INVALID")
	}

	// Smoke test
	var out sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT get_lower_case_value_pl('AbC') FROM dual`).Scan(&out); err != nil {
		return fmt.Errorf("wrapper test call failed: %w", err)
	}
	fmt.Printf("🧪 get_lower_case_value_pl('AbC') -> %q\n", out.String)
	return nil
}