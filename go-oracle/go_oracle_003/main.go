package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/PeterCullenBurbery/go_functions_002/v5/date_time_functions"
	"github.com/goccy/go-yaml"
	_ "github.com/godror/godror"
)

const (
	configPath    = "sysdba.yaml"
	adminUser     = "pdb_admin" // hardcoded as requested
	adminPassword = "f"         // hardcoded as requested
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

func loadConfig(path string) (*config, error) {
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

func normalizeWindowsDir(p string) string {
	p = strings.ReplaceAll(p, "/", `\`)
	if !strings.HasSuffix(p, `\`) {
		p += `\`
	}
	return p
}

func main() {
	// Load config
	cfg, err := loadConfig(configPath)
	if err != nil {
		log.Fatalf("❌ Failed to load config: %v", err)
	}
	oracle := cfg.Oracle_connection

	// Connect as SYSDBA
	dsn := fmt.Sprintf(`user="%s" password="%s" connectString="%s:%d/%s" adminRole=SYSDBA`,
		oracle.Username, oracle.Password, oracle.Host, oracle.Port, oracle.Service_name)
	db, err := sql.Open("godror", dsn)
	if err != nil {
		log.Fatalf("❌ Failed to open DB connection: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Root directory from V$DATAFILE (no fallback)
	rootQ := `
SELECT DISTINCT
       SUBSTR(name, 1, REGEXP_INSTR(name, 'SYSTEM01\.DBF', 1, 1, 0, 'i') - 1)
FROM   v$datafile
WHERE  REGEXP_LIKE(name, 'SYSTEM01\.DBF', 'i')
  AND  NOT REGEXP_LIKE(name, '[\\/]{1}PDB[^\\/]*', 'i')
`
	fmt.Println("▶ Running root query (CDB$ROOT) against V$DATAFILE:")
	fmt.Println(rootQ)

	var rootDir string
	if err := db.QueryRowContext(ctx, rootQ).Scan(&rootDir); err != nil {
		log.Fatalf("❌ Failed to fetch CDB$ROOT datafile directory: %v", err)
	}
	rootDir = normalizeWindowsDir(rootDir)
	fmt.Println("✓ Root query returned:\n  ", rootDir)

	// Build seed dir from root
	seedDir := normalizeWindowsDir(rootDir + "PDBSEED\\")
	fmt.Println("\n📁 Derived PDBSEED directory:")
	fmt.Println("  ", seedDir)

	// Generate PDB name from your helper
	pdbName, err := date_time_functions.Generate_pdb_name_from_timestamp()
	if err != nil {
		log.Fatalf("❌ Failed to generate PDB name: %v", err)
	}

	// Destination dir for the new PDB
	destDir := normalizeWindowsDir(rootDir + pdbName + `\`)
	fmt.Println("\n📁 Destination directory for new PDB:")
	fmt.Println("  ", destDir)

	// Build and execute CREATE PLUGGABLE DATABASE
	createSQL := fmt.Sprintf(
		"CREATE PLUGGABLE DATABASE %s ADMIN USER %s IDENTIFIED BY %s FILE_NAME_CONVERT = ('%s', '%s')",
		pdbName,
		adminUser,
		adminPassword,
		strings.ReplaceAll(seedDir, `'`, `''`),
		strings.ReplaceAll(destDir, `'`, `''`),
	)

	fmt.Println("\n▶ About to execute:")
	fmt.Println("  ", createSQL)

	if _, err := db.ExecContext(ctx, createSQL); err != nil {
		log.Fatalf("❌ CREATE PLUGGABLE DATABASE failed: %v", err)
	}

	fmt.Println("\n✅ CREATE PLUGGABLE DATABASE executed successfully.")
	fmt.Println("   PDB Name:  ", pdbName)
	fmt.Println("   Seed From: ", seedDir)
	fmt.Println("   Files To:  ", destDir)
}