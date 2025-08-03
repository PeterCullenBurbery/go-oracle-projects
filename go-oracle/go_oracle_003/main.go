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

func normalizeForCompare(p string) string {
	// Normalize for case-insensitive compare on Windows and ensure trailing backslash
	p = strings.ReplaceAll(p, "/", `\`)
	if !strings.HasSuffix(p, `\`) {
		p += `\`
	}
	return strings.ToUpper(p)
}

func queryOne(db *sql.DB, ctx context.Context, q string, dest *string) (bool, error) {
	row := db.QueryRowContext(ctx, q)
	err := row.Scan(dest)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
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

	// --- Double method: verify expected vs actual PDBSEED first (no fallback) ---

	rootQ := `
SELECT DISTINCT
       SUBSTR(name, 1, REGEXP_INSTR(name, 'SYSTEM01\.DBF', 1, 1, 0, 'i') - 1)
FROM   v$datafile
WHERE  REGEXP_LIKE(name, 'SYSTEM01\.DBF', 'i')
  AND  NOT REGEXP_LIKE(name, '[\\/]{1}PDB[^\\/]*', 'i')
`
	seedQ := `
SELECT DISTINCT
       SUBSTR(name, 1, REGEXP_INSTR(name, 'SYSTEM01\.DBF', 1, 1, 0, 'i') - 1)
FROM   v$datafile
WHERE  REGEXP_LIKE(name, '[\\/]{1}PDBSEED[\\/]{1}SYSTEM01\.DBF', 'i')
`

	fmt.Println("▶ Running root query (CDB$ROOT) against V$DATAFILE:")
	fmt.Println(rootQ)

	var rootDir string
	ok, err := queryOne(db, ctx, rootQ, &rootDir)
	if err != nil {
		log.Fatalf("❌ Failed to fetch CDB$ROOT datafile directory: %v", err)
	}
	if !ok {
		log.Fatalf("❌ No rows from root query; cannot determine CDB$ROOT datafile directory.")
	}
	rootDir = normalizeWindowsDir(rootDir)
	fmt.Println("✓ Root query returned:\n  ", rootDir)

	// Build expected seed from root
	expectedSeed := normalizeWindowsDir(rootDir + "PDBSEED\\")
	expectedSeedNorm := normalizeForCompare(expectedSeed)

	fmt.Println("\n▶ Running PDB$SEED query against V$DATAFILE:")
	fmt.Println(seedQ)

	var actualSeed string
	found, err := queryOne(db, ctx, seedQ, &actualSeed)
	if err != nil {
		log.Fatalf("❌ Failed while checking PDB$SEED in V$DATAFILE: %v", err)
	}
	if !found {
		log.Fatalf("❌ No rows from PDB$SEED query in V$DATAFILE.")
	}
	actualSeedNorm := normalizeForCompare(actualSeed)

	fmt.Println("✓ PDB$SEED query returned:\n  ", actualSeed)
	fmt.Println("\n▶ Comparing normalized paths:")
	fmt.Println("  Expected:", expectedSeedNorm)
	fmt.Println("  Actual:  ", actualSeedNorm)

	if expectedSeedNorm != actualSeedNorm {
		fmt.Println("❌ Error: Mismatch between expected and actual PDBSEED path.")
		fmt.Println("🔎 Expected:", expectedSeedNorm)
		fmt.Println("🔎 Actual:  ", actualSeedNorm)
		os.Exit(1)
	}

	fmt.Println("✅ Match: expected PDBSEED path equals actual PDBSEED path.")

	// --- Create the PDB only after successful verification ---

	// Generate PDB name
	pdbName, err := date_time_functions.Generate_pdb_name_from_timestamp()
	if err != nil {
		log.Fatalf("❌ Failed to generate PDB name: %v", err)
	}

	// Destination dir for the new PDB
	destDir := normalizeWindowsDir(rootDir + pdbName + `\`)

	// Build CREATE statement
	createSQL := fmt.Sprintf(
		"CREATE PLUGGABLE DATABASE %s ADMIN USER %s IDENTIFIED BY %s FILE_NAME_CONVERT = ('%s', '%s')",
		pdbName,
		adminUser,
		adminPassword,
		strings.ReplaceAll(expectedSeed, `'`, `''`),
		strings.ReplaceAll(destDir, `'`, `''`),
	)

	fmt.Println("\n▶ About to execute:")
	fmt.Println("  ", createSQL)

	// Execute CREATE
	if _, err := db.ExecContext(ctx, createSQL); err != nil {
		log.Fatalf("❌ CREATE PLUGGABLE DATABASE failed: %v", err)
	}

	fmt.Println("\n✅ CREATE PLUGGABLE DATABASE executed successfully.")
	fmt.Println("   PDB Name:  ", pdbName)
	fmt.Println("   Seed From: ", expectedSeed)
	fmt.Println("   Files To:  ", destDir)
}