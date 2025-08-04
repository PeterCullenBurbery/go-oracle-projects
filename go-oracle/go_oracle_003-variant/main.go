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
	// Normalize separators to backslash and ensure trailing backslash
	p = strings.ReplaceAll(p, "/", `\`)
	if !strings.HasSuffix(p, `\`) {
		p += `\`
	}
	return p
}

func normalizeForCompare(p string) string {
	// Normalize separators and case (Windows is case-insensitive), ensure trailing backslash
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

	// --- Safety check: ensure we are in CDB$ROOT ---
	var conName string
	if err := db.QueryRowContext(ctx, "SELECT SYS_CONTEXT('USERENV','CON_NAME') FROM dual").Scan(&conName); err != nil {
		log.Fatalf("❌ Unable to determine current container: %v", err)
	}
	if !strings.EqualFold(conName, "CDB$ROOT") {
		log.Fatalf("❌ Not connected to CDB$ROOT (current: %s). Connect to root first.", conName)
	}
	fmt.Println("✓ Connected to container:", conName)

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

	// --- Generate PDB name and ensure it does not already exist ---
	pdbName, err := date_time_functions.Generate_pdb_name_from_timestamp()
	if err != nil {
		log.Fatalf("❌ Failed to generate PDB name: %v", err)
	}

	var exists int
	checkSQL := fmt.Sprintf("SELECT COUNT(*) FROM DBA_PDBS WHERE PDB_NAME = UPPER('%s')", pdbName)
	if err := db.QueryRowContext(ctx, checkSQL).Scan(&exists); err != nil {
		log.Fatalf("❌ Failed to check for existing PDB: %v", err)
	}
	if exists > 0 {
		log.Fatalf("❌ PDB %s already exists. Aborting.", pdbName)
	}
	fmt.Println("✓ PDB name available:", pdbName)

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

	// --- Post-create: OPEN READ WRITE, SAVE STATE, verify ---
	post := []string{
		fmt.Sprintf("ALTER PLUGGABLE DATABASE %s OPEN READ WRITE", pdbName),
		fmt.Sprintf("ALTER PLUGGABLE DATABASE %s SAVE STATE", pdbName),
		fmt.Sprintf("SELECT NAME, OPEN_MODE FROM V$PDBS WHERE NAME = UPPER('%s')", pdbName),
	}
	for _, sqlText := range post {
		if strings.HasPrefix(sqlText, "SELECT ") {
			row := db.QueryRowContext(ctx, sqlText)
			var name, openMode string
			if err := row.Scan(&name, &openMode); err == nil {
				fmt.Println("🔎 PDB status:", name, openMode)
			} else {
				fmt.Println("⚠️ Verification query failed:", err)
				os.Exit(1)
			}
			continue
		}
		if _, err := db.ExecContext(ctx, sqlText); err != nil {
			fmt.Println("⚠️ Post-create step failed:", sqlText, "->", err)
			os.Exit(1)
		}
		fmt.Println("✓ Executed:", sqlText)
	}

	// --- Confirm saved state recorded (DBA_PDB_SAVED_STATES uses CON_NAME) ---
	q := fmt.Sprintf(`
    SELECT state, restricted
    FROM   dba_pdb_saved_states
    WHERE  con_name = UPPER('%s')`, pdbName)

	var state, restricted string
	if err := db.QueryRowContext(ctx, q).Scan(&state, &restricted); err != nil {
		// The view may be unavailable in some editions/privilege sets.
		fmt.Println("ℹ️ Could not read DBA_PDB_SAVED_STATES (view may be unavailable):", err)
	} else {
		fmt.Printf("💾 Saved state recorded: STATE=%s, RESTRICTED=%s\n", state, restricted)
	}

	// add:
	fmt.Println("\n▶ Teardown: closing and dropping the new PDB")
	teardown := []string{
		fmt.Sprintf("ALTER PLUGGABLE DATABASE %s CLOSE IMMEDIATE", pdbName),
		fmt.Sprintf("ALTER PLUGGABLE DATABASE %s DISCARD STATE", pdbName),
		fmt.Sprintf("DROP PLUGGABLE DATABASE %s INCLUDING DATAFILES", pdbName),
	}
	for _, sqlText := range teardown {
		if _, err := db.ExecContext(ctx, sqlText); err != nil {
			fmt.Println("⚠️ Teardown step failed:", sqlText, "->", err)
			os.Exit(1)
		}
		fmt.Println("✓ Executed:", sqlText)
	}
	verifyDrop := fmt.Sprintf("SELECT COUNT(*) FROM DBA_PDBS WHERE PDB_NAME = UPPER('%s')", pdbName)
	var remaining int
	if err := db.QueryRowContext(ctx, verifyDrop).Scan(&remaining); err != nil {
		fmt.Println("⚠️ Could not verify PDB drop:", err)
		os.Exit(1)
	}
	if remaining != 0 {
		fmt.Printf("❌ Drop not confirmed: DBA_PDBS still shows %d row(s) for %s\n", remaining, pdbName)
		os.Exit(1)
	}
	fmt.Println("🗑️ Drop confirmed: PDB removed from DBA_PDBS.")
}