package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

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

func normalizeWindowsDir(p string) string {
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
	cfg, err := load_config("sysdba.yaml")
	if err != nil {
		log.Fatalf("❌ Failed to load config: %v", err)
	}

	oracle := cfg.Oracle_connection
	dsn := fmt.Sprintf(`user="%s" password="%s" connectString="%s:%d/%s" adminRole=SYSDBA`,
		oracle.Username, oracle.Password, oracle.Host, oracle.Port, oracle.Service_name)

	db, err := sql.Open("godror", dsn)
	if err != nil {
		log.Fatalf("❌ Failed to open DB connection: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Queries (show them so you can see exactly what ran)
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

	// 1) Root dir from V$DATAFILE
	fmt.Println("▶ Running root query (CDB$ROOT) against V$DATAFILE:")
	fmt.Println(rootQ)
	var rootDir string
	ok, err := queryOne(db, ctx, rootQ, &rootDir)
	if err != nil {
		log.Fatalf("❌ Failed to fetch root container path: %v", err)
	}
	if !ok {
		log.Fatalf("❌ No rows from root query; cannot determine CDB$ROOT datafile directory.")
	}
	fmt.Println("✓ Root query returned:\n  ", rootDir)

	// Build expected PDBSEED path
	expectedSeed := normalizeWindowsDir(rootDir) + "PDBSEED\\"

	// 2) PDB$SEED dir from V$DATAFILE
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
	fmt.Println("✓ PDB$SEED query returned:\n  ", actualSeed)

	// 3) Compare (print success if they match)
	expectedSeedNorm := normalizeWindowsDir(expectedSeed)
	actualSeedNorm := normalizeWindowsDir(actualSeed)

	fmt.Println("\n▶ Comparing normalized paths:")
	fmt.Println("  Expected:", expectedSeedNorm)
	fmt.Println("  Actual:  ", actualSeedNorm)

	if expectedSeedNorm == actualSeedNorm {
		fmt.Println("✅ Match: expected PDBSEED path equals actual PDBSEED path.")
	} else {
		fmt.Println("❌ Error: Mismatch between expected and actual PDBSEED path.")
		fmt.Println("🔎 Expected:", expectedSeedNorm)
		fmt.Println("🔎 Actual:  ", actualSeedNorm)
		// Uncomment to signal failure to callers/CI:
		// os.Exit(1)
	}
}