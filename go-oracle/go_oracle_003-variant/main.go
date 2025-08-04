package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/goccy/go-yaml"
	_ "github.com/godror/godror"

	// v5 helpers
	"github.com/PeterCullenBurbery/go_functions_002/v5/oracle_database_system_management_functions"
)

const (
	config_path    = "sysdba.yaml"
	admin_user     = "pdb_admin" // hardcoded as requested
	admin_password = "f"         // hardcoded as requested
	// choose whether to teardown at the end
	do_teardown    = true
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
	// Load config
	cfg, err := load_config(config_path)
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

	// Guard: must be in CDB$ROOT
	if err := oracle_database_system_management_functions.Ensure_connected_to_cdb_root(ctx, db); err != nil {
		log.Fatalf("❌ %v", err)
	}
	log.Println("✓ Connected to CDB$ROOT")

	// Double-check seed location (root\PDBSEED\ vs actual)
	if err := oracle_database_system_management_functions.Verify_pdbseed_directory_matches_expected(ctx, db); err != nil {
		log.Fatalf("❌ PDB$SEED path check failed: %v", err)
	}
	log.Println("✓ PDB$SEED directory matches expected path")

	// Create + open + save state (library generates pdb name internally)
	pdb_name, dest_dir, err := oracle_database_system_management_functions.Create_open_save_state_pdb_from_seed(
		ctx, db, admin_user, admin_password,
	)
	if err != nil {
		log.Fatalf("❌ Failed to create/open/save-state PDB: %v", err)
	}
	log.Println("✅ PDB created & opened:")
	log.Println("   Name: ", pdb_name)
	log.Println("   Files:", dest_dir)

	// Optional: confirm status and saved state (already logged by helper)
	open_mode, err := oracle_database_system_management_functions.Get_pdb_status(ctx, db, pdb_name)
	if err == nil {
		log.Println("🔎 Open mode:", open_mode)
	} else {
		log.Println("ℹ️ Could not read open mode:", err)
	}
	if state, restricted, err := oracle_database_system_management_functions.Get_saved_state_info(ctx, db, pdb_name); err == nil {
		if state != "" {
			log.Printf("💾 Saved state: STATE=%s RESTRICTED=%s\n", state, restricted)
		} else {
			log.Println("ℹ️ No row in DBA_PDB_SAVED_STATES for this PDB (may be normal).")
		}
	} else {
		log.Println("ℹ️ Could not read DBA_PDB_SAVED_STATES:", err)
	}

	// Optionally teardown (close/discard/drop + verify)
	if do_teardown {
		log.Println("▶ Teardown: closing/discarding state/dropping INCLUDING DATAFILES…")
		// kill_sessions=true to be robust; instances_all=false (change if RAC behavior desired)
		if err := oracle_database_system_management_functions.Teardown_drop_pdb(
			ctx, db, pdb_name, /*instances_all*/ false, /*kill_sessions*/ true,
		); err != nil {
			log.Fatalf("❌ Teardown failed: %v", err)
		}
		log.Println("🗑️ Teardown complete and verified.")
	}
}