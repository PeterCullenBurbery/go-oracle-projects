package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/goccy/go-yaml"
	"github.com/olekukonko/tablewriter"
	"github.com/olekukonko/tablewriter/tw"
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

func human_bytes(n int64) string {
	const KB = 1024
	const MB = 1024 * KB
	const GB = 1024 * MB
	switch {
	case n >= GB:
		return fmt.Sprintf("%.2f GB", float64(n)/float64(GB))
	case n >= MB:
		return fmt.Sprintf("%.2f MB", float64(n)/float64(MB))
	case n >= KB:
		return fmt.Sprintf("%.2f KB", float64(n)/float64(KB))
	default:
		return fmt.Sprintf("%d B", n)
	}
}

func main() {
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

	const q = `
SELECT
  df.FILE#   AS file_no,
  df.NAME    AS file_name,
  df.BYTES   AS bytes,
  df.STATUS  AS status
FROM v$datafile df
ORDER BY df.FILE#
`
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		log.Fatalf("❌ Query failed: %v", err)
	}
	defer rows.Close()

	table := tablewriter.NewWriter(os.Stdout)
	table.Options(
		tablewriter.WithHeader([]string{"FILE_NO", "SIZE", "STATUS", "FILE_NAME"}), // v1: set header via option
		tablewriter.WithHeaderAlignment(tw.AlignCenter),                            // v1: header align
		tablewriter.WithBorders(tw.Border{Left: tw.On, Right: tw.On, Top: tw.On, Bottom: tw.On}), // outer border
	)

	for rows.Next() {
		var fileNo int64
		var fileName string
		var bytes int64
		var status string

		if err := rows.Scan(&fileNo, &fileName, &bytes, &status); err != nil {
			log.Fatalf("❌ Row scan failed: %v", err)
		}
		table.Append(
			fmt.Sprintf("%d", fileNo),
			human_bytes(bytes),
			status,
			fileName,
		)
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("❌ Rows error: %v", err)
	}

	if err := table.Render(); err != nil {
		log.Fatalf("❌ Render failed: %v", err)
	}
}