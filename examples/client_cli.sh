#!/usr/bin/env bash
# Connect to OpenDuck from the DuckDB CLI.
#
# Like MotherDuck's `duckdb md:`, OpenDuck uses `duckdb openduck:` or `duckdb od:`.
#
# Prerequisites:
#   - DuckDB CLI installed (https://duckdb.org/docs/installation)
#   - OpenDuck gateway running
#   - OPENDUCK_TOKEN set in environment
#
# Usage:
#   bash examples/client_cli.sh

set -euo pipefail

export OPENDUCK_TOKEN="${OPENDUCK_TOKEN:-demo}"

echo "=== DuckDB CLI → OpenDuck ==="
echo ""

echo "Option 1: Direct connect to default database"
echo '  $ duckdb od:'
echo '  D SHOW TABLES;'
echo ""

echo "Option 2: Named database"
echo '  $ duckdb openduck:my_database'
echo '  D SELECT * FROM my_table LIMIT 10;'
echo ""

echo "Option 3: Short alias"
echo '  $ duckdb od:analytics'
echo '  D SELECT count(*) FROM events;'
echo ""

echo "Option 4: One-liner query"
echo '  $ duckdb od:analytics -c "SELECT count(*) FROM events;"'
echo ""

echo "Option 5: Custom endpoint (local dev)"
echo '  $ duckdb "openduck:dev_db?endpoint=http://localhost:7878"'
echo ""

echo "Option 6: Hybrid — OpenDuck primary + local file attached"
echo '  $ duckdb openduck:warehouse'
echo "  D ATTACH 'local_data.duckdb' AS local;"
echo '  D SELECT l.category, SUM(events.revenue)'
echo '    FROM local.products l'
echo '    JOIN events ON l.product_id = events.product_id'
echo '    GROUP BY l.category;'
echo ""

echo "Option 7: Read-only snapshot"
echo '  $ duckdb "openduck:my_db?snapshot=abc123"'
echo ""

echo "=== End of CLI examples ==="
