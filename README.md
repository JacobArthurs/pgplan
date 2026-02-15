# pgplan

A command-line tool for analyzing and comparing PostgreSQL query execution plans. Get optimization insights and track performance regressions without leaving your terminal.

## Features

- **Plan Analysis** - Run 15+ intelligent rules against a query plan to surface performance issues with actionable fix suggestions
- **Plan Comparison** - Semantically diff two plans side-by-side to understand what changed and whether it got better or worse
- **Flexible Input** - Accept JSON EXPLAIN output, raw SQL files, stdin, or paste plans interactively
- **Connection Profiles** - Save and manage named PostgreSQL connection strings for quick reuse
- **Multiple Output Formats** - Human-readable colored terminal output or structured JSON for tooling integration

## Installation

Not yet published.

## Quick Start

```bash
# Analyze a query plan from a JSON EXPLAIN output
pgplan analyze plan.json

# Analyze by running a SQL file against a database
pgplan analyze query.sql --db postgres://localhost:5432/mydb

# Compare two plans
pgplan compare before.json after.json

# Interactive mode - paste plans or queries directly into the terminal
pgplan analyze
pgplan compare
```

## Commands

### `pgplan analyze [file]`

Analyzes a single query plan and returns optimization findings sorted by severity.

**Arguments:**

| Argument | Description |
| -------- | ----------- |
| `file` | Path to a `.json` (EXPLAIN output) or `.sql` file. Use `-` for stdin. Omit for interactive mode. |

**Flags:**

| Flag | Description |
| ---- | ----------- |
| `-d, --db` | PostgreSQL connection string (required for SQL input) |
| `-p, --profile` | Named connection profile to use |
| `-f, --format` | Output format: `text` (default) or `json` |

**Example:**

```bash
pgplan analyze slow-query.sql --profile prod
```

### `pgplan compare [file1] [file2]`

Compares two query plans and reports on cost, time, row estimate, and buffer differences across every node in the plan tree.

**Arguments:**

| Argument | Description |
| -------- | ----------- |
| `file1` | The "before" plan. `.json`, `.sql`, `-` for stdin, or omit for interactive. |
| `file2` | The "after" plan. Same input options as `file1`. |

**Flags:**

| Flag | Description |
| ---- | ----------- |
| `-d, --db` | PostgreSQL connection string (required for SQL input) |
| `-p, --profile` | Named connection profile to use |
| `-f, --format` | Output format: `text` (default) or `json` |
| `-t, --threshold` | Percent change threshold for significance (default: `5`) |

**Example:**

```bash
pgplan compare before.json after.json --threshold 10
```

### `pgplan profile <subcommand>`

Manages saved PostgreSQL connection profiles stored in `~/.config/pgplan/profiles.yaml`.

| Subcommand | Description |
| ---------- | ----------- |
| `list [--show]` | List saved profiles. Pass `--show` to display connection strings. |
| `add <name> <conn_str>` | Add or update a named profile. |
| `remove <name>` | Remove a profile. |
| `default <name>` | Set a profile as the default. |
| `clear-default` | Clear the default profile. |

**Example:**

```bash
pgplan profile add prod postgres://user:pass@host:5432/mydb
pgplan profile default prod

# Now use it with analyze or compare
pgplan analyze query.sql --profile prod
```

## Analysis Rules

The `analyze` command applies the following rules to identify performance issues. Each finding includes a severity level and an actionable suggestion.

| Severity | Rule | Description |
| -------- | ---- | ----------- |
| Critical | Sort Spill to Disk | Sort operation exceeded `work_mem` and spilled to disk |
| Warning | Hash Spill to Disk | Hash table exceeded `work_mem` |
| Warning | Temp Block I/O | Plan is reading/writing temporary blocks |
| Warning | Seq Scan in Join | Sequential scan used inside a join against a smaller set |
| Warning | Seq Scan with Filter | Standalone sequential scan filtering a large number of rows |
| Warning | Index Scan Filter Inefficiency | Index scan is fetching many rows then discarding most via filter |
| Warning | Bitmap Heap Recheck | Lossy bitmap scan rechecking conditions (bitmap exceeded `work_mem`) |
| Warning | Nested Loop High Loops | Nested loop executing 1,000+ iterations |
| Warning | Correlated Subplan | Subplan re-executing on every outer row |
| Warning | Worker Launch Mismatch | Fewer parallel workers launched than planned |
| Warning | Parallel Overhead | Parallel execution is slower than the serial estimate |
| Warning | Large Join Filter Removal | Join filter is discarding a large percentage of rows |
| Warning | Excessive Materialization | Materialize node looping many times |
| Info | Low Selectivity Index Scan | Index scan is returning most of the table |
| Info | Wide Row Output | Query is selecting more columns than necessary |

## Comparison Output

The `compare` command produces a structured diff of two plans including:

- **Summary** - Overall cost, execution time, and buffer changes with directional indicators
- **Node Details** - Per-node breakdown of metric changes (cost, rows, loops, buffers, filters, indexes)
- **Verdict** - A final assessment such as "faster and cheaper" or "slower but cheaper"

Changes below the significance threshold (default 5%) are filtered out to reduce noise.

## Output Formats

### Text (default)

Colored terminal output with severity-coded findings and directional change indicators. Designed for quick human review.

### JSON

Structured output suitable for piping into other tools, CI systems, or dashboards. Includes all metrics, findings, and comparison deltas.

```bash
pgplan analyze plan.json --format json | jq '.findings[] | select(.severity == "critical")'
```

## Configuration

### Connection Profiles

Profiles are stored in a YAML configuration file at the platform-appropriate config directory:

- **Linux/macOS:** `~/.config/pgplan/profiles.yaml`
- **Windows:** `%APPDATA%\pgplan\profiles.yaml`

```yaml
default: prod
profiles:
  - name: prod
    conn_str: postgres://user:pass@host:5432/production
  - name: dev
    conn_str: postgres://localhost:5432/development
```

Use `--profile <name>` with any command, or set a default to skip the flag entirely. The `--db` and `--profile` flags are mutually exclusive.

## License

This project is licensed under the [MIT License](LICENSE).
