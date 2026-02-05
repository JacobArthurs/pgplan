# pgplan design

## Project Goals

- Compare PostgreSQL EXPLAIN plans with semantic understanding
- Provide CLI-native analysis without browser dependency  
- Support multiple input formats (SQL, JSON)
- Offer actionable optimization recommendations

## Commands

```text
pgplan compare [file1] [file2] [OPTIONS]   Compare two query plans
pgplan analyze [file] [OPTIONS]            Analyze a single query plan
pgplan init                                Create ~/.config/pgplan/config.yml with example template
pgplan --help                              Show help
pgplan --version                           Show version
```

## pgplan compare [file1] [file2] [OPTIONS]

### Compare Files

- `file1` and `file2` are optional
- Can be SQL files, or JSON files (EXPLAIN output)
- Either file (but not both) can be `-` to read from stdin
- Files don't need to be the same type
- If no files provided, enters interactive mode

### Compare Options

| Flag                | Description                             |
| ------------------- | --------------------------------------- |
| `--db <connection>` | PostgreSQL connection string            |
| `--profile <name>`  | Use named profile from config           |
| `--format <type>`   | Output format: `text` (default), `json` |

### Compare Connection Resolution (only needed for SQL input)

1. If `--db` flag provided, use it
2. Else if `--profile` flag provided, load from config.yml
3. Else if config.yml exists with `default_profile`, use default
4. Else, error with helpful message

### Compare Behavior

Process each input independently:

- SQL: requires DB connection, wraps query in `EXPLAIN (ANALYZE, VERBOSE, BUFFERS, FORMAT JSON)` and captures output
- JSON: parse directly

Then compare the two resulting plans.

### Compare Examples

```bash
# Interactive mode (no files)
pgplan compare

# From files (any combination)
pgplan compare old.sql new.sql --db postgres://localhost/mydb
pgplan compare prod-plan.json new-query.sql --profile dev

# Read one plan from stdin
psql -c "EXPLAIN (ANALYZE, VERBOSE, BUFFERS, FORMAT JSON) SELECT ..." | pgplan compare - new.sql --db postgres://localhost/mydb
pgplan compare old-plan.json - < new-plan.json

# Using default profile
pgplan compare old.sql new.sql
```

## pgplan analyze [file] [OPTIONS]

### Analyze File

- `file` is optional
- Can be a SQL file, or JSON file (EXPLAIN output)
- Use `-` for stdin
- If no file provided, enters interactive mode

### Analyze Options

| Flag                | Description                             |
| ------------------- | --------------------------------------- |
| `--db <connection>` | PostgreSQL connection string            |
| `--profile <name>`  | Use named profile from config           |
| `--format <type>`   | Output format: `text` (default), `json` |

### Analyze Connection Resolution (only needed for SQL input)

Same as `pgplan compare`.

### Analyze Behavior

Process input:

- SQL: requires DB connection, wraps query in `EXPLAIN (ANALYZE, VERBOSE, BUFFERS, FORMAT JSON)` and captures output
- JSON: parse directly

Then analyze the plan and provide insights:

- Execution summary (time, cost, rows)
- Detected issues (seq scans, high filter ratios, missing indexes)
- Node-by-node breakdown
- Recommendations for optimization

### Analyze Interactive Mode

If no file provided:

```text
pgplan analyze

→ Paste or type query or query plan (Ctrl+D or Ctrl+Z when done):
[user pastes content]
^D

→ Auto-detect input type
→ If SQL, resolve DB connection
→ Analyze plan
```

### Analyze Examples

```bash
# Interactive mode
pgplan analyze

# Analyze SQL file
pgplan analyze slow-query.sql --db postgres://localhost/mydb

# Analyze saved EXPLAIN output
pgplan analyze prod-slow-plan.json

# Analyze from stdin
pgplan analyze -

# Pipe from psql
psql -c "EXPLAIN (ANALYZE, VERBOSE, BUFFERS, FORMAT JSON) SELECT ..." | pgplan analyze -

# Using profile
pgplan analyze query.sql --profile prod

# JSON output for scripting
pgplan analyze query.sql --db postgres://localhost/mydb --format json
```

## Config File (optional)

Location: `~/.config/pgplan/config.yml`

```yaml
profiles:
  dev:
    host: localhost
    database: myapp_dev
    user: postgres
    password: dev_pass

  prod:
    host: prod.example.com
    database: myapp_prod
    user: readonly
    password: prod_pass

default_profile: dev
```
