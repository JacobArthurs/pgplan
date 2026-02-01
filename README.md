# pgplan

> ⚠️ **Early Development** - Not ready for production use

Compare and analyze PostgreSQL EXPLAIN plans from the CLI.

## Why?

Understand query performance regressions and get optimization insights without leaving your terminal.

## Planned Features

- [ ] EXPLAIN plan parsing (JSON, TEXT formats)
- [ ] Intelligent plan comparison with semantic diff
- [ ] Single plan analysis with recommendations
- [ ] Interactive mode (paste plans directly)
- [ ] Connection profiles for multiple databases
- [ ] Web interface

## Installation

Not yet published. Clone and run locally:
```bash
git clone https://github.com/JacobArthurs/pgplan
cd pgplan
```

## Usage (Planned)

```bash
# Compare two query plans
pgplan compare old.sql new.sql --db postgres://localhost/mydb

# Analyze a single query
pgplan analyze slow-query.sql --profile prod

# Interactive mode
pgplan compare
# Paste Plan A, then Plan B
```

## Development

Contributions welcome! This is an early-stage project.

MIT License
