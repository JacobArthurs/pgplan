package plan

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
)

func Resolve(input string, dbConn string, label string) (ExplainOutput, error) {
	data, err := readInput(input, label)
	if err != nil {
		return ExplainOutput{}, err
	}

	inputType := detectType(data, input)

	var plans []ExplainOutput

	switch inputType {
	case "json":
		plans, err = ParseJSONPlan(data)
	case "sql":
		trimmed := strings.TrimSpace(string(data))
		if strings.HasPrefix(strings.ToUpper(trimmed), "EXPLAIN") {
			return ExplainOutput{}, fmt.Errorf("input should not include EXPLAIN prefix - provide the raw query only")
		}

		if dbConn == "" {
			return ExplainOutput{}, fmt.Errorf("SQL input requires a database connection")
		}
		plans, err = Execute(dbConn, string(data))
	case "text":
		return ExplainOutput{}, fmt.Errorf(`text format not supported - use JSON format:

EXPLAIN (ANALYZE, VERBOSE, BUFFERS, FORMAT JSON) <your query>

Then provide the complete JSON output.`)
	default:
		return ExplainOutput{}, fmt.Errorf("unable to detect %sinput type: expected JSON plan, SQL query, or .json/.sql file", label)
	}

	if err != nil {
		return ExplainOutput{}, err
	}
	if len(plans) == 0 {
		return ExplainOutput{}, fmt.Errorf("no query plan found in %sinput", label)
	}
	return plans[0], nil
}

func readInput(input string, label string) ([]byte, error) {
	switch input {
	case "":
		return readInteractive(label)
	case "-":
		return io.ReadAll(os.Stdin)
	default:
		return os.ReadFile(input)
	}
}

func readInteractive(label string) ([]byte, error) {
	fmt.Printf("Paste %sEXPLAIN (ANALYZE, VERBOSE, BUFFERS, FORMAT JSON) output or SQL query", label)
	if runtime.GOOS == "windows" {
		fmt.Print(" (Ctrl+Z, Enter to submit)\n")
	} else {
		fmt.Print(" (Ctrl+D to submit)\n")
	}

	data, err := io.ReadAll(os.Stdin)
	if err != nil {
		return nil, err
	}

	trimmed := strings.TrimSpace(string(data))

	if (strings.HasPrefix(trimmed, "[") ||
		strings.HasPrefix(trimmed, "{")) &&
		!json.Valid(data) {
		return nil, fmt.Errorf("input appears truncated; for large inputs use: pgplan analyze <file>")
	}

	return data, nil
}

func detectType(data []byte, filename string) string {
	if strings.HasSuffix(filename, ".json") {
		return "json"
	}
	if strings.HasSuffix(filename, ".sql") {
		return "sql"
	}
	if strings.HasSuffix(filename, ".txt") {
		return "text"
	}

	trimmed := strings.TrimSpace(string(data))

	if strings.HasPrefix(trimmed, "[") || strings.HasPrefix(trimmed, "{") {
		return "json"
	}

	if strings.Contains(trimmed, "(cost=") {
		return "text"
	}

	if strings.HasPrefix(strings.ToUpper(trimmed), "SELECT") ||
		strings.HasPrefix(strings.ToUpper(trimmed), "WITH") ||
		strings.HasPrefix(strings.ToUpper(trimmed), "INSERT") ||
		strings.HasPrefix(strings.ToUpper(trimmed), "UPDATE") ||
		strings.HasPrefix(strings.ToUpper(trimmed), "DELETE") ||
		strings.HasPrefix(strings.ToUpper(trimmed), "EXPLAIN") {
		return "sql"
	}

	return "unknown"
}
