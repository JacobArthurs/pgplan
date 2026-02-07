package plan

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
)

func Resolve(input string, dbConn string, label string) ([]ExplainOutput, error) {
	data, err := readInput(input, label)
	if err != nil {
		return nil, err
	}

	inputType := detectType(data, input)

	switch inputType {
	case "json":
		return ParseJSONPlan(data)
	case "sql":
		trimmed := strings.TrimSpace(string(data))
		if strings.HasPrefix(strings.ToUpper(trimmed), "EXPLAIN") {
			return nil, fmt.Errorf("input should not include EXPLAIN prefix - provide the raw query only")
		}

		if dbConn == "" {
			return nil, fmt.Errorf("SQL input requires a database connection")
		}
		return nil, fmt.Errorf("TODO: Execute not implemented")
	case "text":
		return nil, fmt.Errorf(`text format not supported - use JSON format:

EXPLAIN (ANALYZE, VERBOSE, BUFFERS, FORMAT JSON) <your query>

Then provide the complete JSON output.`)
	default:
		return nil, fmt.Errorf("unable to detect %sinput type: expected JSON plan, SQL query, or .json/.sql file", label)
	}
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
	if runtime.GOOS == "windows" {
		fmt.Printf("Paste %sEXPLAIN (ANALYZE, VERBOSE, BUFFERS, FORMAT JSON) output or SQL query:", label)
		fmt.Println("End with Ctrl+Z then Enter on a new line")
	} else {
		fmt.Printf("Paste %sEXPLAIN (ANALYZE, VERBOSE, BUFFERS, FORMAT JSON) output or SQL query:", label)
		fmt.Println("End with Ctrl+D")
	}

	data, err := io.ReadAll(os.Stdin)
	if err != nil {
		return nil, err
	}

	trimmed := strings.TrimSpace(string(data))

	// Detect common mistakes
	if (strings.HasPrefix(trimmed, "[") ||
		strings.HasPrefix(trimmed, "{")) &&
		!json.Valid(data) {
		return nil, fmt.Errorf("input appears truncated; for large inputs use: pgplan analyze <file>")
	}

	return data, nil
}

func detectType(data []byte, filename string) string {
	// Use file extensions
	if strings.HasSuffix(filename, ".json") {
		return "json"
	}
	if strings.HasSuffix(filename, ".sql") {
		return "sql"
	}
	if strings.HasSuffix(filename, ".txt") {
		return "text"
	}

	// Use file content
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
		strings.HasPrefix(strings.ToUpper(trimmed), "DELETE") {
		return "sql"
	}

	return "unknown"
}
