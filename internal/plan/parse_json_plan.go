package plan

import (
	"encoding/json"
	"fmt"
)

func ParseJSONPlan(data []byte) ([]ExplainOutput, error) {
	var plans []ExplainOutput
	if err := json.Unmarshal(data, &plans); err != nil {
		return nil, fmt.Errorf("invalid EXPLAIN JSON: %w", err)
	}
	if len(plans) == 0 {
		return nil, fmt.Errorf("empty EXPLAIN output")
	}
	return plans, nil
}
