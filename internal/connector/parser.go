package destination

import (
	"encoding/json"
	"os"
)

func UnmarshalFromPath(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	return json.Unmarshal(b, v)
}
