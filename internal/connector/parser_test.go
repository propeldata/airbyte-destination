package connector

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/propeldata/airbyte-destination/internal/airbyte"
)

func TestUnmarshal(t *testing.T) {
	c := require.New(t)

	var configuredCatalog airbyte.ConfiguredCatalog
	err := UnmarshalFromPath("./test_files/parser_sample.json", &configuredCatalog)
	c.NoError(err)

	bytes, err := json.Marshal(configuredCatalog)
	c.NoError(err)
	c.Contains(string(bytes), `"repository":{"title":"","description":"","type":"string"`)
	c.Contains(string(bytes), `"protection_url":{"title":"","description":"","type":["null","string"]}`)
}
