package connector

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/propeldata/airbyte-destination/internal/airbyte"
)

func TestUnmarshal(t *testing.T) {
	c := require.New(t)
	catalogPath := "./test_files/parser_sample.json"

	var configuredCatalog airbyte.ConfiguredCatalog
	err := UnmarshalFromPath(catalogPath, &configuredCatalog)
	c.NoError(err)
}
