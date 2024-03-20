package connector

import (
	"testing"

	"github.com/propeldata/go-client/models"
	"github.com/stretchr/testify/assert"

	"github.com/propeldata/airbyte-destination/internal/airbyte"
)

func TestConvertAirbyteTypeToPropelType(t *testing.T) {
	tests := []struct {
		name               string
		propTypes          []airbyte.PropType
		format             airbyte.FormatType
		expectedPropelType models.PropelType
		expectedError      string
	}{
		{
			name:               "No types",
			propTypes:          []airbyte.PropType{},
			expectedPropelType: models.StringPropelType,
			expectedError:      "",
		},
		{
			name:               "Multiple types",
			propTypes:          []airbyte.PropType{airbyte.Null, airbyte.Object, airbyte.Integer},
			expectedPropelType: models.StringPropelType,
			expectedError:      "",
		},
		{
			name:               "Date type",
			propTypes:          []airbyte.PropType{airbyte.Null, airbyte.String},
			format:             airbyte.DateTime,
			expectedPropelType: models.TimestampPropelType,
			expectedError:      "",
		},
		{
			name:               "Single non-null type",
			propTypes:          []airbyte.PropType{airbyte.Null, airbyte.Boolean},
			expectedPropelType: models.BooleanPropelType,
			expectedError:      "",
		},
		{
			name:               "Unexpected Airbyte type",
			propTypes:          []airbyte.PropType{airbyte.PropType("unexpected")},
			expectedPropelType: models.PropelType{},
			expectedError:      "airbyte type unexpected:: not supported",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			a := assert.New(st)

			propelType, err := ConvertAirbyteTypeToPropelType(airbyte.PropertyType{
				TypeSet: &airbyte.PropTypes{Types: tt.propTypes},
				Format:  tt.format,
			})
			a.Equal(tt.expectedPropelType, propelType)

			if tt.expectedError != "" {
				a.Equal(tt.expectedError, err.Error())
			} else {
				a.NoError(err)
			}
		})
	}
}
