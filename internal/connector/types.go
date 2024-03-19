package connector

import (
	"fmt"

	"github.com/propeldata/go-client/models"

	"github.com/propeldata/airbyte-destination/internal/airbyte"
)

func ConvertAirbyteTypeToPropelType(airbyteProperty airbyte.PropertyType) (models.PropelType, error) {
	if airbyteProperty.TypeSet == nil {
		// if no general type is specified, default to string
		return models.StringPropelType, nil
	}

	types := removeNullType(airbyteProperty.TypeSet.Types)
	if len(types) == 0 {
		// if no general type is specified, default to string
		return models.StringPropelType, nil
	}

	if len(types) > 1 {
		// if field may have different types, default to string
		return models.StringPropelType, nil
	}

	switch types[0] {
	case airbyte.String:
		switch airbyteProperty.Format {
		case airbyte.Date:
			return models.DatePropelType, nil
		case airbyte.DateTime:
			return models.TimestampPropelType, nil
		case airbyte.Time:
			return models.StringPropelType, nil
		}
		return models.StringPropelType, nil
	case airbyte.Boolean:
		return models.BooleanPropelType, nil
	case airbyte.Number:
		return models.DoublePropelType, nil
	case airbyte.Integer:
		return models.Int64PropelType, nil
	case airbyte.Object, airbyte.Array:
		return models.JsonPropelType, nil
	default:
		return models.PropelType{}, fmt.Errorf("airbyte type %s:%s:%s not supported", types[0], airbyteProperty.Format, airbyteProperty.AirbyteType)
	}
}

func removeNullType(input []airbyte.PropType) []airbyte.PropType {
	result := make([]airbyte.PropType, 0, len(input))

	for _, s := range input {
		if s != airbyte.Null {
			result = append(result, s)
		}
	}

	return result
}
