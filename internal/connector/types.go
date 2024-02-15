package connector

import (
	"fmt"

	"github.com/propeldata/go-client/models"

	"github.com/propeldata/airbyte-destination/internal/airbyte"
)

func ConvertAirbyteTypeToPropelType(airbyteProperty airbyte.PropertyType) (models.PropelType, error) {
	var propelType models.PropelType
	if airbyteProperty.TypeSet == nil || len(airbyteProperty.TypeSet.Types) == 0 {
		// if no general type is specified, default to string
		return models.StringPropelType, nil
	}

	types := removeNullType(airbyteProperty.TypeSet.Types)
	if len(types) > 1 {
		// if field may have different types, default to string
		return models.StringPropelType, nil
	}

	for _, aType := range types {
		switch aType {
		case airbyte.String:
			switch airbyteProperty.Format {
			case airbyte.Date:
				propelType = models.DatePropelType
			case airbyte.DateTime:
				switch airbyteProperty.AirbyteType {
				case airbyte.TimestampWOTZ:
					propelType = models.StringPropelType
				default:
					propelType = models.TimestampPropelType
				}
			case airbyte.Time:
				propelType = models.StringPropelType
			}

			propelType = models.StringPropelType
		case airbyte.Boolean:
			propelType = models.BooleanPropelType
		case airbyte.Number:
			propelType = models.DoublePropelType
		case airbyte.Integer:
			propelType = models.Int64PropelType
		case airbyte.Object, airbyte.Array:
			propelType = models.JsonPropelType
		default:
			return models.PropelType{}, fmt.Errorf("airbyte type %s:%s:%s not supported", aType, airbyteProperty.Format, airbyteProperty.AirbyteType)
		}
	}

	return propelType, nil
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
