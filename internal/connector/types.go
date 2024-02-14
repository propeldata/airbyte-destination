package connector

import (
	"fmt"

	"github.com/propeldata/go-client/models"

	"github.com/propeldata/airbyte-destination/internal/airbyte"
)

func ConvertAirbyteTypeToPropelType(airbyteProperty airbyte.PropertyType) (models.PropelType, error) {
	var propelType models.PropelType

	if len(airbyteProperty.TypeSet.Types) > 2 {
		return models.StringPropelType, nil
	}

	for _, aType := range airbyteProperty.TypeSet.Types {
		switch aType {
		case airbyte.Null:
			continue
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
			return models.PropelType{}, fmt.Errorf("airbyte type %v:%s:%s not supported", aType, airbyteProperty.Format, airbyteProperty.AirbyteType)
		}
	}

	return propelType, nil
}
