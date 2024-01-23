package connector

import (
	"fmt"

	"github.com/propeldata/go-client/models"

	"github.com/propeldata/airbyte-destination/internal/airbyte"
)

// TODO: Missing to handle the oneOf
func ConvertAirbyteTypeToPropelType(airbytePropery airbyte.PropertyType) (models.PropelType, error) {
	var propelType models.PropelType

	switch airbytePropery.Type {
	case airbyte.String:
		switch airbytePropery.Format {
		case airbyte.Date:
			propelType = models.DatePropelType
		case airbyte.DateTime:
			switch airbytePropery.AirbyteType {
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
		return models.PropelType{}, fmt.Errorf("Airbyte type %s:%s:%s not supported", airbytePropery.Type, airbytePropery.Format, airbytePropery.AirbyteType)
	}

	return propelType, nil
}
