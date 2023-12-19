package destination

import (
	"fmt"

	"github.com/bitstrapped/airbyte"
	"github.com/propeldata/fivetran-destination/pkg/client"
)

// TODO: Missing to handle the oneOf
func ConvertAirbyteTypeToPropelType(airbytePropery airbyte.PropertyType) (client.PropelType, error) {
	var propelType client.PropelType

	switch airbytePropery.Type {
	case airbyte.String:
		switch airbytePropery.Format {
		case airbyte.Date:
			propelType = client.DatePropelType
		case airbyte.DateTime:
			switch airbytePropery.AirbyteType {
			case airbyte.TimestampWOTZ:
				propelType = client.StringPropelType
			default:
				propelType = client.TimestampPropelType
			}
		case airbyte.Time:
			propelType = client.StringPropelType
		}

		propelType = client.StringPropelType
	case airbyte.Boolean:
		propelType = client.BooleanPropelType
	case airbyte.Number:
		propelType = client.DoublePropelType
	case airbyte.Integer:
		propelType = client.Int64PropelType
	case airbyte.Object, airbyte.Array, airbyte.Null:
		propelType = client.JsonPropelType
	default:
		return client.PropelType{}, fmt.Errorf("Airbyte type %s:%s:%s not supported", airbytePropery.Type, airbytePropery.Format, airbytePropery.AirbyteType)
	}

	return propelType, nil
}
