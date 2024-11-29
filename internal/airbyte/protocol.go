package airbyte

import (
	"encoding/json"
	"fmt"
)

// From Airbyte protocol https://github.com/airbytehq/airbyte-protocol/blob/main/protocol-models/src/main/resources/airbyte_protocol/airbyte_protocol.yaml

type messageType string

const (
	MessageTypeRecord           messageType = "RECORD"
	MessageTypeState            messageType = "STATE"
	messageTypeLog              messageType = "LOG"
	messageTypeSpec             messageType = "SPEC"
	messageTypeConnectionStatus messageType = "CONNECTION_STATUS"
	messageTypeLogCatalog       messageType = "CATALOG"
)

type Message struct {
	Type                   messageType             `json:"type"`
	Log                    *LogMessage             `json:"log,omitempty"`
	ConnectorSpecification *ConnectorSpecification `json:"spec,omitempty"`
	ConnectionStatus       *ConnectionStatus       `json:"connectionStatus,omitempty"`
	Catalog                *Catalog                `json:"catalog,omitempty"`
	Record                 *Record                 `json:"record,omitempty"`
	State                  *State                  `json:"state,omitempty"`
}

// LogLevel defines the log levels that can be emitted with airbyte logs
type LogLevel string

const (
	LogLevelFatal LogLevel = "FATAL"
	LogLevelError LogLevel = "ERROR"
	LogLevelWarn  LogLevel = "WARN"
	LogLevelInfo  LogLevel = "INFO"
	LogLevelDebug LogLevel = "DEBUG"
	LogLevelTrace LogLevel = "TRACE"
)

type LogMessage struct {
	Level   LogLevel `json:"level"`
	Message string   `json:"message"`
}

// DestinationSyncMode represents how the connector should interpret your data
type DestinationSyncMode string

var (
	// DestinationSyncModeAppend is used for the connector to know it needs to append data
	DestinationSyncModeAppend DestinationSyncMode = "append"
	// DestinationSyncModeOverwrite is used to indicate the connector should overwrite data
	DestinationSyncModeOverwrite DestinationSyncMode = "overwrite"
	// DestinationSyncModeAppendDedup is used to indicate the connector should deduplicate it on primary key.
	DestinationSyncModeAppendDedup DestinationSyncMode = "append_dedup"
)

// ConnectionSpecification is used to define the settings that are configurable "per" instance of your connector
type ConnectionSpecification struct {
	Title       string `json:"title"`
	Description string `json:"description"`
	Properties
	Type     string   `json:"type"` // should always be "object"
	Required []string `json:"required"`
}

// ConnectorSpecification is used to define the connector wide settings. Every connection using your connector will comply to these settings
type ConnectorSpecification struct {
	DocumentationURL              string                  `json:"documentationUrl,omitempty"`
	ChangeLogURL                  string                  `json:"changeLogUrl"`
	SupportsIncremental           bool                    `json:"supportsIncremental"`
	SupportsNormalization         bool                    `json:"supportsNormalization"`
	SupportsDBT                   bool                    `json:"supportsDBT"`
	SupportedDestinationSyncModes []DestinationSyncMode   `json:"supported_destination_sync_modes"`
	ConnectionSpecification       ConnectionSpecification `json:"connectionSpecification"`
}

type CheckStatus string

const (
	CheckStatusSuccess CheckStatus = "SUCCEEDED"
	CheckStatusFailed  CheckStatus = "FAILED"
)

type ConnectionStatus struct {
	Status  CheckStatus `json:"status"`
	Message string      `json:"message,omitempty"`
}

type StateType string

const (
	StateTypeStream StateType = "STREAM"
	StateTypeGlobal StateType = "GLOBAL"
	StateTypeLegacy StateType = "LEGACY"
)

// State is used to store data between syncs - useful for incremental syncs and state storage
type State struct {
	Type             StateType      `json:"state_type"`
	Data             any            `json:"data,omitempty"`
	Stream           map[string]any `json:"stream,omitempty"`
	Global           map[string]any `json:"global,omitempty"`
	SourceStats      StateStats     `json:"sourceStats,omitempty"`
	DestinationStats StateStats     `json:"destinationStats,omitempty"`
}

// SyncMode defines the modes that your source is able to sync in
type SyncMode string

const (
	// SyncModeFullRefresh means the data will be wiped and fully synced on each run
	SyncModeFullRefresh SyncMode = "full_refresh"
	// SyncModeIncremental is used for incremental syncs
	SyncModeIncremental SyncMode = "incremental"
)

// PropType defines the property types any field can take. See more here:  https://docs.airbyte.com/understanding-airbyte/supported-data-types
type PropType string

const (
	String  PropType = "string"
	Boolean PropType = "boolean"
	Number  PropType = "number"
	Integer PropType = "integer"
	Object  PropType = "object"
	Array   PropType = "array"
	Null    PropType = "null"
)

// AirbytePropType is used to define airbyte specific property types. See more here: https://docs.airbyte.com/understanding-airbyte/supported-data-types
type AirbytePropType string

const (
	TimestampWithTZ    AirbytePropType = "timestamp_with_timezone"
	TimestampWOTZ      AirbytePropType = "timestamp_without_timezone"
	TimeWithTZ         AirbytePropType = "time_with_timezone"
	TimeWOTZ           AirbytePropType = "time_without_timezone"
	AirbyteTypeInteger AirbytePropType = "integer"
	BigInteger         AirbytePropType = "big_integer"
	BigNumber          AirbytePropType = "big_number"
)

// FormatType is used to define data type formats supported by airbyte where needed (usually for strings formatted as dates). See more here: https://docs.airbyte.com/understanding-airbyte/supported-data-types
type FormatType string

const (
	Date     FormatType = "date"
	DateTime FormatType = "date-time"
	Time     FormatType = "time"
)

// PropTypes describes the possible types a field may have.
// It can be a string or an array of strings, so it is always unmarshalled as an array.
type PropTypes struct {
	Types []PropType `json:"types,omitempty"`
}

// UnmarshalJSON converts a single PropType string into an array with just one PropType item.
// This is because a column may have one or multiple types defined as just a string (e.g. "int64") or
// an array of multiple strings (e.g. ["int64", "object", "null"]
func (pt *PropTypes) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err == nil {
		pt.Types = []PropType{PropType(str)}
		return nil
	}

	var arr []string
	if err := json.Unmarshal(data, &arr); err == nil {
		pt.Types = make([]PropType, len(arr))
		for i, propType := range arr {
			pt.Types[i] = PropType(propType)
		}
		return nil
	}

	return fmt.Errorf("cannot unmarshal type %s into string or []string", data)
}

func (pt *PropTypes) MarshalJSON() ([]byte, error) {
	if len(pt.Types) == 1 {
		return json.Marshal(string(pt.Types[0]))
	}

	return json.Marshal(pt.Types)
}

type PropertyType struct {
	TypeSet     *PropTypes      `json:"type,omitempty"`
	AirbyteType AirbytePropType `json:"airbyte_type,omitempty"`
	Format      FormatType      `json:"format,omitempty"`
}

type PropertySpec struct {
	Title        string `json:"title"`
	Description  string `json:"description"`
	PropertyType `json:",omitempty"`
	Examples     []string                `json:"examples,omitempty"`
	Items        map[string]any          `json:"items,omitempty"`
	Properties   map[string]PropertySpec `json:"properties,omitempty"`
	IsSecret     bool                    `json:"airbyte_secret,omitempty"`
}

// Properties defines the property map which is used to define any single "field name" along with its specification
type Properties struct {
	Properties map[string]PropertySpec `json:"properties"`
}

// Stream defines a single "schema" you'd like to sync - think of this as a table, collection, topic, etc. In airbyte terminology these are "streams"
type Stream struct {
	Name                    string     `json:"name"`
	JSONSchema              Properties `json:"json_schema"`
	SupportedSyncModes      []SyncMode `json:"supported_sync_modes,omitempty"`
	SourceDefinedCursor     bool       `json:"source_defined_cursor,omitempty"`
	DefaultCursorField      []string   `json:"default_cursor_field,omitempty"`
	SourceDefinedPrimaryKey [][]string `json:"source_defined_primary_key,omitempty"`
	Namespace               string     `json:"namespace"`
}

// Catalog defines the complete available schema you can sync with a source
// This should not be mistaken with ConfiguredCatalog which is the "selected" schema you want to sync
type Catalog struct {
	Streams []Stream `json:"streams"`
}

// ConfiguredStream defines a single selected stream to sync
type ConfiguredStream struct {
	Stream              Stream              `json:"stream"`
	SyncMode            SyncMode            `json:"sync_mode"`
	CursorField         []string            `json:"cursor_field"`
	DestinationSyncMode DestinationSyncMode `json:"destination_sync_mode"`
	PrimaryKey          [][]string          `json:"primary_key"`
}

// ConfiguredCatalog is the "selected" schema you want to sync
// This should not be mistaken with Catalog which represents the complete available schema to sync
type ConfiguredCatalog struct {
	Streams []ConfiguredStream `json:"streams"`
}

// Record defines a record as per airbyte - a "data point"
type Record struct {
	Namespace string         `json:"namespace"`
	Stream    string         `json:"stream"`
	Data      map[string]any `json:"data"`
	EmittedAt int64          `json:"emitted_at"`
}

// StateStats to emit checkpoints while replicating data
type StateStats struct {
	RecordCount float64 `json:"recordCount"`
}
