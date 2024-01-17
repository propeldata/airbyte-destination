package airbyte

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
	Type   StateType      `json:"state_type"`
	Data   any            `json:"data"`
	Stream map[string]any `json:"stream"`
	Global map[string]any `json:"global"`
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

type PropertyType struct {
	Type        PropType        `json:"type,omitempty"`
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
