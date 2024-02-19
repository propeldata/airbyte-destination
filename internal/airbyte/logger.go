package airbyte

import (
	"encoding/json"
	"io"
	"time"
)

const MaxBatchSize = 10_000

type Logger interface {
	Log(level LogLevel, message string)
	Spec(spec *ConnectorSpecification)
	ConnectionStatus(status *ConnectionStatus)
	Catalog(catalog *Catalog)
	Record(namespace string, stream string, data map[string]any)
	State(syncState *State)
	Flush()
}

type logger struct {
	recordEncoder *json.Encoder
	writer        io.Writer
	records       []Message
}

var _ Logger = &logger{}

func NewLogger(w io.Writer) Logger {
	l := logger{}
	l.writer = w
	l.recordEncoder = json.NewEncoder(w)
	l.records = make([]Message, 0, MaxBatchSize)
	return &l
}

func (l *logger) Log(level LogLevel, message string) {
	l.recordEncoder.Encode(Message{
		Type: messageTypeLog,
		Log: &LogMessage{
			Level:   level,
			Message: message,
		},
	})
}

func (l *logger) Spec(spec *ConnectorSpecification) {
	l.recordEncoder.Encode(Message{
		Type:                   messageTypeSpec,
		ConnectorSpecification: spec,
	})
}

func (l *logger) ConnectionStatus(status *ConnectionStatus) {
	l.recordEncoder.Encode(Message{
		Type:             messageTypeConnectionStatus,
		ConnectionStatus: status,
	})
}

func (l *logger) Catalog(catalog *Catalog) {
	l.recordEncoder.Encode(Message{
		Type:    messageTypeLogCatalog,
		Catalog: catalog,
	})
}

func (l *logger) Record(namespace string, stream string, data map[string]any) {
	now := time.Now()

	msg := Message{
		Type: MessageTypeRecord,
		Record: &Record{
			Namespace: namespace,
			Stream:    stream,
			Data:      data,
			EmittedAt: now.UnixMilli(),
		},
	}

	l.recordEncoder.Encode(msg)

	l.records = append(l.records, msg)
	if len(l.records) == MaxBatchSize {
		l.Flush()
	}
}

func (l *logger) Flush() {
	for _, record := range l.records {
		l.recordEncoder.Encode(record)
	}
	l.records = l.records[:0]
}

func (l *logger) State(syncState *State) {
	l.recordEncoder.Encode(Message{
		Type:  MessageTypeState,
		State: syncState,
	})
}
