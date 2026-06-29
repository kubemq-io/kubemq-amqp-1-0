package worker

import (
	"strconv"
	"time"

	amqp "github.com/Azure/go-amqp"

	"github.com/kubemq-io/kubemq-amqp-1-0/burnin/payload"
)

// buildMessage constructs a burn-in AMQP 1.0 message: an opaque Data body plus
// the self-accounting envelope in application-properties (spec §9.3). The body
// passes through the connector bit-exact; the worker-id / sequence / crc /
// timestamp are carried as application-properties (which survive the connector's
// properties → Metadata round-trip).
func buildMessage(producerID string, seq uint64, body []byte, crcHex string) *amqp.Message {
	msg := amqp.NewMessage(body)
	msg.ApplicationProperties = map[string]any{
		payload.PropWorkerID:    producerID,
		payload.PropSequence:    strconv.FormatUint(seq, 10),
		payload.PropContentHash: crcHex,
		payload.PropTimestampNS: strconv.FormatInt(time.Now().UnixNano(), 10),
	}
	return msg
}

// extractMeta pulls (producerID, seq, crcHex, sentAt) from message
// application-properties. Missing/garbled fields yield zero values with ok=false.
// Values may arrive as string or numeric depending on the connector encoding, so
// both are tolerated.
func extractMeta(msg *amqp.Message) (producerID string, seq uint64, crcHex string, sentAt time.Time, ok bool) {
	if msg == nil || msg.ApplicationProperties == nil {
		return "", 0, "", time.Time{}, false
	}
	ap := msg.ApplicationProperties

	producerID = asString(ap[payload.PropWorkerID])
	crcHex = asString(ap[payload.PropContentHash])
	seq = asUint64(ap[payload.PropSequence])
	if ns := asInt64(ap[payload.PropTimestampNS]); ns > 0 {
		sentAt = time.Unix(0, ns)
	}

	ok = producerID != ""
	return producerID, seq, crcHex, sentAt, ok
}

func asString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	default:
		return ""
	}
}

func asUint64(v any) uint64 {
	switch t := v.(type) {
	case string:
		if n, err := strconv.ParseUint(t, 10, 64); err == nil {
			return n
		}
	case uint64:
		return t
	case int64:
		if t >= 0 {
			return uint64(t)
		}
	case int:
		if t >= 0 {
			return uint64(t)
		}
	}
	return 0
}

func asInt64(v any) int64 {
	switch t := v.(type) {
	case string:
		if n, err := strconv.ParseInt(t, 10, 64); err == nil {
			return n
		}
	case int64:
		return t
	case uint64:
		return int64(t)
	case int:
		return int64(t)
	}
	return 0
}
