// Package config defines the burn-in harness configuration model, defaults,
// loading, validation, and duration parsing for the KubeMQ AMQP 1.0 connector
// burn-in (spec §9.3/§9.5). It is cloned from the kubemq-amqp-rabbitmq burn-in
// config and recast for AMQP 1.0: the 0-9-1 amqp block is replaced by an amqp10
// knob block (settle_mode, credit, drain, …), the six 0-9-1 workers by the six
// AMQP 1.0 workers, and the broker var is KUBEMQ_BROKER_ADDRESS (default
// localhost:5672, dialed as amqp://{address}, SASL ANONYMOUS by default).
package config

import (
	"bytes"
	cryptorand "crypto/rand"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// ConfigVersion is the required config schema version.
const ConfigVersion = "2"

// Worker name constants (spec §9.2 — the six AMQP 1.0 workers).
const (
	WorkerQueues      = "queues"
	WorkerEvents      = "events"
	WorkerEventsStore = "events_store"
	WorkerCommands    = "commands"
	WorkerQueries     = "queries"
	WorkerCreditFlow  = "credit_flow"
)

// AllWorkerNames lists all six AMQP 1.0 burn-in workers in a stable order.
var AllWorkerNames = []string{
	WorkerQueues,
	WorkerEvents,
	WorkerEventsStore,
	WorkerCommands,
	WorkerQueries,
	WorkerCreditFlow,
}

// IsRPCWorker reports whether the worker uses the native AMQP request/reply
// model (dynamic reply node + correlation-id) rather than producer/consumer.
func IsRPCWorker(name string) bool {
	return name == WorkerCommands || name == WorkerQueries
}

// BrokerConfig holds the broker address (host:port for the AMQP 1.0 listener).
type BrokerConfig struct {
	Address        string `yaml:"address" json:"address"`
	ClientIDPrefix string `yaml:"client_id_prefix" json:"client_id_prefix"`
}

// TLSConfig is the optional amqp10.tls sub-block (amqps://…:5671/). When Enabled
// is true the harness dials over TLS. CA/cert/key paths are optional (one-way
// TLS works with just the CA, mTLS supplies all three).
type TLSConfig struct {
	Enabled    bool   `yaml:"enabled" json:"enabled"`
	CAFile     string `yaml:"ca_file" json:"ca_file"`
	CertFile   string `yaml:"cert_file" json:"cert_file"`
	KeyFile    string `yaml:"key_file" json:"key_file"`
	ServerName string `yaml:"server_name" json:"server_name"`
	SkipVerify bool   `yaml:"skip_verify" json:"skip_verify"`
}

// AuthConfig is the optional amqp10.auth sub-block. When Enabled is true the
// harness dials with SASL PLAIN (username + JWT-in-password); otherwise it dials
// SASL ANONYMOUS (the connector default).
type AuthConfig struct {
	Enabled  bool   `yaml:"enabled" json:"enabled"`
	Username string `yaml:"username" json:"username"`
	Password string `yaml:"password" json:"password"`
}

// Amqp10Config is the AMQP 1.0 knob block (spec §9.3) replacing the 0-9-1 amqp
// block. Per-worker overrides (settle_mode) are applied by the workers.
type Amqp10Config struct {
	// SettleMode: "unsettled" (at-least-once) | "settled" (at-most-once).
	SettleMode string `yaml:"settle_mode" json:"settle_mode"`
	// Credit is the standing link credit a consumer grants (≤ MaxUnsettledPerLink=1024).
	Credit int32 `yaml:"credit" json:"credit"`
	// Drain sends FLOW{drain=true} to exhaust then stop (credit_flow worker).
	Drain bool `yaml:"drain" json:"drain"`
	// GetBatchSize is informational (server caps queue Get at min(credit, GetBatchSize, MaxUnsettledPerLink-unsettled)).
	GetBatchSize int `yaml:"get_batch_size" json:"get_batch_size"`
	// RPCTimeoutMs maps to the request header.ttl; server clamps to [1s, DefaultRpcTimeoutSeconds*10].
	RPCTimeoutMs int `yaml:"rpc_timeout_ms" json:"rpc_timeout_ms"`
	// Durable: events_store SourceExpiryPolicy=ExpiryPolicyNever.
	Durable bool `yaml:"durable" json:"durable"`
	// StartPosition: events_store x-opt-kubemq-start (new-only|first|last|sequence:N|time:T|time-delta:S).
	StartPosition string `yaml:"start_position" json:"start_position"`
	// Selector is an optional events/events_store source filter (apache.org:selector-filter:string).
	Selector string `yaml:"selector" json:"selector"`

	TLS  TLSConfig  `yaml:"tls" json:"tls"`
	Auth AuthConfig `yaml:"auth" json:"auth"`
}

// WorkerConfig holds the per-worker concurrency + rate knobs. ProducersPerChannel
// and ConsumersPerChannel apply to the pub/sub-style workers; SendersPerChannel
// and RespondersPerChannel apply to the RPC workers (commands/queries, spec §9.3).
// Subscribers is used by the events worker (consumer-group members) when > 0.
type WorkerConfig struct {
	Enabled              bool   `yaml:"enabled" json:"enabled"`
	Channels             int    `yaml:"channels" json:"channels"`
	ProducersPerChannel  int    `yaml:"producers_per_channel" json:"producers_per_channel"`
	ConsumersPerChannel  int    `yaml:"consumers_per_channel" json:"consumers_per_channel"`
	SendersPerChannel    int    `yaml:"senders_per_channel" json:"senders_per_channel"`
	RespondersPerChannel int    `yaml:"responders_per_channel" json:"responders_per_channel"`
	Subscribers          int    `yaml:"subscribers" json:"subscribers"`
	Rate                 int    `yaml:"rate" json:"rate"`
	SettleMode           string `yaml:"settle_mode" json:"settle_mode"`
}

// WorkersConfig groups the six worker blocks.
type WorkersConfig struct {
	Queues      WorkerConfig `yaml:"queues" json:"queues"`
	Events      WorkerConfig `yaml:"events" json:"events"`
	EventsStore WorkerConfig `yaml:"events_store" json:"events_store"`
	Commands    WorkerConfig `yaml:"commands" json:"commands"`
	Queries     WorkerConfig `yaml:"queries" json:"queries"`
	CreditFlow  WorkerConfig `yaml:"credit_flow" json:"credit_flow"`
}

// RPCConfig holds RPC round-trip timeout.
type RPCConfig struct {
	TimeoutMs int `yaml:"timeout_ms" json:"timeout_ms"`
}

// MessageConfig holds payload sizing knobs (CRC32 + sequence stamped into AMQP
// application-properties, body passed through bit-exact).
type MessageConfig struct {
	SizeMode         string `yaml:"size_mode" json:"size_mode"`
	SizeBytes        int    `yaml:"size_bytes" json:"size_bytes"`
	SizeDistribution string `yaml:"size_distribution" json:"size_distribution"`
	ReorderWindow    int    `yaml:"reorder_window" json:"reorder_window"`
}

// MetricsConfig holds the control HTTP port and report interval.
type MetricsConfig struct {
	Port           int    `yaml:"port" json:"port"`
	ReportInterval string `yaml:"report_interval" json:"report_interval"`
}

// LoggingConfig holds log format and level.
type LoggingConfig struct {
	Format string `yaml:"format" json:"format"`
	Level  string `yaml:"level" json:"level"`
}

// ForcedDisconnConfig drives the connection-churn injector.
type ForcedDisconnConfig struct {
	Interval string `yaml:"interval" json:"interval"`
	Duration string `yaml:"duration" json:"duration"`
}

// RecoveryConfig holds reconnect backoff knobs.
type RecoveryConfig struct {
	ReconnectInterval    string  `yaml:"reconnect_interval" json:"reconnect_interval"`
	ReconnectMaxInterval string  `yaml:"reconnect_max_interval" json:"reconnect_max_interval"`
	ReconnectMultiplier  float64 `yaml:"reconnect_multiplier" json:"reconnect_multiplier"`
}

// ShutdownConfig holds the drain timeout.
type ShutdownConfig struct {
	DrainTimeoutSeconds int `yaml:"drain_timeout_seconds" json:"drain_timeout_seconds"`
}

// OutputConfig holds report output knobs.
type OutputConfig struct {
	ReportFile string `yaml:"report_file" json:"report_file"`
	SDKVersion string `yaml:"sdk_version" json:"sdk_version"`
}

// ThresholdsConfig holds pass/fail thresholds (spec §9.4). The amqp-rabbitmq base
// already exposes P50/P95/P99/P999; defaults are retuned to 500/1500/2000/10000
// for AMQP 1.0. MaxEventsLossPct gates the at-most-once events worker; the
// CE/0-9-1 fidelity-failure threshold is replaced by MaxAmqp10FidelityFailures.
type ThresholdsConfig struct {
	MaxLossPct                float64 `yaml:"max_loss_pct" json:"max_loss_pct"`
	MaxEventsLossPct          float64 `yaml:"max_events_loss_pct" json:"max_events_loss_pct"`
	MaxDuplicationPct         float64 `yaml:"max_duplication_pct" json:"max_duplication_pct"`
	MaxP50LatencyMS           float64 `yaml:"max_p50_latency_ms" json:"max_p50_latency_ms"`
	MaxP95LatencyMS           float64 `yaml:"max_p95_latency_ms" json:"max_p95_latency_ms"`
	MaxP99LatencyMS           float64 `yaml:"max_p99_latency_ms" json:"max_p99_latency_ms"`
	MaxP999LatencyMS          float64 `yaml:"max_p999_latency_ms" json:"max_p999_latency_ms"`
	MinThroughputPct          float64 `yaml:"min_throughput_pct" json:"min_throughput_pct"`
	MaxErrorRatePct           float64 `yaml:"max_error_rate_pct" json:"max_error_rate_pct"`
	MaxMemoryGrowthFactor     float64 `yaml:"max_memory_growth_factor" json:"max_memory_growth_factor"`
	MaxDowntimePct            float64 `yaml:"max_downtime_pct" json:"max_downtime_pct"`
	MaxAmqp10FidelityFailures int     `yaml:"max_amqp10_fidelity_failures" json:"max_amqp10_fidelity_failures"`
	MaxDuration               string  `yaml:"max_duration" json:"max_duration"`
}

// WarmupConfig holds warmup parallelism + per-channel timeout.
type WarmupConfig struct {
	MaxParallelChannels int `yaml:"max_parallel_channels" json:"max_parallel_channels"`
	TimeoutPerChannelMs int `yaml:"timeout_per_channel_ms" json:"timeout_per_channel_ms"`
}

// CORSConfig holds the allowed origins for the control API.
type CORSConfig struct {
	Origins string `yaml:"origins" json:"origins"`
}

// Config is the full burn-in configuration.
type Config struct {
	Version          string              `yaml:"version" json:"version"`
	Broker           BrokerConfig        `yaml:"broker" json:"broker"`
	Mode             string              `yaml:"mode" json:"mode"`
	Duration         string              `yaml:"duration" json:"duration"`
	RunID            string              `yaml:"run_id" json:"run_id"`
	WarmupDuration   string              `yaml:"warmup_duration" json:"warmup_duration"`
	Amqp10           Amqp10Config        `yaml:"amqp10" json:"amqp10"`
	Workers          WorkersConfig       `yaml:"workers" json:"workers"`
	RPC              RPCConfig           `yaml:"rpc" json:"rpc"`
	Message          MessageConfig       `yaml:"message" json:"message"`
	Metrics          MetricsConfig       `yaml:"metrics" json:"metrics"`
	Logging          LoggingConfig       `yaml:"logging" json:"logging"`
	ForcedDisconnect ForcedDisconnConfig `yaml:"forced_disconnect" json:"forced_disconnect"`
	Recovery         RecoveryConfig      `yaml:"recovery" json:"recovery"`
	Shutdown         ShutdownConfig      `yaml:"shutdown" json:"shutdown"`
	Output           OutputConfig        `yaml:"output" json:"output"`
	Thresholds       ThresholdsConfig    `yaml:"thresholds" json:"thresholds"`
	Warmup           WarmupConfig        `yaml:"warmup" json:"warmup"`
	CORS             CORSConfig          `yaml:"cors" json:"cors"`

	DurationParsed        time.Duration `yaml:"-" json:"-"`
	WarmupDurationParsed  time.Duration `yaml:"-" json:"-"`
	ReportIntervalParsed  time.Duration `yaml:"-" json:"-"`
	ForcedDisconnInterval time.Duration `yaml:"-" json:"-"`
	ForcedDisconnDuration time.Duration `yaml:"-" json:"-"`
	ReconnectInterval     time.Duration `yaml:"-" json:"-"`
	ReconnectMaxInterval  time.Duration `yaml:"-" json:"-"`
	MaxDurationParsed     time.Duration `yaml:"-" json:"-"`
	Warnings              []string      `yaml:"-" json:"-"`
}

// DefaultConfig returns the built-in default configuration.
func DefaultConfig() *Config {
	c := &Config{}
	c.Version = ConfigVersion
	c.Broker.Address = "localhost:5672"
	c.Broker.ClientIDPrefix = "burnin-amqp10"
	c.Mode = "soak"
	c.Duration = "1h"

	c.Amqp10 = Amqp10Config{
		SettleMode:    "unsettled",
		Credit:        100,
		Drain:         false,
		GetBatchSize:  32,
		RPCTimeoutMs:  5000,
		Durable:       true,
		StartPosition: "new-only",
		Selector:      "",
	}

	// All six workers are real and enabled by default.
	c.Workers = WorkersConfig{
		Queues: WorkerConfig{
			Enabled: true, Channels: 1,
			ProducersPerChannel: 1, ConsumersPerChannel: 2,
			Rate: 100,
		},
		Events: WorkerConfig{
			Enabled: true, Channels: 1,
			ProducersPerChannel: 1, Subscribers: 2,
			Rate: 50,
		},
		EventsStore: WorkerConfig{
			Enabled: true, Channels: 1,
			ProducersPerChannel: 1, ConsumersPerChannel: 1,
			Rate: 50,
		},
		Commands: WorkerConfig{
			Enabled: true, Channels: 1,
			SendersPerChannel: 1, RespondersPerChannel: 1,
			Rate: 30,
		},
		Queries: WorkerConfig{
			Enabled: true, Channels: 1,
			SendersPerChannel: 1, RespondersPerChannel: 1,
			Rate: 30,
		},
		CreditFlow: WorkerConfig{
			Enabled: true, Channels: 1,
			ProducersPerChannel: 1, ConsumersPerChannel: 1,
			Rate: 50,
		},
	}

	c.RPC.TimeoutMs = 5000

	c.Message = MessageConfig{
		SizeMode:         "fixed",
		SizeBytes:        1024,
		SizeDistribution: "256:80,4096:15,65536:5",
		ReorderWindow:    10_000,
	}

	c.Metrics = MetricsConfig{
		Port:           8896,
		ReportInterval: "30s",
	}

	c.Logging = LoggingConfig{Format: "text", Level: "info"}

	c.ForcedDisconnect = ForcedDisconnConfig{
		Interval: "0",
		Duration: "5s",
	}

	c.Recovery = RecoveryConfig{
		ReconnectInterval:    "1s",
		ReconnectMaxInterval: "30s",
		ReconnectMultiplier:  2.0,
	}

	c.Shutdown.DrainTimeoutSeconds = 10

	c.Thresholds = ThresholdsConfig{
		MaxLossPct:                0.0,
		MaxEventsLossPct:          5.0,
		MaxDuplicationPct:         0.1,
		MaxP50LatencyMS:           500,
		MaxP95LatencyMS:           1500,
		MaxP99LatencyMS:           2000,
		MaxP999LatencyMS:          10000,
		MinThroughputPct:          80,
		MaxErrorRatePct:           1.0,
		MaxMemoryGrowthFactor:     2.0,
		MaxDowntimePct:            10,
		MaxAmqp10FidelityFailures: 0,
		MaxDuration:               "168h",
	}

	c.Warmup = WarmupConfig{
		MaxParallelChannels: 10,
		TimeoutPerChannelMs: 5000,
	}

	c.CORS.Origins = "*"

	return c
}

// GetWorkerConfig returns a pointer to the named worker's config block.
func (c *Config) GetWorkerConfig(name string) *WorkerConfig {
	switch name {
	case WorkerQueues:
		return &c.Workers.Queues
	case WorkerEvents:
		return &c.Workers.Events
	case WorkerEventsStore:
		return &c.Workers.EventsStore
	case WorkerCommands:
		return &c.Workers.Commands
	case WorkerQueries:
		return &c.Workers.Queries
	case WorkerCreditFlow:
		return &c.Workers.CreditFlow
	default:
		return nil
	}
}

// GetWorkerRate returns the configured rate for a worker (fallback 100).
func (c *Config) GetWorkerRate(name string) int {
	if wc := c.GetWorkerConfig(name); wc != nil {
		return wc.Rate
	}
	return 100
}

// GetWorkerChannels returns the configured channel count for a worker (min 1).
func (c *Config) GetWorkerChannels(name string) int {
	if wc := c.GetWorkerConfig(name); wc != nil && wc.Channels > 0 {
		return wc.Channels
	}
	return 1
}

// WorkerSettleMode returns the effective settle mode for a worker (per-worker
// override falling back to the global amqp10.settle_mode).
func (c *Config) WorkerSettleMode(name string) string {
	if wc := c.GetWorkerConfig(name); wc != nil && wc.SettleMode != "" {
		return wc.SettleMode
	}
	if c.Amqp10.SettleMode != "" {
		return c.Amqp10.SettleMode
	}
	return "unsettled"
}

// TotalChannelCount sums enabled worker channel counts.
func (c *Config) TotalChannelCount() int {
	total := 0
	for _, name := range AllWorkerNames {
		if wc := c.GetWorkerConfig(name); wc != nil && wc.Enabled {
			total += wc.Channels
		}
	}
	return total
}

// Load reads and parses the config file (or just defaults when path == ""),
// applies env overrides, parses durations, and mints a run ID.
func Load(path string) (*Config, error) {
	c := DefaultConfig()

	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read config file %s: %w", path, err)
		}

		decoder := yaml.NewDecoder(bytes.NewReader(data))
		decoder.KnownFields(true)
		if err := decoder.Decode(c); err != nil {
			// Re-parse tolerantly so unknown fields warn rather than fail.
			c2 := DefaultConfig()
			if err2 := yaml.Unmarshal(data, c2); err2 != nil {
				return nil, fmt.Errorf("parse config file %s: %w", path, err2)
			}
			*c = *c2
			c.Warnings = append(c.Warnings, fmt.Sprintf("config has unknown fields: %v", err))
		}
	}

	applyEnvOverrides(c)

	if err := parseDurations(c); err != nil {
		return nil, err
	}

	if c.RunID == "" {
		c.RunID = RandomRunID()
	}

	return c, nil
}

// FindConfigFile resolves the config path search order (spec §9.5):
// CLI flag → ./burnin-config.yaml → /etc/burnin/config.yaml.
func FindConfigFile(cliPath string) string {
	if cliPath != "" {
		return cliPath
	}
	if _, err := os.Stat("./burnin-config.yaml"); err == nil {
		return "./burnin-config.yaml"
	}
	if _, err := os.Stat("/etc/burnin/config.yaml"); err == nil {
		return "/etc/burnin/config.yaml"
	}
	return ""
}

// Validate checks the config and returns a slice of errors. Entries prefixed
// "WARNING:" are advisory and do not fail validation.
func (c *Config) Validate() []error {
	var errs []error

	if c.Version != ConfigVersion {
		errs = append(errs, fmt.Errorf("version must be %q, got %q", ConfigVersion, c.Version))
	}

	if c.Broker.Address == "" {
		errs = append(errs, fmt.Errorf("broker.address is required"))
	}

	validSettle := map[string]bool{"": true, "unsettled": true, "settled": true}
	if !validSettle[c.Amqp10.SettleMode] {
		errs = append(errs, fmt.Errorf("amqp10.settle_mode must be one of \"\"|unsettled|settled, got %q", c.Amqp10.SettleMode))
	}
	if c.Amqp10.Credit < 1 || c.Amqp10.Credit > 1024 {
		errs = append(errs, fmt.Errorf("amqp10.credit must be 1-1024 (MaxUnsettledPerLink), got %d", c.Amqp10.Credit))
	}
	if c.Amqp10.GetBatchSize < 1 {
		errs = append(errs, fmt.Errorf("amqp10.get_batch_size must be >= 1, got %d", c.Amqp10.GetBatchSize))
	}
	if c.Amqp10.RPCTimeoutMs <= 0 {
		errs = append(errs, fmt.Errorf("amqp10.rpc_timeout_ms must be > 0, got %d", c.Amqp10.RPCTimeoutMs))
	}
	if !validStartPosition(c.Amqp10.StartPosition) {
		errs = append(errs, fmt.Errorf("amqp10.start_position invalid: %q (new-only|first|last|sequence:N|time:T|time-delta:S)", c.Amqp10.StartPosition))
	}
	if c.Amqp10.Auth.Enabled && c.Amqp10.Auth.Username == "" {
		errs = append(errs, fmt.Errorf("amqp10.auth.username is required when auth.enabled"))
	}

	enabledCount := 0
	totalWorkers := 0

	for _, name := range AllWorkerNames {
		wc := c.GetWorkerConfig(name)
		if wc == nil || !wc.Enabled {
			continue
		}
		enabledCount++

		if wc.Channels < 1 || wc.Channels > 1000 {
			errs = append(errs, fmt.Errorf("%s.channels: must be 1-1000, got %d", name, wc.Channels))
		}
		if wc.Rate <= 0 {
			errs = append(errs, fmt.Errorf("%s.rate: must be > 0, got %d", name, wc.Rate))
		}
		if wc.SettleMode != "" && !validSettle[wc.SettleMode] {
			errs = append(errs, fmt.Errorf("%s.settle_mode invalid: %q", name, wc.SettleMode))
		}

		if IsRPCWorker(name) {
			if wc.SendersPerChannel < 1 {
				errs = append(errs, fmt.Errorf("%s.senders_per_channel: must be >= 1, got %d", name, wc.SendersPerChannel))
			}
			if wc.RespondersPerChannel < 1 {
				errs = append(errs, fmt.Errorf("%s.responders_per_channel: must be >= 1, got %d", name, wc.RespondersPerChannel))
			}
			totalWorkers += wc.Channels * (wc.SendersPerChannel + wc.RespondersPerChannel)
		} else {
			if wc.ProducersPerChannel < 1 {
				errs = append(errs, fmt.Errorf("%s.producers_per_channel: must be >= 1, got %d", name, wc.ProducersPerChannel))
			}
			consumers := wc.ConsumersPerChannel
			if name == WorkerEvents {
				if wc.Subscribers < 1 {
					errs = append(errs, fmt.Errorf("%s.subscribers: must be >= 1, got %d", name, wc.Subscribers))
				}
				consumers = wc.Subscribers
			} else if wc.ConsumersPerChannel < 1 {
				errs = append(errs, fmt.Errorf("%s.consumers_per_channel: must be >= 1, got %d", name, wc.ConsumersPerChannel))
			}
			totalWorkers += wc.Channels * (wc.ProducersPerChannel + consumers)
		}
	}

	if enabledCount == 0 {
		errs = append(errs, fmt.Errorf("at least one worker must be enabled"))
	}

	if c.Message.SizeMode != "fixed" && c.Message.SizeMode != "distribution" {
		errs = append(errs, fmt.Errorf("message.size_mode must be 'fixed' or 'distribution', got %q", c.Message.SizeMode))
	}
	if c.Message.SizeMode == "fixed" && c.Message.SizeBytes < 64 {
		errs = append(errs, fmt.Errorf("message.size_bytes: must be >= 64, got %d", c.Message.SizeBytes))
	}
	if c.Message.ReorderWindow < 100 {
		errs = append(errs, fmt.Errorf("message.reorder_window: must be >= 100, got %d", c.Message.ReorderWindow))
	}

	if c.RPC.TimeoutMs <= 0 {
		errs = append(errs, fmt.Errorf("rpc.timeout_ms: must be > 0, got %d", c.RPC.TimeoutMs))
	}
	if c.Shutdown.DrainTimeoutSeconds <= 0 {
		errs = append(errs, fmt.Errorf("shutdown.drain_timeout_seconds: must be > 0, got %d", c.Shutdown.DrainTimeoutSeconds))
	}
	if c.Metrics.Port < 1 || c.Metrics.Port > 65535 {
		errs = append(errs, fmt.Errorf("metrics.port: must be 1-65535, got %d", c.Metrics.Port))
	}

	if c.Thresholds.MaxLossPct < 0 || c.Thresholds.MaxLossPct > 100 {
		errs = append(errs, fmt.Errorf("thresholds.max_loss_pct: must be 0-100"))
	}
	if c.Thresholds.MaxEventsLossPct < 0 || c.Thresholds.MaxEventsLossPct > 100 {
		errs = append(errs, fmt.Errorf("thresholds.max_events_loss_pct: must be 0-100"))
	}
	if c.Thresholds.MaxDuplicationPct < 0 || c.Thresholds.MaxDuplicationPct > 100 {
		errs = append(errs, fmt.Errorf("thresholds.max_duplication_pct: must be 0-100"))
	}
	for label, v := range map[string]float64{
		"max_p50_latency_ms":  c.Thresholds.MaxP50LatencyMS,
		"max_p95_latency_ms":  c.Thresholds.MaxP95LatencyMS,
		"max_p99_latency_ms":  c.Thresholds.MaxP99LatencyMS,
		"max_p999_latency_ms": c.Thresholds.MaxP999LatencyMS,
	} {
		if v <= 0 {
			errs = append(errs, fmt.Errorf("thresholds.%s: must be > 0", label))
		}
	}
	if c.Thresholds.MinThroughputPct <= 0 || c.Thresholds.MinThroughputPct > 100 {
		errs = append(errs, fmt.Errorf("thresholds.min_throughput_pct: must be > 0 and <= 100"))
	}
	if c.Thresholds.MaxErrorRatePct < 0 || c.Thresholds.MaxErrorRatePct > 100 {
		errs = append(errs, fmt.Errorf("thresholds.max_error_rate_pct: must be 0-100"))
	}
	if c.Thresholds.MaxMemoryGrowthFactor < 1.0 {
		errs = append(errs, fmt.Errorf("thresholds.max_memory_growth_factor: must be >= 1.0"))
	}
	if c.Thresholds.MaxDowntimePct < 0 || c.Thresholds.MaxDowntimePct > 100 {
		errs = append(errs, fmt.Errorf("thresholds.max_downtime_pct: must be 0-100"))
	}
	if c.Thresholds.MaxAmqp10FidelityFailures < 0 {
		errs = append(errs, fmt.Errorf("thresholds.max_amqp10_fidelity_failures: must be >= 0"))
	}
	if c.Recovery.ReconnectMultiplier < 1.0 {
		errs = append(errs, fmt.Errorf("recovery.reconnect_multiplier: must be >= 1.0, got %f", c.Recovery.ReconnectMultiplier))
	}

	if totalWorkers > 1000 {
		errs = append(errs, fmt.Errorf("WARNING: high worker count: %d -- may impact system resources", totalWorkers))
	}

	return errs
}

// validStartPosition validates the events_store start position grammar.
func validStartPosition(s string) bool {
	switch s {
	case "", "new-only", "first", "last":
		return true
	}
	for _, prefix := range []string{"sequence:", "time:", "time-delta:"} {
		if strings.HasPrefix(s, prefix) && len(s) > len(prefix) {
			return true
		}
	}
	return false
}

// LogResourceWarnings logs any advisory (WARNING:-prefixed) validation entries.
func (c *Config) LogResourceWarnings(logger *slog.Logger) {
	for _, e := range c.Validate() {
		if strings.HasPrefix(e.Error(), "WARNING:") {
			logger.Warn(e.Error())
		}
	}
}

func applyEnvOverrides(cfg *Config) {
	if v := os.Getenv("KUBEMQ_BROKER_ADDRESS"); v != "" {
		cfg.Broker.Address = v
	}

	envNames := map[string]string{
		WorkerQueues:      "QUEUES",
		WorkerEvents:      "EVENTS",
		WorkerEventsStore: "EVENTS_STORE",
		WorkerCommands:    "COMMANDS",
		WorkerQueries:     "QUERIES",
		WorkerCreditFlow:  "CREDIT_FLOW",
	}

	for name, env := range envNames {
		wc := cfg.GetWorkerConfig(name)
		if wc == nil {
			continue
		}
		if v := os.Getenv("BURNIN_" + env + "_RATE"); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				wc.Rate = n
			}
		}
		if v := os.Getenv("BURNIN_" + env + "_CHANNELS"); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				wc.Channels = n
			}
		}
		if v := os.Getenv("BURNIN_" + env + "_ENABLED"); v != "" {
			wc.Enabled = v == "true" || v == "1"
		}
	}
}

func parseDurations(c *Config) error {
	var err error

	if c.Duration != "" && c.Duration != "0" {
		c.DurationParsed, err = parseDuration(c.Duration)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", c.Duration, err)
		}
	}
	if c.WarmupDuration != "" {
		c.WarmupDurationParsed, err = parseDuration(c.WarmupDuration)
		if err != nil {
			return fmt.Errorf("invalid warmup_duration %q: %w", c.WarmupDuration, err)
		}
	}
	if c.Metrics.ReportInterval != "" {
		c.ReportIntervalParsed, err = parseDuration(c.Metrics.ReportInterval)
		if err != nil {
			return fmt.Errorf("invalid metrics.report_interval %q: %w", c.Metrics.ReportInterval, err)
		}
	}
	if c.ForcedDisconnect.Interval != "" && c.ForcedDisconnect.Interval != "0" {
		c.ForcedDisconnInterval, err = parseDuration(c.ForcedDisconnect.Interval)
		if err != nil {
			return fmt.Errorf("invalid forced_disconnect.interval %q: %w", c.ForcedDisconnect.Interval, err)
		}
	}
	if c.ForcedDisconnect.Duration != "" {
		c.ForcedDisconnDuration, err = parseDuration(c.ForcedDisconnect.Duration)
		if err != nil {
			return fmt.Errorf("invalid forced_disconnect.duration %q: %w", c.ForcedDisconnect.Duration, err)
		}
	}
	if c.Recovery.ReconnectInterval != "" {
		c.ReconnectInterval, err = parseDuration(c.Recovery.ReconnectInterval)
		if err != nil {
			return fmt.Errorf("invalid recovery.reconnect_interval %q: %w", c.Recovery.ReconnectInterval, err)
		}
	}
	if c.Recovery.ReconnectMaxInterval != "" {
		c.ReconnectMaxInterval, err = parseDuration(c.Recovery.ReconnectMaxInterval)
		if err != nil {
			return fmt.Errorf("invalid recovery.reconnect_max_interval %q: %w", c.Recovery.ReconnectMaxInterval, err)
		}
	}
	if c.Thresholds.MaxDuration != "" {
		c.MaxDurationParsed, err = parseDuration(c.Thresholds.MaxDuration)
		if err != nil {
			return fmt.Errorf("invalid thresholds.max_duration %q: %w", c.Thresholds.MaxDuration, err)
		}
	}

	return nil
}

func parseDuration(s string) (time.Duration, error) {
	if strings.HasSuffix(s, "d") {
		s = strings.TrimSuffix(s, "d")
		days, err := strconv.Atoi(s)
		if err != nil {
			return 0, fmt.Errorf("invalid day duration: %s", s)
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	return time.ParseDuration(s)
}

// RandomRunID returns an 8-hex-char random run identifier.
func RandomRunID() string {
	b := make([]byte, 4)
	_, _ = cryptorand.Read(b)
	return fmt.Sprintf("%08x", b)
}

// ParseDurationsPublic re-parses durations on a config (used after API overlay).
func ParseDurationsPublic(cfg *Config) error {
	return parseDurations(cfg)
}
