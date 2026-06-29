// Package payload builds and verifies burn-in message bodies and stamps the
// self-accounting envelope. Per spec §9.3 the CRC32 is computed over the RAW
// AMQP Data body bytes (the connector passes the body through bit-exact) and the
// worker-id / sequence / contenthash / timestamp are carried in AMQP 1.0
// application-properties, NOT in the hashed body — so no canonicalization is
// needed and the body round-trips byte-for-byte.
package payload

import (
	"fmt"
	"hash/crc32"
	"math/rand/v2"
	"strconv"
	"strings"
)

// Application-property keys stamped onto every burn-in send. String/int values
// survive the connector's application-properties → Metadata round-trip unchanged.
const (
	PropWorkerID    = "worker-id"
	PropSequence    = "sequence"
	PropContentHash = "crc"
	PropTimestampNS = "timestamp-ns"
)

// Build returns a deterministic-length body of targetSize bytes (min 1) and its
// CRC32 hex string. The body is opaque random bytes; integrity is verified by
// re-hashing the received body against the contenthash application-property.
func Build(targetSize int) (body []byte, crcHex string) {
	if targetSize < 1 {
		targetSize = 1
	}
	body = randomBytes(targetSize)
	crcHex = fmt.Sprintf("%08x", crc32.ChecksumIEEE(body))
	return body, crcHex
}

// VerifyCRC checks the CRC32 hex tag against the actual body bytes.
func VerifyCRC(body []byte, crcHex string) bool {
	actual := fmt.Sprintf("%08x", crc32.ChecksumIEEE(body))
	return actual == crcHex
}

// SizeDistribution represents weighted size options for message payloads.
type SizeDistribution struct {
	sizes   []int
	weights []int
	total   int
}

// ParseDistribution parses a "size:weight,size:weight" string.
func ParseDistribution(s string) (*SizeDistribution, error) {
	d := &SizeDistribution{}
	for _, p := range strings.Split(s, ",") {
		kv := strings.SplitN(strings.TrimSpace(p), ":", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid distribution entry: %q", p)
		}
		size, err := strconv.Atoi(strings.TrimSpace(kv[0]))
		if err != nil {
			return nil, fmt.Errorf("invalid size in distribution: %q", kv[0])
		}
		weight, err := strconv.Atoi(strings.TrimSpace(kv[1]))
		if err != nil {
			return nil, fmt.Errorf("invalid weight in distribution: %q", kv[1])
		}
		d.sizes = append(d.sizes, size)
		d.weights = append(d.weights, weight)
		d.total += weight
	}
	if d.total == 0 {
		return nil, fmt.Errorf("distribution total weight must be > 0")
	}
	return d, nil
}

// SelectSize returns a size sampled from the weighted distribution.
func (d *SizeDistribution) SelectSize() int {
	r := rand.IntN(d.total)
	cumulative := 0
	for i, w := range d.weights {
		cumulative += w
		if r < cumulative {
			return d.sizes[i]
		}
	}
	return d.sizes[len(d.sizes)-1]
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	for i := 0; i < n; {
		v := rand.Uint64()
		for j := 0; j < 8 && i < n; j++ {
			b[i] = byte(v)
			v >>= 8
			i++
		}
	}
	return b
}
