package iputil

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// IPRange represents an IP address range with associated attributes.
type IPRange struct {
	Start uint32
	End   uint32
	Attrs map[string]string // attributes like country, province, city, isp
}

// IPDB is a loaded IP database ready for queries.
type IPDB struct {
	ranges    []IPRange // sorted by Start, non-overlapping
	cacheSize int
	cacheMu   sync.RWMutex
	cache     map[string]map[string]string
	cacheKeys []string
}

// LoadCSV loads IP ranges from a CSV file.
// Expected format: start_ip,end_ip[,attr1,attr2,...]
// columns defines the attribute names for each column after the first two.
func LoadCSV(path string, columns []string) (*IPDB, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open IP DB: %w", err)
	}
	defer f.Close()

	var ranges []IPRange
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 256*1024), 16*1024*1024)

	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Skip header row if first line looks like a header
		if lineNum == 1 && (strings.Contains(line, "start") || strings.Contains(line, "begin") || strings.Contains(line, "起始")) {
			continue
		}

		parts := strings.Split(line, ",")
		if len(parts) < 2 {
			continue
		}

		start, err := ipToUint32(strings.TrimSpace(parts[0]))
		if err != nil {
			return nil, fmt.Errorf("line %d: invalid start IP %q: %w", lineNum, parts[0], err)
		}
		end, err := ipToUint32(strings.TrimSpace(parts[1]))
		if err != nil {
			return nil, fmt.Errorf("line %d: invalid end IP %q: %w", lineNum, parts[1], err)
		}

		attrs := make(map[string]string)
		for i := 2; i < len(parts) && i-2 < len(columns); i++ {
			attrs[columns[i-2]] = strings.TrimSpace(parts[i])
		}

		ranges = append(ranges, IPRange{Start: start, End: end, Attrs: attrs})
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan IP DB: %w", err)
	}

	// Sort by start IP
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].Start < ranges[j].Start })

	return &IPDB{ranges: ranges}, nil
}

// WithCache enables a small FIFO lookup cache. A size <= 0 disables caching.
func (db *IPDB) WithCache(size int) *IPDB {
	if db == nil || size <= 0 {
		return db
	}
	db.cacheSize = size
	db.cache = make(map[string]map[string]string, size)
	db.cacheKeys = make([]string, 0, size)
	return db
}

// Lookup finds the IP range that contains the given IP.
// Returns the attributes of the matching range, or nil if not found.
func (db *IPDB) Lookup(ipStr string) map[string]string {
	key := strings.TrimSpace(ipStr)
	if key == "" {
		return nil
	}
	if cached, ok := db.cacheGet(key); ok {
		return cached
	}

	ip, err := ipToUint32(key)
	if err != nil {
		return nil
	}

	r, ok := db.findRange(ip)
	if !ok {
		return nil
	}
	db.cacheSet(key, r.Attrs)
	return r.Attrs
}

func (db *IPDB) cacheGet(key string) (map[string]string, bool) {
	if db.cacheSize <= 0 {
		return nil, false
	}
	db.cacheMu.RLock()
	defer db.cacheMu.RUnlock()
	v, ok := db.cache[key]
	return v, ok
}

func (db *IPDB) cacheSet(key string, attrs map[string]string) {
	if db.cacheSize <= 0 || attrs == nil {
		return
	}
	db.cacheMu.Lock()
	defer db.cacheMu.Unlock()
	if _, ok := db.cache[key]; ok {
		return
	}
	if len(db.cacheKeys) >= db.cacheSize {
		oldest := db.cacheKeys[0]
		delete(db.cache, oldest)
		copy(db.cacheKeys, db.cacheKeys[1:])
		db.cacheKeys = db.cacheKeys[:len(db.cacheKeys)-1]
	}
	db.cache[key] = attrs
	db.cacheKeys = append(db.cacheKeys, key)
}

// findRange performs binary search for the range containing ip.
func (db *IPDB) findRange(ip uint32) (IPRange, bool) {
	if len(db.ranges) == 0 {
		return IPRange{}, false
	}

	// Binary search for the last range with Start <= ip
	lo, hi := 0, len(db.ranges)-1
	for lo <= hi {
		mid := (lo + hi) / 2
		if db.ranges[mid].Start <= ip {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}

	if hi < 0 {
		return IPRange{}, false
	}

	r := db.ranges[hi]
	if ip >= r.Start && ip <= r.End {
		return r, true
	}
	return IPRange{}, false
}

// ipToUint32 converts an IPv4 string to uint32.
func ipToUint32(ipStr string) (uint32, error) {
	ipStr = strings.TrimSpace(ipStr)

	// Try dotted-decimal
	ip := net.ParseIP(ipStr)
	if ip == nil {
		// Try plain integer
		n, err := strconv.ParseUint(ipStr, 10, 32)
		if err != nil {
			return 0, fmt.Errorf("invalid IP: %s", ipStr)
		}
		return uint32(n), nil
	}

	ip4 := ip.To4()
	if ip4 == nil {
		return 0, fmt.Errorf("not an IPv4 address: %s", ipStr)
	}

	return uint32(ip4[0])<<24 | uint32(ip4[1])<<16 | uint32(ip4[2])<<8 | uint32(ip4[3]), nil
}

// Count returns the number of IP ranges.
func (db *IPDB) Count() int {
	return len(db.ranges)
}
