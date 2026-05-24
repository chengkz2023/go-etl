package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

var (
	root        = flag.String("root", "examples/stress-test", "stress-test directory")
	dnsFiles    = flag.Int("dns-files", 20, "number of DNS files to generate")
	httpFiles   = flag.Int("http-files", 20, "number of HTTP files to generate")
	rowsPerFile = flag.Int("rows", 50000, "data rows per file")
	clean       = flag.Bool("clean", true, "clean watch/archive/dead directories and file_status.db before generating")
)

var (
	srcIPs = []string{
		"10.10.1.10",
		"10.20.2.20",
		"172.16.3.30",
		"192.168.1.20",
	}
	dstIPs = []string{
		"8.8.8.8",
		"1.1.1.1",
		"93.184.216.34",
		"203.0.113.10",
	}
	dnsTypes     = []string{"1", "2", "5", "15", "28"}
	responseCode = []string{"0", "0", "0", "3"}
	statusCodes  = []int{200, 200, 200, 204, 301, 404, 500}
	regions      = []string{"south", "east", "north", "west"}
	domains      = []string{"example.com", "openai.com", "mail.example.com", "api.service.local", "cdn.example.net"}
)

func main() {
	flag.Parse()

	if *rowsPerFile <= 0 || *dnsFiles < 0 || *httpFiles < 0 {
		fail("dns-files, http-files must be >= 0 and rows must be > 0")
	}

	if *clean {
		cleanPath(filepath.Join(*root, "watch"))
		cleanPath(filepath.Join(*root, "archive"))
		cleanPath(filepath.Join(*root, "dead"))
		cleanPath(filepath.Join(*root, "file_status.db"))
	}

	must(os.MkdirAll(filepath.Join(*root, "watch", "dns"), 0755))
	must(os.MkdirAll(filepath.Join(*root, "watch", "http"), 0755))

	start := time.Now()
	totalDNS := generateDNS()
	totalHTTP := generateHTTP()
	elapsed := time.Since(start)

	fmt.Printf("generated dns_rows=%d http_rows=%d total_rows=%d files=%d elapsed=%s\n",
		totalDNS,
		totalHTTP,
		totalDNS+totalHTTP,
		*dnsFiles+*httpFiles,
		elapsed.Round(time.Millisecond),
	)
}

func generateDNS() int64 {
	var total int64
	baseTime := time.Date(2026, 5, 24, 0, 0, 0, 0, time.Local)

	for fileIdx := 0; fileIdx < *dnsFiles; fileIdx++ {
		path := filepath.Join(*root, "watch", "dns", fmt.Sprintf("dns_stress_%05d.cdr", fileIdx))
		f, err := os.Create(path)
		must(err)
		w := bufio.NewWriterSize(f, 1024*1024)

		probeID := fmt.Sprintf("probe-dns-%03d", fileIdx%64)
		region := regions[fileIdx%len(regions)]
		writeLine(w, "%s|%s\n", probeID, region)

		for rowIdx := 0; rowIdx < *rowsPerFile; rowIdx++ {
			n := fileIdx**rowsPerFile + rowIdx
			ts := baseTime.Add(time.Duration(n) * time.Millisecond).Format("2006-01-02 15:04:05")
			writeLine(w, "%s|%s|%s|%s|%s|%s\n",
				ts,
				srcIPs[n%len(srcIPs)],
				dstIPs[(n/3)%len(dstIPs)],
				domains[n%len(domains)],
				dnsTypes[n%len(dnsTypes)],
				responseCode[n%len(responseCode)],
			)
		}

		must(w.Flush())
		must(f.Close())
		must(os.WriteFile(path+".ok", nil, 0644))
		total += int64(*rowsPerFile)
	}

	return total
}

func generateHTTP() int64 {
	var total int64
	baseTime := time.Date(2026, 5, 24, 0, 0, 0, 0, time.Local)

	for fileIdx := 0; fileIdx < *httpFiles; fileIdx++ {
		finalPath := filepath.Join(*root, "watch", "http", fmt.Sprintf("http_stress_%05d.log", fileIdx))
		tmpPath := finalPath + ".tmp"
		f, err := os.Create(tmpPath)
		must(err)
		w := bufio.NewWriterSize(f, 1024*1024)

		probeID := fmt.Sprintf("probe-http-%03d", fileIdx%64)
		region := regions[fileIdx%len(regions)]

		for rowIdx := 0; rowIdx < *rowsPerFile; rowIdx++ {
			n := fileIdx**rowsPerFile + rowIdx
			ts := baseTime.Add(time.Duration(n) * time.Millisecond).Format("2006-01-02 15:04:05")
			method := (n % 5) + 1
			writeLine(w, "%s|++|%s|++|%s|++|%d|++|/api/v1/resource/%d|++|%d|++|%d|++|%s|++|%s\n",
				ts,
				srcIPs[n%len(srcIPs)],
				dstIPs[(n/5)%len(dstIPs)],
				method,
				n%10000,
				statusCodes[n%len(statusCodes)],
				512+(n%4096),
				probeID,
				region,
			)
		}

		must(w.Flush())
		must(f.Close())
		must(os.Rename(tmpPath, finalPath))
		total += int64(*rowsPerFile)
	}

	return total
}

func writeLine(w *bufio.Writer, format string, args ...interface{}) {
	if _, err := fmt.Fprintf(w, format, args...); err != nil {
		fail("write file: %v", err)
	}
}

func cleanPath(path string) {
	if err := os.RemoveAll(path); err != nil {
		fail("clean %s: %v", path, err)
	}
}

func must(err error) {
	if err != nil {
		fail("%v", err)
	}
}

func fail(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
