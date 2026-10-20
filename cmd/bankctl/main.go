// bankctl discovers a leader and retries ambiguous failures with the original
// transaction body. The server, rather than the client, deduplicates effects.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"quorumledger/internal/consensus"
	"quorumledger/internal/ledger"
)

func main() {
	nodes := flag.String("nodes", "http://127.0.0.1:8101,http://127.0.0.1:8102,http://127.0.0.1:8103", "comma-separated member URLs")
	file := flag.String("file", "-", "transaction JSON file, or - for stdin")
	timeout := flag.Duration("timeout", 20*time.Second, "overall retry deadline")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "PaxosVault: A Resilient Distributed Banking and Settlement System Using Multi-Paxos\n\nUsage: bankctl [flags] status|state|audit|submit\nFlags must precede the command.")
		flag.PrintDefaults()
	}
	flag.Parse()
	fail := func(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(2)
	}
	command := flag.Arg(0)
	if command != "status" && command != "state" && command != "audit" && command != "submit" {
		fail(fmt.Errorf("unknown command %q", command))
	}
	urls := strings.Split(*nodes, ",")
	for i := range urls {
		urls[i] = strings.TrimRight(strings.TrimSpace(urls[i]), "/")
	}
	client := &http.Client{Timeout: 2 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	request := func(method, url string, body []byte) ([]byte, int, error) {
		r, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
		if err != nil {
			return nil, 0, err
		}
		r.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(r)
		if err != nil {
			return nil, 0, err
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
		return b, resp.StatusCode, err
	}
	pretty := func(b []byte) {
		var out bytes.Buffer
		if json.Indent(&out, b, "", "  ") == nil {
			fmt.Println(out.String())
		} else {
			fmt.Println(string(b))
		}
	}
	if command == "status" {
		for _, u := range urls {
			b, _, err := request("GET", u+"/status", nil)
			if err != nil {
				fmt.Printf("%s: %v\n", u, err)
			} else {
				pretty(b)
			}
		}
		return
	}
	var body []byte
	if command == "submit" {
		var err error
		if *file == "-" {
			body, err = io.ReadAll(io.LimitReader(os.Stdin, 64<<10))
		} else {
			body, err = os.ReadFile(*file)
		}
		if err != nil {
			fail(err)
		}
		var c ledger.Command
		if err := json.Unmarshal(body, &c); err != nil {
			fail(err)
		}
		if err := c.Validate(true); err != nil {
			fail(err)
		}
	}
	for ctx.Err() == nil {
		for _, u := range urls {
			b, code, err := request("GET", u+"/status", nil)
			if err != nil || code != 200 {
				continue
			}
			var status consensus.Status
			if json.Unmarshal(b, &status) != nil || !status.Leader {
				continue
			}
			path, method := "/v1/"+command, "GET"
			if command == "submit" {
				path, method = "/v1/transactions", "POST"
			}
			b, code, err = request(method, u+path, body)
			if err != nil || code == 409 || code == 503 {
				continue
			}
			pretty(b)
			if code >= 400 {
				os.Exit(1)
			}
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	fail(fmt.Errorf("deadline exceeded; transaction outcome may be unknown; reuse the same transaction ID when retrying"))
}
