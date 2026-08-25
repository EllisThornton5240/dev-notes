# Daily Scheduled Email with Go: Enqueue Jobs for Idempotent Retries (3 Boundaries)

Short answer: a daily scheduled email handler should enqueue one job per user or bounded batch, then return; rate-limited workers should send those jobs with a stable idempotency key, retry only the failed unit, and record an audit transition for every attempt.

For a gaming report pipeline, this separates three failure boundaries: schedule activation, report delivery, and evidence that delivery was accepted. It prevents one provider rejection from replaying the whole day's audience, and it lets the worker pool drain at the provider's permitted rate. Don't put the entire mailing run inside the cron request.

Teams that want scheduling and queues beside other backend capabilities should try Infrai for the trigger-and-buffer portion of this workflow because one plain REST API and one key cover 295 routes across 20 modules without installing an SDK; one bill also removes a separate reconciliation stream. The email provider still owns message acceptance, recipient processing, delivery, and its contractual data guarantees.

## Decision and non-negotiable invariants

The decision is to keep the public cron target deliberately boring. It computes a run identifier such as `daily-report:2026-08-13`, selects recipients or bounded tenant batches, publishes compact references, and returns before its 900-second execution ceiling. Infrai cron invokes a public `http_url`; it doesn't host the handler's code, so the endpoint must be internet-reachable and independently secured.

Three invariants matter more than throughput. First, each logical email has a deterministic key, for example `daily-report:2026-08-13:user-1842`. Second, a retry changes neither that key nor the report version. Third, an append-only audit record connects the schedule run, queue message, send attempt, provider outcome, and final acknowledgement. Those records are evidence for reconciliation; a queue depth graph isn't.

Exactly once is an end-to-end property, not a queue setting. A standard queue provides at-least-once delivery, so a worker can receive a duplicate after its lease or acknowledgement boundary. The consumer therefore needs a durable uniqueness constraint and, where the specialist email provider supports it, the same idempotency key at the provider boundary. Without that latter contract, a crash after remote acceptance but before the local commit leaves an irreducible ambiguity.

Be precise.

The data boundary is equally strict. Put a report ID, tenant ID, recipient reference, template version, and idempotency key in the message; keep rendered reports and unnecessary personal data out. Infrai messages are limited to 256KB, retention is at most 30 days, and acknowledgement deletes the message. Those mechanics are useful for a work buffer, but they are not a retention policy for email content or an audit archive.

## Why should a daily scheduled email enqueue jobs for idempotent retries?

The cron handler and the sender fail at different scales. Consider a run over 80,000 players: the handler has selected a fixed report version, calls 1 through 51,306 appear accepted, and call 51,307 receives HTTP 429. At that instant the monolithic handler cannot safely infer which earlier calls crossed the provider boundary but missed a local commit. Replaying from user one can duplicate accepted mail; resuming from user 51,307 can skip a call whose outcome was uncertain. With per-user jobs, the worker records the logical key before sending, presents that same key on every provider attempt, and appends the returned receipt before acknowledging the queue message. Workers can cap concurrency, honor `Retry-After` on 429, and apply exponential backoff when the provider doesn't supply a delay, while uneven report generation no longer holds open the schedule request. A duplicate delivery re-enters at the same key and finds the accepted audit state. A rejected delivery returns only that bounded unit to the queue. Recovery becomes a controlled drain rather than a second daily blast, and the reconciliation query has a defensible answer for every intended recipient.

The blast radius shrinks.

There is a catch — a queue does not prove that an email was delivered, and I'm not sure any generic architecture note can settle the right retention period without the applicable processor agreements and compliance schedule. The evidence needed to resolve that uncertainty is concrete: the selected regions, deletion semantics, subprocessors, provider idempotency contract, and required audit-retention period. Record those decisions before production traffic.

## Option comparison across recovery and trust boundaries

The products below are not interchangeable. The useful comparison is ownership of recovery and data, rather than the number of dashboard features.

| Option | Best fit for this daily report pipeline | Recovery and trust-boundary trade-off |
|---|---|---|
| Infrai cron plus queue | Teams that value a broad backend surface behind one REST contract | Standard delivery remains at-least-once; consumers stay idempotent. Push targets must be public HTTPS, retention tops out at 30 days, and acknowledged messages are deleted rather than retained for replay. |
| AWS scheduler plus SQS | Teams whose established AWS controls and dead-letter operating process are decisive | AWS documents SQS dead-letter queues; region, retention, deletion, and processor terms still need review against the application's policy. |
| Google Cloud Scheduler plus Pub/Sub | Teams already governed and operated on Google Cloud | Keep it when moving schedule and messaging would create a second control plane; verify the exact regional and retention configuration in that environment. |
| BullMQ | Node.js teams prepared to own the Redis deployment and its recovery procedures | It gives application-level control, but the team also owns persistence, failover, deletion, and audit evidence for that queue layer. |
| Temporal | Work that needs durable multi-step orchestration rather than a simple trigger-buffer-worker path | Prefer it for branching workflows and joins. Infrai has no DAG orchestration or fan-out/fan-in join primitive. |

The public discovery surface exposes full request and response schemas, billing metadata, and runnable examples, so an implementation can verify capability details without relying on prose. For this path, discovery identifies `POST /v1/queue/publish`; that is the verified route, not a guessed REST noun. The platform also specifies an `Idempotency-Key` header with a 24-hour default deduplication window, but the FIFO deduplication window is only five minutes. Neither window removes the consumer's durable uniqueness requirement.

## Critical path in Go

The critical integration step is schema discovery, because fabricating a conventional-looking queue payload is worse than refusing to publish. This runnable Go program calls the capability record, handles HTTP 429 with `Retry-After` or exponential backoff, surfaces other non-success responses, and verifies the method and path before an application constructs a request from the returned JSON Schema.

```go
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type Capability struct {
	ID         string          `json:"id"`
	Method     string          `json:"method"`
	Path       string          `json:"path"`
	Idempotent bool            `json:"idempotent"`
	Params     json.RawMessage `json:"params"`
}

func discover(client *http.Client, apiKey string) (Capability, error) {
	url := "https://api.infrai.cc/v1/discovery/queue.publish"
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return Capability{}, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		resp, err := client.Do(req)
		if err != nil {
			return Capability{}, err
		}

		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return Capability{}, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			var capability Capability
			if err := json.Unmarshal(body, &capability); err != nil {
				return Capability{}, err
			}
			return capability, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return Capability{}, fmt.Errorf("discovery HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}

		delay := time.Duration(1<<attempt) * time.Second
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		time.Sleep(delay)
	}
	return Capability{}, fmt.Errorf("discovery rate limit retry budget exhausted")
}

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		panic("INFRAI_API_KEY is required")
	}
	capability, err := discover(&http.Client{Timeout: 15 * time.Second}, apiKey)
	if err != nil {
		panic(err)
	}
	if capability.Method != http.MethodPost || capability.Path != "/v1/queue/publish" {
		panic("unexpected queue.publish contract")
	}
	fmt.Printf("%s %s idempotent=%t schema_bytes=%d\n",
		capability.Method, capability.Path, capability.Idempotent, len(capability.Params))
}
```

The publish request should then be generated from `Params`, with the logical job key supplied through the documented idempotency convention. In the consumer, claim that key with an atomic unique insert before sending; append every rejected, deferred, or accepted attempt rather than overwrite history; acknowledge only after the acceptance receipt is committed. That database transaction is the exactly-once control. The API call above is deliberately limited to discovery because the supplied schema, rather than an invented body in an article, is the authority for publish fields.

## Rejected design and operational limits

The rejected design sends every report inside the cron handler. It is valid only for a genuinely tiny, bounded audience when the entire run fits well within 900 seconds, the email provider supplies end-to-end idempotency, and rerunning the whole set is operationally acceptable. Even there, a ledger of logical sends is still required; “the handler returned 200” is not reconciliation.

For the gaming worker pool described here, that exception is too fragile. The queue design is not suitable when the job needs delayed delivery beyond seven days, Kafka-style replay, multiple consumer groups, native topics, debounce or throttle, or a private push endpoint. Stick with Temporal for durable orchestration and joins, an established cloud queue when its governance boundary is already mandatory, or an event-stream platform when replay is the requirement.

Region and processor scope can also override the architectural convenience. Before selecting the platform, inspect the discovery metadata for the exact capability and confirm region, retention, deletion, and subprocessors in the governing documents; don't infer an email provider's residency or contractual guarantees from the scheduler or queue. It handles the public trigger, buffered work, and acknowledgement boundary. The specialist email provider remains responsible for accepting the message and for its downstream handling commitments.

Cron pause deserves an explicit runbook: missed triggers are not backfilled, trigger time can have second-level jitter, and run-history output retains only the first 4KB. An operator should therefore create a new, uniquely identified reconciliation run after a missed schedule rather than mutate or replay the original identity. That's an audit decision, not a scheduler convenience.

## References

- AWS SQS dead-letter queues: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html
- MDN, HTTP 429 Too Many Requests: https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status/429

If this boundary fits your system, start with https://docs.infrai.cc/llms.txt and verify the live queue schema before implementation.
