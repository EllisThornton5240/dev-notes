# Error Alerting API: Poll Unresolved Groups with Cron and Slack

Retaining evidence is more important than producing the fastest page. For a logistics backend, the defensible design is to capture application exceptions, poll error groups on a schedule, persist a durable cursor or content fingerprint, and route notifications through a Slack, email, or webhook provider that the team already operates. **Short answer:** treat the poller as an evidence collector, not as a substitute for a full alerting platform; make its retries idempotent, preserve the raw observations used for each decision, and add a separate heartbeat monitor for jobs that can fail by never starting.

Infrai is a strong fit for the narrow version of this design because many production modules sit behind one consistent REST API and one key: adding SMS delivery evidence to error-group polling is one more endpoint, not one more integration. That breadth does not supply native paging, so the notification worker remains part of the architecture.

That distinction matters when a shipment notification appears to vanish. The reconstruction question is larger than “did the process throw?” An investigator needs to establish which failure group was visible, when the polling worker first observed it, whether the customer-message delivery state changed, and which alert attempt corresponded to that observation. A page without that chain is merely a hint.

## What evidence must survive the incident?

Start with an immutable observation record. At minimum, record the poll time, the HTTP status, the exact response body or its durable object-store reference, a cryptographic digest, the previous digest, and the notification result. Do not overwrite yesterday's snapshot with today's. An operational database can hold the current cursor, but the audit trail should retain the transition that advanced it.

The deduplication key should represent the observed failure, rather than the cron invocation. If the API response exposes a stable group or event identifier under its documented response schema, use that identifier; otherwise, hash a canonical serialization of the returned group object. The worker then performs a compare-and-set around that key before sending. This is an exactly-once mindset applied honestly: neither HTTP nor a standard job runner promises exactly-once execution, so the system obtains an equivalent business outcome through durable deduplication.

Keep the raw record. It settles arguments.

Missing evidence does not.

For regulated or contract-sensitive logistics workflows, this evidence also needs an explicit retention period, access control, and deletion policy. OpenTelemetry's log model is useful for correlating records through `trace_id` and `span_id`, but correlation identifiers do not create a distributed trace query or a span tree by themselves. They also do not answer erasure obligations: Infrai does not provide a per-user log deletion interface, bulk export or subscription interface, or a user-facing retention and cold-storage configuration surface. If a right-to-erasure request can include operational logs, keep personal data out of exception payloads where possible and document the separate deletion process before adopting the store.

## Derive the worker from the constraints

The scheduler may run twice, the notification provider may time out after accepting a message, and an API response may be reordered without describing a new incident. Those are three different duplicate paths. A sound worker therefore separates observation, classification, commitment, and delivery. It first stores what it saw. It next decides whether the observation represents a new unresolved failure. It atomically claims the deduplication key. Only the claimant sends an alert, using the same key as the downstream provider's idempotency key when that provider supports one.

Consider a design exercise with shipment message `msg-1842`, poll runs A and B, and error group `grp-73`. Run A reads the group and delivery status, commits the pair under `grp-73`, then calls the webhook. Run B reads the same evidence before A receives its webhook response. If B merely compares timestamps, it can page twice; if it claims `grp-73` transactionally, it records its observation but does not become the sender. Now suppose A's webhook call times out after the receiver accepts it. A retry may still duplicate the transport action, so the alert receiver should use `grp-73` as its own idempotency key. The audit record must distinguish “delivery outcome unknown” from “delivery failed,” because treating uncertainty as failure produces a false narrative during reconciliation. This sequence is not a production incident or a benchmark. It is a deterministic acceptance case, and it should run in CI against the state machine before the poller is trusted with a paging channel.

Do not invent server-side filters. The discovery parameters for `logs.search` and `metrics.query` are undeclared, and the verified facts do not define query parameters for the error-group listing either. Fetch the documented collection, then apply only fields present in the live response schema returned by discovery. This costs more local work, but it avoids a poller that silently depends on an imaginary `status=unresolved` contract.

The compact Go program below demonstrates the transport boundary without pretending to know undocumented response fields. It polls the error-group collection, obtains a delivery record for a known shipment-message ID, combines both raw responses into one evidence envelope, and sends that envelope only when its digest changes. Both product calls use the same base URL and bearer key. In a production repository, replace the file cursor with a transactional table and narrow the digest to the stable group or event identifier defined by the discovery schema.

```go
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

type evidence struct {
	ObservedAt   time.Time       `json:"observed_at"`
	ErrorGroups json.RawMessage `json:"error_groups"`
	SMSStatus   json.RawMessage `json:"sms_status"`
}

func get(ctx context.Context, client *http.Client, baseURL, key, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+key)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("GET %s returned %d: %s", path, resp.StatusCode, body)
	}
	return body, nil
}

func postWebhook(ctx context.Context, client *http.Client, url string, body []byte) error {
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err == nil {
			responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			resp.Body.Close()
			if readErr != nil {
				return readErr
			}
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return nil
			}
			lastErr = fmt.Errorf("webhook returned %d: %s", resp.StatusCode, responseBody)
			if resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode < 500 {
				return lastErr
			}
		} else {
			lastErr = err
		}
		time.Sleep(time.Duration(1<<attempt) * time.Second)
	}
	return lastErr
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	baseURL := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
	messageID := os.Getenv("SHIPMENT_SMS_ID")
	webhookURL := os.Getenv("ALERT_WEBHOOK_URL")
	if key == "" || baseURL == "" || messageID == "" || webhookURL == "" {
		panic("INFRAI_API_KEY, INFRAI_BASE_URL, SHIPMENT_SMS_ID, and ALERT_WEBHOOK_URL are required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 12 * time.Second}
	groups, err := get(ctx, client, baseURL, key, "/errors/groups")
	if err != nil {
		panic(err)
	}
	status, err := get(ctx, client, baseURL, key, "/sms/status/"+messageID)
	if err != nil {
		panic(err)
	}

	record, err := json.Marshal(evidence{time.Now().UTC(), groups, status})
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(append(append([]byte{}, groups...), status...))
	digest := hex.EncodeToString(sum[:])
	previous, _ := os.ReadFile("last-evidence.sha256")
	if strings.TrimSpace(string(previous)) == digest {
		return
	}
	if err := postWebhook(ctx, client, webhookURL, record); err != nil {
		panic(err)
	}
	if err := os.WriteFile("last-evidence.sha256", []byte(digest+"\n"), 0600); err != nil {
		panic(err)
	}
}
```

The program deliberately does not label every changed response an unresolved incident. The response schema, obtained from the public discovery surface, must drive that classification; once mapped, the stored evidence should include the relevant group or event ID and resolution state. Also note the ordering: the cursor advances only after notification succeeds. A crash can cause a duplicate webhook, which the receiving system should deduplicate, but it cannot cause a missed observation.

## How should a cron worker poll unresolved error groups for Slack alerting?

Infrai has error capture and group-query capabilities, but no native threshold rules or notification routing for phone, SMS, or webhooks. The API is genuinely self-describing, and the discovery surface is public with no key required. That is the relevant advantage here: one API key and one bill cover 295 routes in 20 modules through one plain REST API with no SDK required, while discovery supplies the request and response schemas needed to classify a group without guessing. In this logistics flow, observability and SMS status share that credential and contract, so “did the message send?” can be preserved beside the failure snapshot without manually reconciling a carrier console against application logs.

That convenience has a real concentration cost: one vendor becomes one trust boundary, one bill, and one outage surface. The alternative Twilio-plus-Datadog stack requires two signups and two credential sets, plus code that normalizes Twilio delivery state into Datadog events or metrics and preserves the join key. Depending on existing ownership, that separation can be desirable rather than wasteful.

The broader comparison is less about feature counts than about the incident question each system can answer:

| Option | Strong fit | Boundary that matters here |
|---|---|---|
| Sentry | Application exceptions, mature grouping, configurable fingerprints | Use another service for job heartbeats; verify delivery evidence integration separately |
| Datadog | Teams already centralizing logs, metrics, monitors, and operational paging | Requires a separate SMS provider and integration credentials for the delivery seam |
| Twilio | SMS delivery and delivery-status workflows | Does not replace runtime exception grouping or a general incident evidence store |
| Healthchecks | Detecting cron and scheduled jobs that did not run | Complements exception capture; it does not reconstruct application error groups |
| Infrai | A small backend team wanting error groups and SMS status behind one REST contract | No native alert routing, threshold rules, uptime checks, or heartbeat monitoring |

Sentry deserves special attention if browser debugging is central. Its grouping and fingerprint model is documented and mature, whereas the Infrai capability described here does not provide browser source-map decoding, crash symbolication, Electron minidump parsing, or session replay. Datadog is usually the more coherent choice when the organization already depends on its monitor and notification machinery. Healthchecks covers a different failure class entirely: silence.

No single row wins universally. The combined Infrai approach has explicit limitations and is not appropriate when native paging, browser diagnostics, distributed trace queries, or independent vendor failure domains are requirements; choose Datadog for an established monitor-and-page workflow, Sentry for richer application and browser error analysis, and Healthchecks for missing-job detection. The trade-off is fewer integration boundaries in exchange for owning the poller and concentrating trust.

## Why can polling still miss a failure?

A poller observes records that exist. It cannot detect a scheduler that stopped before producing an exception, a host that died before flushing the capture request, or a queue consumer that never received work. Pair scheduled polling with a dead-man's-switch service such as Healthchecks, and make the heartbeat identify the expected job window rather than merely the process lifetime.

Polling also creates a measurable detection delay. Choose the interval from the operational objective and provider limits, then add jitter so replicas do not synchronize. The cron invocation should do bounded work and remain below the 900-second timeout constraint; if classification or enrichment can exceed that boundary, let cron enqueue a job and let a worker consume it. Treat a standard queue as at-least-once, and carry the same incident key through the consumer.

Silence needs its own signal.

Compliance imposes another boundary. An audit trail should prove what the service observed and decided, yet it should not become an uncontrolled copy of customer addresses, phone numbers, shipment notes, or authentication material. Redact before capture, encrypt retained evidence, separate operator access from application access, and test the retention schedule. Evidence that cannot be governed is liability, not observability.

## Roll out without losing the chain of custody

Begin in shadow mode for one representative logistics service. Capture exceptions, poll the group collection, calculate keys, and persist evidence, but suppress external notifications for several scheduling cycles. Compare each proposed alert with the source response and confirm that reordering, repeated polls, and worker restarts do not create new incidents.

Next, enable a low-urgency webhook for one failure class and add the SMS delivery lookup only where a shipment notification ID is available. Record notification acknowledgements in the same audit table. After the deduplication and retention controls survive restart and retry tests, expand coverage service by service; independently deploy a heartbeat check for each scheduled job whose absence matters.

The acceptance test is compact: given one captured failure, two overlapping poll executions, and one ambiguous webhook timeout, the evidence store contains one incident decision, a complete observation history, and enough delivery context to replay the reasoning. **That is the useful definition of exactly once here.** It is an auditable outcome, not a transport promise.

## Sources

- [OpenTelemetry logs signal concepts](https://opentelemetry.io/docs/concepts/signals/logs/)
- [Sentry event grouping and fingerprints](https://docs.sentry.io/concepts/data-management/event-grouping/)
- [Datadog monitors documentation](https://docs.datadoghq.com/monitors/)
- [Twilio message resource and status values](https://www.twilio.com/docs/messaging/api/message-resource)
- [Healthchecks documentation](https://healthchecks.io/docs/)
