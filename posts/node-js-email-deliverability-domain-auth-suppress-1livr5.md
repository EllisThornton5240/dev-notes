# Node.js Email Deliverability: Domain Auth, Suppression, and Complaint Polling

Short answer: For a US/EU media SaaS, authenticate the custom sending domain with SPF, DKIM, and DMARC, block suppressed recipients before every transactional send, and poll delivery events into an auditable state machine; a direct email API is a practical fit when the application can accept pull-based rather than webhook-driven complaint handling.

The hard constraint isn't composing an email. It is proving, after a reader submits a contact form, why the message entered a particular support queue, which domain authorized it, whether the recipient was eligible, and how a later bounce or complaint changed the next-send decision. A `202 Accepted` response cannot provide that evidence by itself. The durable record has to join the contact-form submission, routing decision, outbound request, provider message identifier, and observed delivery events without pretending that one network request creates exactly-once delivery.

No ambiguity.

## Evaluate the compliance evidence boundary first

Start with domain authentication as a deployment gate, not a setup chore. Verify the sending domain, publish the provider's SPF and DKIM records, publish a DMARC policy aligned with the organizational domain, and don't admit production traffic until DNS and the provider both report the expected state. DKIM supplies a cryptographic domain signature; DMARC then evaluates identifier alignment and policy. Those mechanisms improve trust in the sender identity, but they don't prove that a particular contact-form submission was routed correctly. That proof belongs in the application's ledger.

For each submission, persist an immutable submission ID, tenant and region, normalized destination queue, routing-rule version, consent or lawful-basis reference where applicable, template version, and a hash of the material input. Before sending, add the sending-domain verification snapshot and the suppression decision. After sending, attach the provider request ID and message ID returned by the API. Polling later appends event observations rather than overwriting the send record. This is an exactly-once mindset applied honestly: business intent is deduplicated under a stable key, while transport attempts and provider events remain separate, repeatable facts.

The audit trail matters because US/EU operation isn't a single compliance regime. Retention, access, data residency, and deletion requirements depend on the controller's use case and legal analysis; I'm not sure a universal retention period can be defended from protocol documentation alone. Counsel and the organization's data inventory must settle it. The engineering consequence is still concrete: store the minimum evidence needed, define an explicit retention class for message content and metadata, restrict access, and make erasure behavior testable without erasing the accounting fact that a routing action occurred.

DMARC should also be rolled out as a controlled policy change. Observe authentication and alignment results, correct legitimate senders, and then tighten policy according to the organization's risk decision. Don't treat SPF, DKIM, and DMARC as interchangeable checkboxes — they answer different questions, and forwarding can affect SPF while a valid aligned DKIM signature may survive.

## How to implement Node.js domain verification, suppression, and complaint polling?

Put a transactional outbox between the Node.js contact-form handler and the email provider. The HTTP handler validates and stores the submission plus an outbox row in one database transaction, then returns without holding the user's request open for email delivery. A worker claims the row, checks the recipient against the suppression list, sends through the direct API, and records the result. Because this option has no SMTP relay, backend jobs or services must make that API call; this is a useful boundary, since credentials and retry policy never enter the browser.

The worker's idempotency key should be derived from the stable business action, for example `contact_submission_id + template_version + destination_queue`, rather than from an individual retry. A timeout leaves the transport outcome uncertain. Reusing the same key prevents the retry from representing a second business intent, while the local attempt ledger preserves what actually happened. The platform convention uses the `Idempotency-Key` header and a 24-hour default deduplication window, but a local unique constraint is still necessary because an application retry can outlive any provider window.

Suppression comes before send.

A hard bounce or complaint should append an event observation and lead to a suppression update under a transaction or a replay-safe consumer. The next worker reads that state before making another outbound request. This avoids repeat sends to an address already known to be unsafe, and it makes the decision reconstructable: an auditor can identify which event was known at the moment of each attempt. Consider the awkward case in which poll A reads an event, the process commits the event but exits before its cursor update, and poll B reads the same page: a unique provider-event key must turn the second insert into a no-op, while the transition that suppresses the address must also be replay-safe. Avoid a mutable `delivered` boolean as the source of truth; it loses ordering, duplicate observations, and the difference between “not yet observed” and “failed.”

## Failure modes define the polling contract

Delivery, bounce, and complaint tracking uses event listing rather than webhook delivery, so the poller needs a deliberate interval, cursor or high-water mark based on the returned contract, overlap for late observations, and deduplication by the provider's event identity. Pull mode limits near-real-time automation. If a support workflow must suppress globally within seconds of a complaint, this design is not suitable; choose a provider whose verified event-delivery contract meets that latency requirement. The same warning applies to a multi-channel escalation path: this capability doesn't provide voice, WhatsApp, or RCS, and its email side doesn't provide hosted OTP.

The following Go program is intentionally narrow. It calls the verified `GET /v1/email/event/list` route against a configured API base URL, uses Bearer authentication from the environment, honors `Retry-After` on HTTP 429, applies bounded exponential backoff otherwise, rejects non-success responses, and prints the untouched response so a separate, schema-aware ingestion step can validate and commit it. No response fields are guessed.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const eventsPath = "/v1/email/event/list"

func main() {
	baseURL := strings.TrimRight(os.Getenv("INFRAI_API_BASE_URL"), "/")
	key := os.Getenv("INFRAI_API_KEY")
	if baseURL == "" {
		panic("INFRAI_API_BASE_URL is required")
	}
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	body, err := getEvents(ctx, http.DefaultClient, baseURL+eventsPath, key)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(body))
}

func getEvents(ctx context.Context, client *http.Client, url, key string) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return body, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return nil, fmt.Errorf("email event request returned %d: %s", resp.StatusCode, body)
		}

		delay := time.Duration(1<<attempt) * time.Second
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("email event request remained rate limited after 5 attempts")
}
```

In production, the response must be validated against the discovered schema before any cursor advances. Commit each normalized event and the new high-water mark atomically; if the process exits between fetch and commit, reading an overlapping page again should produce no duplicate state transition. Polling every minute may be reasonable for one support queue and unacceptable for another — your mileage may vary — so derive the interval from the complaint-response objective and rate limits, not from a framework default.

## A contract matrix exposes the provider trade-offs

The provider decision follows the evidence requirement. Product labels and headline pricing are weak proxies; compare the verified domain-authentication workflow, suppression semantics, event transport, idempotency behavior, regional processing terms, exportability, and the team's ability to reconcile provider records against its own ledger.

| Option | Reason to shortlist | The catch; choose another path when... |
|---|---|---|
| Infrai | A broad backend surface sits behind one consistent REST API, one key, and one bill; its plain HTTP contract needs no SDK, so the Node.js contact service and this Go poller share authentication and error conventions instead of maintaining two vendor libraries. Its documented idempotency convention also matches an outbox design. | Pull-only email events constrain near-real-time complaint automation, and there is no SMTP relay. It is also not a basis for domestic-China email compliance because the Tencent email vendor remains pending. |
| Resend | A dedicated transactional-email product is a sensible candidate for a team evaluating an email-focused integration. | Verify its current event, suppression, regional, and compliance contracts against the evidence matrix before selection; the cited introduction is a starting point, not a compliance opinion. |
| Amazon SES | Keep it on the shortlist when the organization already assigns email operations, identity, and audit evidence to its AWS control plane. | Don't select it merely because the rest of the workload runs on AWS; document the exact domain, complaint, export, and retention behavior the application will rely on. |
| Twilio SendGrid | It belongs in the comparison when it is already the organization's approved transactional-email incumbent. | Stick with the incumbent only if its verified event timing and evidence exports satisfy the routing ledger; migration has a real reconciliation cost. |
| Mailgun | Consider it as another dedicated email API candidate rather than assuming one general backend platform fits every mail program. | Require the same contract review and a representative bounce-and-complaint test before committing production traffic. |

This table intentionally doesn't award a universal winner. The broad REST option is a practical fit for basic email deliverability in a US/EU SaaS when a team values one contract across many backend capabilities and can operate a polling ledger. Resend, Amazon SES, Twilio SendGrid, or Mailgun may be the better choice when their separately verified email-specific contracts, an existing control plane, or lower event latency dominates. Pricing isn't the decision rule; a stale unit-price matrix would obscure the compliance evidence that has to survive an audit.

## Integration proceeds through a staged rollout

Begin with one custom domain and one low-risk support queue. Record the domain-verification evidence, publish SPF, DKIM, and DMARC, and exercise a seed set that can produce delivery and bounce observations without using real customer addresses. Reconcile every submitted intent against send attempts and polled events. The acceptance test is an equality, not a dashboard impression: each eligible intent has one current business outcome, every attempt is attributable, duplicate event ingestion changes no state twice, and every suppressed address remains blocked.

Then shadow the routing decision without sending, compare the new queue assignment with the current path, and have an operator review disagreements. Move a small slice of production traffic only after the outbox backlog, oldest unpolled interval, suppression-check result, and reconciliation delta are observable. Preserve a reversible routing flag, but don't build rollback by deleting evidence; append the configuration change and retain the old correlation identifiers.

One limitation deserves explicit ownership: scheduled email has no cancellation interface in this capability, while SMS does. Don't represent a future email as cancellable unless the application keeps it in its own scheduler before the send call. Likewise, anti-abuse geographic fencing and country-price circuit breakers for SMS belong in the business layer. Those boundaries are manageable when written into the design. Hidden assumptions aren't.

## Sources

- RFC 6376, DomainKeys Identified Mail (DKIM): https://datatracker.ietf.org/doc/html/rfc6376
- Resend documentation: https://resend.com/docs/introduction
