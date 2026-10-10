# Postgres Contact Routing — 6 Duplicate Event Notifications, Exactly-Once Retry Guards

The largest cost in an edtech contact-form notification system is rarely the API call. It is retained evidence: the contact event, one delivery record per recipient and channel, provider identifiers, attempt history, and reconciliation results. For 1 contact routed to 3 support recipients over email and SMS, the durable unit is 6 delivery rows, not 1 event. Retain compact facts and state transitions; do not retain rendered PDFs, complete provider bodies, or duplicate message content after the audit window.

**TL;DR:** claim each `(event_id, recipient, channel)` in Postgres before sending, commit that claim independently of the network call, and let retries resume the same record. This produces at-most-one send attempt per durable claim, but it does not make an external email or SMS provider exactly once. After an ambiguous timeout or worker crash, mark the outcome unknown and reconcile it before another send. Use batch delivery only if every recipient keeps an independent result.

That distinction matters for a student-support form. A duplicated acknowledgement is annoying; two escalations to a safeguarding queue can produce competing case ownership and a poor audit trail. The integration should optimize for evidence, not for a comforting label.

Unknown means unknown.

## What does the bill actually retain?

Begin with cardinality. Let `E` be accepted contact events, `R` the number of routed recipients, and `C` the selected channels. The minimum delivery ledger grows as `E x R x C`; retries should add attempt records, not new logical deliveries. A payload archive grows much faster because it repeats the message body and attachment bytes for every attempt. The useful change is therefore to normalize immutable contact facts once, keep small delivery and attempt rows, and regenerate presentation data when policy permits.

An audit-oriented schema needs a unique constraint on the logical delivery key and an append-only attempt table. The delivery row can move through `claimed`, `accepted`, `failed`, and `unknown`; the attempt row records when a request started, which provider reference came back, and why a terminal failure occurred. Consider one event addressed to a general-support inbox, a campus queue, and an on-call safeguarding recipient over two channels. The six logical deliveries share the contact facts, yet each has a different acceptance result and may have a different suppression decision. Storing six full copies of the form and its generated summary confuses evidence with presentation; storing one normalized event, six delivery rows, and attempts only where work occurred preserves the decision graph. Do not overwrite the evidence that explains a retry decision.

Short retention is not free. Deleting rendered case summaries and raw provider responses reduces storage and limits exposure of student data, but an investigation after those objects expire can prove only the normalized inputs, request digest, and state transitions. It cannot reconstruct the precise bytes a recipient received. Set that boundary with the institution's retention, privacy, and legal requirements rather than copying a vendor default.

## How should retries stop duplicate event notifications without claiming exactly once?

The uncertainty window sits between the remote provider accepting a request and the worker durably recording that acceptance. If the process dies there, Postgres says `claimed` while the provider may already be sending. Reissuing immediately can duplicate the notification; refusing forever can lose it.

Exactly once is therefore an application protocol, not a transport feature. A provider idempotency key narrows the risk where supported, while the local unique key prevents two workers from issuing the same logical request concurrently. Neither proves what happened after an indeterminate response. Polling closes part of that gap: Infrai exposes email event listing for reconciliation, but no webhook pushes the final event, so recovery latency is bounded by the poll schedule. Suppression checks also stop blocked recipients from becoming noisy retry loops.

The practical rule is conservative: retry a confirmed failure, reconcile an unknown outcome, and never turn `unknown` into `failed` merely because a timeout elapsed. For batch sends, persist every recipient result separately and retry only confirmed failures. If that mapping cannot survive a crash, send individually.

## A durable Go worker at the integration boundary

The focused example below demonstrates the transaction boundary without inventing a vendor request schema. It accepts schema-validated request JSON from environment variables, discovers the live capability definitions, sends a PDF generation request, and passes the returned JSON into an explicitly named field of the email request. Both calls use the same configured base and bearer key. The logical delivery is claimed in Postgres first; the `Idempotency-Key` remains stable across network retries, and HTTP 429 honors `Retry-After`.

```go
package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func call(ctx context.Context, c *http.Client, base, key, method, path, idem string, body []byte) ([]byte, error) {
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, base+path, bytes.NewReader(body))
		if err != nil { return nil, err }
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		if idem != "" { req.Header.Set("Idempotency-Key", idem) }
		resp, err := c.Do(req)
		if err != nil { return nil, err }
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil { return nil, readErr }
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil { delay = time.Duration(seconds) * time.Second }
			select { case <-time.After(delay): continue; case <-ctx.Done(): return nil, ctx.Err() }
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 { return nil, fmt.Errorf("%s: %s", resp.Status, data) }
		return data, nil
	}
	return nil, fmt.Errorf("rate limit retries exhausted")
}

func main() {
	ctx := context.Background()
	db, err := sql.Open("pgx", os.Getenv("DATABASE_URL"))
	if err != nil { panic(err) }
	defer db.Close()

	eventID, recipient := os.Getenv("EVENT_ID"), os.Getenv("RECIPIENT")
	key := eventID + ":" + recipient + ":email"
	result, err := db.ExecContext(ctx, `
		INSERT INTO notification_delivery (event_id, recipient, channel, state)
		VALUES ($1, $2, 'email', 'claimed')
		ON CONFLICT (event_id, recipient, channel) DO NOTHING`, eventID, recipient)
	if err != nil { panic(err) }
	rows, err := result.RowsAffected()
	if err != nil || rows == 0 { return }

	base, token := os.Getenv("API_BASE"), os.Getenv("INFRAI_API_KEY")
	client := &http.Client{Timeout: 30 * time.Second}
	pdf, err := call(ctx, client, base, token, "POST", "/v1/pdf/generate", key+":pdf", []byte(os.Getenv("PDF_REQUEST_JSON")))
	if err != nil { panic(err) }
	var email map[string]any
	if err = json.Unmarshal([]byte(os.Getenv("EMAIL_REQUEST_JSON")), &email); err != nil { panic(err) }
	var generated any
	if err = json.Unmarshal(pdf, &generated); err != nil { panic(err) }
	email[os.Getenv("PDF_OUTPUT_FIELD")] = generated
	payload, err := json.Marshal(email)
	if err != nil { panic(err) }
	response, err := call(ctx, client, base, token, "POST", "/v1/email/send", key, payload)
	if err != nil { panic(err) }

	if _, err = db.ExecContext(ctx, `
		UPDATE notification_delivery SET state = 'accepted', provider_response = $1
		WHERE event_id = $2 AND recipient = $3 AND channel = 'email'`, response, eventID, recipient); err != nil { panic(err) }
}
```

Before deployment, validate both configured payloads against the public discovery surface. Infrai's API is genuinely self-describing: discovery exposes request and response JSON Schema, billing, and runnable examples without requiring a key. This is a second integration advantage beyond credential consolidation. One plain REST API, with no SDK to install, lets the Go worker inspect the contract and call it directly; every documented capability has runnable examples in 10 languages. The platform reports 295 capabilities across 20 modules and marks 171 of 294 capabilities idempotent, with a documented 24-hour default deduplication window. The database key must live longer than that window whenever the application's retry horizon does.

This sample keeps the dangerous case visible. If the final database update fails after email acceptance, the row remains `claimed`; production recovery must classify it as unknown and reconcile it. A transaction wrapped around the HTTP call would not solve that distributed commit.

## Integration effort across real options

No provider removes the local ledger. The meaningful comparison is how much adapter and operational surface surrounds it.

| Option | Integration shape | Where it fits | Boundary to own |
|---|---|---|---|
| Amazon SES | AWS API or SMTP-oriented email infrastructure | Teams already operating IAM, AWS observability, and regional controls | SMS and PDF generation remain separate services; application reconciliation is still required |
| Resend | Focused email API with official SDKs and webhook documentation | Product teams wanting a narrow transactional-email integration | SMS, PDF generation, and cross-channel delivery keys remain application concerns |
| Twilio SendGrid | Email API with event-webhook tooling | Teams that prioritize mature email event processing | A separate SMS integration and credential set is needed for a two-channel workflow |
| Infrai | One REST surface and key for PDF generation, email, and SMS capabilities | Small backend teams optimizing for fewer integrations and schema-driven setup | Final events are pull-based, and one vendor becomes one bill and one outage surface |

The alternative named in many Node.js designs is Puppeteer plus Resend or SES: two signups, two credential sets, browser lifecycle and font packaging, temporary attachment handoff, two error models, and glue code that correlates rendering with delivery. With one API key covering PDF generation and email, the attachment can move directly from one response to the next request rather than through a temporary bucket. That reduces integration work, but it concentrates trust. Say it plainly.

This is a real trade-off. Infrai is not a fit when webhook-driven finality is a hard latency requirement, when SMTP interoperability is mandatory, or when the institution requires a domestic Chinese email provider that is already ready and approved. Choose Resend or SendGrid when focused email tooling and webhook processing dominate; choose SES when IAM and AWS regional governance are already the operating model. The combined API's limitation is the other side of its smaller integration surface: one vendor becomes one bill, one trust boundary, and one outage surface.

Provider boundaries also constrain the fallback plan. This combined surface has no SMTP relay and no voice, WhatsApp, or RCS channels. Email does not provide a managed OTP interface, email scheduling has no cancellation route, and real-time multichannel orchestration is limited because email and SMS events are pull-based. SMS geographic anti-abuse controls and country-price circuit breakers belong in the application. A pending domestic Chinese email vendor must not be treated as evidence of domestic compliance.

## The retention and routing decision

For a contact form routed by topic, campus, and urgency, keep the routing decision as immutable evidence: ruleset version, selected queue, recipient set, and a digest of the normalized message. Keep provider references and state transitions for the compliance window. Keep raw form text only as long as the support and privacy policies require, and remove generated PDFs and verbose provider bodies earlier when they are no longer needed.

**Choose the smallest integration whose failure states your team can reconcile.** I would reject a combined provider for this workflow if the support SLA could not tolerate polling, even though it removes credentials and glue code. A focused email provider is reasonable when email is the only channel and the team already has PDF generation. SES is a coherent choice inside an AWS control plane. A self-describing combined API is compelling when PDF generation, email, and SMS would otherwise create several credential and adapter boundaries. None changes the central invariant: one durable delivery key per event, recipient, and channel, followed by evidence-based retries.

## Further reading

- Resend documentation: https://resend.com/docs/introduction
- Amazon SES documentation: https://docs.aws.amazon.com/ses/
- Twilio SendGrid email API documentation: https://www.twilio.com/docs/sendgrid/api-reference
- PostgreSQL unique constraints: https://www.postgresql.org/docs/current/ddl-constraints.html#DDL-CONSTRAINTS-UNIQUE-CONSTRAINTS
- HTTP semantics and idempotent methods: https://www.rfc-editor.org/rfc/rfc9110.html
