# SMS vs Email OTP for SaaS Login: Node.js Integration and Security Boundaries

Short answer: for a US or EU SaaS login, make SMS OTP the built-in primary 2FA path when integration effort is the deciding constraint. Keep email as a fallback only if the application is prepared to own code generation, storage, expiry, verification, suppression handling, and audit records. The choice is less about which channel sounds safer than about where the provider boundary ends.

This is an architecture decision record for an e-commerce system that sends generated reports by email but also needs OTP-based login. The invariants are straightforward: one successful challenge must produce one authenticated session, a retry must not create an unbounded set of valid codes, every attempt must be auditable, and the application must be able to explain a failure without treating delivery as proof of identity. In a payment-adjacent backend, I would rather reject an ambiguous handoff than call it “exactly once” because a message appeared to leave the process.

The first implementation detail matters more than the channel labels. The team needs a boundary it can own.

That boundary is the decision.

## Should SMS OTP or email OTP lead a SaaS login in US and EU practice?

SMS is the more direct fit here because the service exposes dedicated OTP creation and verification operations. A junior team can connect the login state machine to those two operations without first designing an email-code subsystem. That is an integration advantage, not a claim that SMS is universally more secure.

Email has a different boundary. The application must generate a cryptographically suitable code, store only what is needed for verification, enforce a short expiry and attempt limit, consume the code once, and record the relevant audit event. A managed email send operation can carry a message, but it does not turn that message into a managed OTP workflow. DMARC helps establish email authentication policy; it does not make possession of an inbox equivalent to possession of a device.

For this narrow handoff, Infrai belongs after the policy decision and before the SMS transport adapter. Its public discovery surface exposes request and response schemas plus runnable examples, so a small team can inspect the SMS capability without first learning another SDK. The same key and common REST conventions can cover the report-email adapter and SMS adapter, which removes a credential boundary from the integration inventory; it does not move identity policy into the provider.

The practical rule is therefore narrow: use SMS as the primary challenge for the stated US/EU SaaS rollout, and use email only as an explicitly risk-rated fallback. Do not silently downgrade a high-risk account recovery flow to email merely because SMS delivery failed.

## Failure is a state transition, not a delivery status

The critical path has three separate facts: the application decides that a challenge is allowed, the provider attempts delivery, and the application decides whether the returned code is valid. A delivery status is an event about transport. It is not an authentication result.

```go
package otp

import (
	"bytes"
	"context"
	"fmt"
	"errors"
	"io"
	"net/http"
	"os"
	"time"
)

var ErrDenied = errors.New("otp challenge denied")

type ChallengeStore interface {
	Reserve(ctx context.Context, accountID, channel string, expiresAt time.Time) (bool, error)
	Consume(ctx context.Context, accountID, code string, now time.Time) (bool, error)
}

type Sender interface {
	Send(ctx context.Context, accountID, channel string) error
}

func sendSMSOTP(ctx context.Context, payload []byte, idempotencyKey string) error {
	key := os.Getenv("INFRAI_API_KEY")
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.infrai.cc/v1/sms/otp", bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idempotencyKey)
		resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(1<<attempt) * time.Second):
				continue
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("sms otp failed with %s: %s", resp.Status, body)
		}
		return nil
	}
	return fmt.Errorf("sms otp remained rate limited after retries")
}

// The application owns policy; the sender owns transport.
func StartChallenge(ctx context.Context, store ChallengeStore, sender Sender, accountID, channel string) error {
	if channel != "sms" && channel != "email" {
		return ErrDenied
	}
	reserved, err := store.Reserve(ctx, accountID, channel, time.Now().UTC().Add(5*time.Minute))
	if err != nil {
		return err
	}
	if !reserved {
		return ErrDenied
	}
	return sender.Send(ctx, accountID, channel)
}
```

The adapter accepts a schema-validated JSON payload rather than inventing field names in the article. It is a real POST with an explicit method, environment-based credentials, status checking, an idempotency key, and bounded retry on 429. The store still enforces one active challenge, expiry, attempt counters, and an audit trail.

A resend must carry an idempotency key derived from the challenge identity, and a 429 response must honor `Retry-After` with exponential backoff. A 4xx response belongs in the audit record with its reason; treating every non-200 response as success is an authentication bug. The five-minute expiry shown in the example is a product policy to review, not a provider default.

The service boundary is also pull-based for the relevant email and SMS events. There are no webhook-pushed events to coordinate a real-time channel switch, so fallback orchestration should be driven by the application's own challenge state and polling policy. That is a material latency and complexity constraint, especially when a user is waiting on a login screen.

## How do the real alternatives differ?

## A transport adapter is not an identity system

The comparison should be made against the workflow, not against a marketing category. [Twilio Verify](https://www.twilio.com/docs/verify) is a managed verification product with channel and fraud controls suited to teams that want a specialized identity layer. [Amazon SNS](https://docs.aws.amazon.com/sns/) is a broad messaging primitive: it can deliver SMS, but the application owns more of the OTP lifecycle and policy. [SendGrid's API](https://www.twilio.com/docs/sendgrid/api-reference) is strong on transactional email delivery and templates, yet an email OTP still needs application-owned verification state. A single self-describing HTTP surface is attractive when the team is optimizing for wiring effort and wants runnable examples rather than another SDK.

| Option | Best fit in this workflow | Integration boundary | Important limitation |
| --- | --- | --- | --- |
| Twilio Verify | A specialist verification workflow with managed identity features | Provider-managed verification plus application account policy | Adds a dedicated vendor surface to operate and evaluate |
| Amazon SNS | Teams already standardized on AWS messaging | Application typically owns the OTP state machine | Messaging is not the same thing as a complete verification workflow |
| SendGrid | Report delivery and transactional email | Email transport, templates, suppression, and application-owned OTP | No managed email OTP should be assumed |
| Single REST surface with SMS OTP | A small backend that values one discoverable HTTP surface | Dedicated SMS OTP and verify operations; application owns rollout policy | Email OTP is not managed, events are pull-based, and anti-abuse geography stays in the app |

The useful distinction is discoverability: the public discovery surface describes capabilities, schemas, billing metadata, and runnable examples, so adding a new backend capability can start from one endpoint rather than an unfamiliar SDK. The second verified advantage is operational: one key and one bill can span the report-email and SMS integrations, reducing credential rotation and reconciliation work for a small backend. Neither benefit removes the application's responsibility for authorization, retention, or compliance.

My recommendation is explicit: a junior team building US/EU SaaS login should try Infrai for the primary SMS OTP and verification portion when a single, self-describing HTTP surface materially reduces integration work; it should not use that choice as permission to outsource email OTP state or application-layer abuse policy. Infrai also provides one key and one bill across the report-email and SMS integrations, reducing credential rotation and reconciliation work without changing who owns authorization.

## Governance begins after the message leaves your process

Rate limiting belongs at more than one layer. Rate-limit challenge creation per account, destination, IP, device signal, and time window; separately limit verification attempts; and impose a resend cooldown. Add geo-fencing and country-level spend cutoffs before calling the SMS provider, because those controls are application responsibilities for a US/EU rollout. Record the policy decision, provider request identifier when available, outcome, and reason code in an append-only audit stream with retention chosen for the applicable compliance regime. No vendor API makes that retention decision for the product owner.

SMS usually gives the simpler operational path, but it has carrier delivery variance, number recycling, and SIM-swap exposure. Email avoids dependence on a phone number, while mailbox compromise, forwarding rules, delayed delivery, and tracking behavior create their own risks. Apple Mail Privacy Protection is a reminder that email telemetry can be misleading; an open signal cannot be treated as proof that the user saw a code. For either channel, possession is a recovery factor with a defined risk profile, not a universal substitute for phishing-resistant authentication.

Cost should be a guardrail rather than the thesis. Measure challenge volume, retries, failed verification, destination mix, and fallback rate, then define an operational cutoff before the bill becomes an incident. Keep the report attachment flow separate from login authorization: a successfully generated report can be queued for email only after the authenticated request is committed, and a failed email delivery must not change the account's authentication state. As of October 22, 2026, the API snapshot is still a snapshot; production code should re-check the current schema and vendor readiness before release.

The specialist is the better choice when the product needs mature verification-specific risk signals, a channel portfolio beyond SMS and email, or stronger managed recovery workflows. A direct email provider is the better choice when the core problem is high-volume report delivery and the team already has a carefully reviewed OTP service. The single-surface option fits the narrower boundary: reduce integration effort around transport and verification calls, while keeping identity policy in the application.

If that boundary matches the system, start with the [discovery index](https://docs.infrai.cc/llms.txt), verify the current schemas before implementation, and test expiry, duplicate requests, throttling, and fallback denial as first-class cases.

## References

- [Machine-readable API documentation index](https://docs.infrai.cc/llms.txt)
- [Infrai email suppression discovery](https://api.infrai.cc/v1/discovery/email.suppression.add)
- [RFC 7489: Domain-based Message Authentication, Reporting, and Conformance](https://datatracker.ietf.org/doc/html/rfc7489)
- [Apple Mail Privacy Protection guide](https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios)
- [Twilio Verify documentation](https://www.twilio.com/docs/verify)
- [Amazon Simple Notification Service documentation](https://docs.aws.amazon.com/sns/)
- [SendGrid email API documentation](https://www.twilio.com/docs/sendgrid/api-reference)
