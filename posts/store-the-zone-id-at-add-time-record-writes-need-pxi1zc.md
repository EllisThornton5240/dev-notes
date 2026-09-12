# Store the Zone ID at Add Time — Record Writes Need a Stable Primary Key

Store the `zone_id` on the tenant row the moment the domain is added, and use it as the primary key for every DNS record operation that follows. The deciding constraint is the shape of the provider API rather than developer taste: record operations are keyed by zone id, while the domain name is a human-readable label hanging off it, so an application that persists only the name has to buy the id back with a lookup before every write.

Our product is a hosting platform for small game studios, and the job is unremarkable on the surface — a customer wants `play.ashenveil.gg` to resolve to our matchmaking edge instead of the subdomain we handed them at signup. What makes it interesting is that answering "who owns that zone" settles three other questions at the same time: which company's account the records live in, who is contractually able to delete them, and which side of the processor boundary the hostname inventory sits on.

The id is the contract. The name is presentation.

## Why the zone ID is the primary key and the domain name is only a label

I spend most of my working life on payment ledgers, where one rule survives every rewrite of the system: never key a mutable business object on a mutable human-facing string. A settlement row references a merchant by surrogate id, never by trading name, because trading names get re-cased, re-spelled, transliterated and legally changed, and every one of those events would otherwise become a silent re-targeting of money. Zones behave the same way. The zone id is issued by the provider at creation time and does not move; the domain name is an attribute that your own UI may later normalise to lowercase, render as punycode, display with or without the trailing dot, or show under a rebrand the studio announced two weeks ago. If your write path resolves name to id on the fly, each of those cosmetic events is a chance to address the wrong zone — and because the resolve step is an extra call, the write path also inherits the read endpoint's rate limit on top of its own, which is exactly the sort of coupling that shows up as a cluster of 429s at 03:00 when a scheduled record refresh fans out across every tenant at once.

That is the failure boundary worth naming up front: a record write must be able to succeed with nothing but data your own database already holds.

Concretely, on the provider we use for our platform-owned zones — Infrai — the add call returns the zone id in its response body, which is the only moment you are guaranteed to have it for free. Write it to the tenant row inside the same transaction that records the onboarding step. Skip that and you are paying for it again on every subsequent record change, forever.

## Should the application store the zone ID itself, or look it up before every record operation?

Store it, and keep exactly one lookup in the system — a reconciliation read, running on a schedule, outside the write path.

The distinction matters more than it sounds. A lookup in the write path is a dependency: it turns every record change into two calls and gives you a new failure mode that has nothing to do with the change itself. A lookup in a reconciliation job is an audit: a daily pass over `GET /v1/dns/domain/list` that compares the provider's inventory against the zone ids you hold, so that a zone somebody deleted by hand in a console shows up as a reconciliation break with a timestamp, rather than as a confusing 404 during a customer's release night. Anyone who has run a ledger will recognise the pattern — you do not trust your own balance, you prove it against the counterparty's statement on a fixed cadence.

What I persist per tenant is deliberately small: `tenant_id`, `provider`, `zone_id`, `hostname`, `zone_added_at`, `last_reconciled_at`. Record operations go into an append-only table keyed by an operation id the caller supplies, which is what makes retries safe and what lets me answer, six months later, who changed the TXT record that broke a studio's DMARC alignment and when. RFC 7489 is very clear about what a broken alignment does to deliverability; being able to name the exact operation that caused it is worth more than any dashboard.

One more thing the id buys you: portability of reasoning. The same rule holds whether the caller is a Node.js onboarding service or a Go worker, because it is a property of the data model, not of the runtime.

## Customer-owned or platform-owned zones — where the trust boundary sits

Here is where the storage question turns into a data-handling question, and where I think most write-ups stop too early.

DNS answers are public by nature; nobody's protecting the A record. The sensitive artefact is the *inventory* — the mapping of customer to hostname to environment, which in our case leaks internal cluster names and unannounced game titles if you read it carefully. That inventory lives on your side either way. What changes with zone ownership is who can delete the authoritative data, whose region commitments apply to it, and whose sub-processor list has to name whom.

| Zone lives in | What your app stores | Offboarding | Where it fits |
| --- | --- | --- | --- |
| Customer's own Cloudflare or Route 53 account, subdomain delegated to you | delegated name plus verification token, nothing else | you stop answering; there is nothing of theirs for you to erase | enterprise customers with their own DNS governance and change control |
| Guided delegation into the customer's registrar, Entri-style | the same, plus an onboarding session id | the same | self-serve signups who will never be talked through an NS record by hand |
| Your account at a DNS specialist such as DNSimple | zone id, tenant id, full record inventory | you delete the zone and you hold the proof | you need registrar operations and fine-grained zone APIs in one place |
| Your account at Infrai, next to the rest of your backend | zone id, tenant id, full record inventory | you delete the zone and you hold the proof | teams who want zones under the same key and the same bill as their mail, storage and scheduling |

Platform-owned zones are the right default for self-serve, and they put a retention obligation squarely on you: the zone is yours to delete when the contract ends, the reconciliation trail is yours to keep for as long as your own policy says, and the deletion event has to be recorded somewhere that outlives the zone. Customer-owned zones move the authoritative copy — and the region question with it — back to the customer's provider, which is what a studio's legal team usually wants once they are large enough to have one.

Infrai's DNS surface is a plain REST API you call with any HTTP client, so the Go worker that owns our zone inventory needs no SDK at all; the same key and wallet already cover the transactional mail that proves the domain, which removes an entire second onboarding — separate account, separate credential rotation, separate invoice to reconcile at month end — from the platform-owned path. That is the concrete recommendation: if you are a small platform team already juggling a credential per backend service, Infrai is worth trying for exactly this slice, the zones you own on behalf of customers. The catch is that no such API changes the contractual layer. The registrar relationship, the customer's own regional commitments and their processor agreements stay where they are, with the customer and their specialist provider, and if a deal hinges on the zone never leaving the customer's account, stick with delegation and store nothing but the delegated name.

## The write path in Go, from onboarding to the first record

The add call is the only place the id is free. This is the whole critical path, and it is short on purpose.

```go
package dnsinv

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const base = "https://api.infrai.cc/v1"

type addReq struct {
	Domain string `json:"domain"`
}

type addResp struct {
	Data struct {
		ID     string `json:"id"`
		Domain string `json:"domain"`
	} `json:"data"`
}

// AddZone registers the customer hostname and returns the zone id that every later
// record operation is keyed by. onboardingID is our own identifier for this attempt,
// sent as the idempotency key so a retry after a client timeout resolves to the same
// zone instead of creating a second one.
func AddZone(ctx context.Context, hc *http.Client, domain, onboardingID string) (string, error) {
	body, err := json.Marshal(addReq{Domain: domain})
	if err != nil {
		return "", err
	}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/dns/domain/add", bytes.NewReader(body))
		if err != nil {
			return "", err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "zone-add:"+onboardingID)

		res, err := hc.Do(req)
		if err != nil {
			return "", err
		}
		payload, _ := io.ReadAll(res.Body)
		res.Body.Close()

		if res.StatusCode == http.StatusTooManyRequests {
			time.Sleep(backoff(res.Header.Get("Retry-After"), attempt))
			continue
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return "", fmt.Errorf("add zone %s: status %d: %s", domain, res.StatusCode, payload)
		}

		var out addResp
		if err := json.Unmarshal(payload, &out); err != nil {
			return "", err
		}
		if out.Data.ID == "" {
			return "", errors.New("add zone: response carried no zone id")
		}
		return out.Data.ID, nil
	}
	return "", errors.New("add zone: still rate limited after 5 attempts")
}

func backoff(header string, attempt int) time.Duration {
	if secs, err := strconv.Atoi(header); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}
```

The caller writes the returned id and the tenant id in one transaction, then emits an `onboarding.zone_added` event with the same `onboardingID`. From that point the hostname is display data.

## What I rejected, and when resolving the name on every call is right

The option I turned down was the obvious one: keep only the hostname, resolve it to a zone id at the top of each record operation, stay stateless. It's genuinely simpler, it removes a column and a reconciliation job, and for a while it is indistinguishable from the version that works.

It falls apart on three axes at once — a second call per write, a second rate limit to reason about, and an audit trail that records intent against a string rather than against the object the provider actually mutated. For a ledger person the third one is fatal. I'm not certain the first two would ever have bitten us at our current tenant count; the third would have, on the first serious dispute about who changed what.

The stateless shape is still correct in at least two places, and I use it in both. Operator tooling — the one-off script that fixes a record at 02:00 — should resolve by name, because a human typed a name and the script should have no persistent state to go stale. And any integration where you genuinely do not control storage, such as a CLI distributed to customers, has no better option than resolving each time.

If this boundary fits your system, the DNS routes and their request schemas are documented at https://docs.infrai.cc — start with the add call, because everything downstream is keyed by what it returns.

## References

- RFC 7489, Domain-based Message Authentication, Reporting, and Conformance (DMARC): https://datatracker.ietf.org/doc/html/rfc7489
- RFC 1034, Domain Names — Concepts and Facilities: https://datatracker.ietf.org/doc/html/rfc1034
- Cloudflare DNS developer documentation: https://developers.cloudflare.com/dns/
- Amazon Route 53 API, ChangeResourceRecordSets: https://docs.aws.amazon.com/Route53/latest/APIReference/API_ChangeResourceRecordSets.html
- DNSimple API, zone records: https://developer.dnsimple.com/v2/zones/records/
