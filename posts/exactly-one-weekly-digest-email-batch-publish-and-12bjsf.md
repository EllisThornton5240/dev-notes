# Exactly One Weekly Digest Email: Batch Publish and Rate-Limited Node.js Queue Workers

Pick the delivery guarantee before you pick the queue. For a weekly logistics digest — one email per active customer, summarizing the previous week's shipments, exception scans, and invoice adjustments — the guarantee that survives production is at-least-once delivery of batch-published messages plus a per-customer idempotency key enforced in your own database. Batch enqueue is an efficiency device for the publisher. Background workers still carry the whole burden of not emailing anyone twice, and no batch API can take that burden off them.

That order matters.

Most teams start at the other end. They pick a queue, read its retry semantics afterwards, and learn during the first deploy that restarts workers mid-run that "at-least-once" was a promise about redelivery, not about side effects. The mail provider has no idea the same digest already went out four minutes ago; SMTP has no dedupe layer; and the queue's own deduplication window, where one exists at all, is measured in minutes while your recovery scenarios are measured in hours.

## Start from the guarantee, not from the queue

The two failure modes are not symmetric, and the asymmetry is the whole design input. A missing digest produces a support ticket and an apology. A duplicated digest across an entire active-customer segment produces unsubscribes that are permanent, a spike in complaint rate that follows the sending domain around, and — for a logistics operator whose digest carries invoice adjustments — an argument about which of the two numbers the customer is supposed to believe.

So write the guarantee as a sentence someone can test: each active customer receives at most one digest per ISO week, a digest that has not left by the scheduled window is reported as missing rather than dropped, and every send is attributable to a row that names the customer, the week, the provider message id, and the process that made the transition.

That last clause is what turns a queue into something you can reconcile against, and it costs one durable write per item.

| What you actually implement | What it costs | Reasonable when |
|---|---|---|
| At-most-once: acknowledge, then send | Silent gaps whenever a worker dies between the two steps | Telemetry, cache warmers, anything reproducible next tick |
| At-least-once: send, then acknowledge | Duplicates on every redelivery, restart, and manual replay | The default for background jobs, if something downstream absorbs the duplicates |
| At-least-once plus a ledger claim | One extra durable write, plus a reconciliation job you have to operate | Customer-visible, money-adjacent mail such as this digest |

The third row is the one to implement here. The claim is a row insert under a unique key on `(customer_id, digest_week)`; the second delivery of the same message loses that race and acknowledges without contacting the provider.

## What batch enqueue buys, and what it quietly costs

Batch publishing is about publisher throughput and nothing else. Forty-two thousand individual publish calls at, say, 40 ms each is a half-hour of wall clock in a single-threaded loop; the same work as batches of 100 is a couple of minutes. The unit of work should still be one customer, because the smallest independently acknowledgeable item is the thing whose retry, audit row, and failure mode you can reason about.

One fat "send the weekly digest to everyone" message is the anti-pattern that keeps showing up in imports too. It hides partial success. When it dies at row 18,000 of a route-manifest import, the redelivered message either repeats 18,000 side effects or needs an internal checkpoint that duplicates, badly, what the queue already offers.

Two details decide whether batch publication is honest:

- Write publication intent before you publish. One row per intended customer-week, inserted in chunks, gives reconciliation a stable question later: which intended rows have no terminal ledger state? Queue depth cannot answer that. Queue depth cannot even distinguish "already processed" from "never published".
- Treat the batch response as a per-entry result set. Batch endpoints on managed queues report per-entry outcomes; a boolean read of the HTTP status will drop the entries the broker refused, and those entries will be missing forever with no error anywhere in your logs.

Keep the message small. Identifiers and a version pointer for the template, never rendered HTML, so the worker reads current suppression state at send time rather than shipping a week-old copy of it through the queue.

## How should a Node.js worker drain a queue of digest emails under a provider rate limit?

Put the rate limit in the worker, because only the worker knows the number. The queue enforces a delivery model; the ceiling belongs to whoever holds the provider contract, and it changes when the contract changes.

Library limiters differ on one detail that decides whether your configured number is real: some enforce the ceiling per worker process, others coordinate through a shared store so the queue as a whole is limited. With four replicas and a per-process limiter set to 10 messages per second, you are sending 40. Read which one you have before trusting the value, then set worker concurrency and replica count as one number rather than two.

On rejection, honor `Retry-After` when the provider sends it and fall back to exponential backoff with full jitter when it does not. Both are cheap. Neither is optional at 42,000 items, because a tight retry loop against a rate-limited endpoint turns one slow hour into a suspended account.

The worked example below is Go, and the state machine is the point rather than the language — the same three collaborators (ledger, suppression, provider) map onto a Node.js consumer, where the equivalent shape is an async handler that awaits the ledger claim before it awaits the provider call.

```go
// One work item: a single customer's digest for one ISO week.
type Digest struct {
	CustomerID int64
	Week       string // "2026-W32"
	AckID      string
}

// Claim inserts (customer_id, digest_week) under a unique key and reports
// whether this delivery won the race. Everything else is an audit transition.
type Ledger interface {
	Claim(Digest) (bool, error)
	MarkSent(Digest, string) error
	MarkAmbiguous(Digest, error) error
	Release(Digest) error
}

type rejected struct {
	status int
	after  time.Duration
}

func (r *rejected) Error() string { return fmt.Sprintf("provider rejected: %d", r.status) }

// Deliver reports whether the queue message may be acknowledged.
func Deliver(d Digest, led Ledger, suppressed func(int64) (bool, error),
	send func(Digest) (string, error)) (bool, error) {

	skip, err := suppressed(d.CustomerID) // consent state, read at send time
	if err != nil {
		return false, err
	}
	if skip {
		return true, nil
	}

	claimed, err := led.Claim(d)
	if err != nil {
		return false, err
	}
	if !claimed {
		return true, nil // an earlier delivery already owns this customer-week
	}

	providerID, err := send(d)
	if err == nil {
		return true, led.MarkSent(d, providerID)
	}

	var r *rejected
	if errors.As(err, &r) && r.status == 429 {
		// Nothing left the building: release the claim and let the message retry.
		return false, errors.Join(led.Release(d), err)
	}

	// Ambiguous: the request left the process and no provider id came back.
	// Hold the claim, record the gap, resolve it against the provider's log.
	return false, errors.Join(led.MarkAmbiguous(d, err), err)
}

// Full jitter, capped at 30s, deferring to Retry-After when the provider sent one.
func backoff(attempt int, retryAfter time.Duration) time.Duration {
	if retryAfter > 0 {
		return retryAfter
	}
	d := math.Min(30_000, 200*math.Pow(2, float64(attempt)))
	return time.Duration(rand.Int63n(int64(d))) * time.Millisecond
}
```

The ambiguous branch is the one that pays for itself. A timeout after the provider accepted the message is indistinguishable, from inside the process, from a timeout before it accepted anything — so the code refuses to guess, keeps the claim, and leaves a row for a human or a provider-side lookup. Releasing the claim there would be a decision to send twice.

## Reconciling 42,000 intended sends against what actually left

Reconciliation needs four counts per run, and they are only useful when every one of them comes from a different system: intended rows written by the publisher, entries the broker accepted, terminal ledger states written by workers, and messages acknowledged. Equality proves nothing on its own — two bugs can cancel — but every inequality points at exactly one boundary, and that is far more than "the queue drained" tells you. Intended above accepted means the publisher lost entries in a partial batch. Accepted above terminal means work is in flight, poisoned, or silently discarded after exhausting retries, and the dead-letter destination will say which. Terminal above acknowledged means workers are dying between the durable write and the ack, which is safe by construction here but expensive, since every one of those messages comes back and re-reads the ledger.

```sql
create table digest_send (
  customer_id  bigint      not null,
  digest_week  text        not null,
  state        text        not null check (state in ('claimed','sent','ambiguous','suppressed')),
  provider_id  text,
  attempted_at timestamptz not null default now(),
  primary key (customer_id, digest_week)
);
```

Two constraints that come from outside engineering belong in the same table. Commercial email in the United States has to honor an opt-out within 10 business days under the CAN-SPAM Act, and the opt-out mechanism has to keep working for at least 30 days after the message goes out — which is why suppression is read by the worker at send time and recorded as a terminal state, not filtered once at publish time and forgotten. The second constraint is retention. A queue's message retention is an operational window, usually days; the evidence a dispute needs lives in the ledger, under whatever retention your contracts and regulators require. I'd rather over-record here than reconstruct intent from broker metrics.

## Rolling this out, and when this is the wrong tool

Ship it in the order that keeps the failure cheap. Write intent rows for the full segment first with publication disabled, and check the cardinality against the source query. Then enable publication for one region, with the worker's provider call replaced by a no-op that still writes the ledger transition. Then let it actually send to a cohort — a few hundred accounts, not the whole book — and run the four-count reconciliation manually before it becomes a scheduled job.

Trigger it with cron, but keep cron on the trigger side. Managed platform cron invokes an HTTP endpoint on a schedule and the handler inherits that platform's function timeout, so the handler should write intent, publish batches, and return. A cron that tries to send 42,000 emails inline is a job that dies halfway with no record of where.

The catch is that this design is only worth its weight above a certain size. Under a thousand recipients, a single scheduled script with a unique constraint and a retry loop does the same job with a tenth of the operational surface, and adding a broker there buys queue dashboards nobody reads. Stick with the simple version until the send window stops fitting in one invocation.

Two other boundaries are worth naming. Queue-per-item is not a good fit when the work is a multi-step process with joins and compensation — durable workflow engines such as Temporal model that as replayable state with its own history, which is a heavier abstraction to operate but the honest one when steps depend on each other. And this design deliberately doesn't support strict ordering across customers; if a downstream system requires per-key sequencing, that is a FIFO-with-message-group problem, and retrofitting order onto a fan-out of independent sends won't get you there.

Everything above assumes duplicate suppression is worth one write per item. For a marketing blast, maybe it isn't. For a digest carrying invoice adjustments, I'm not sure I'd sign off on anything less.

## References

- [CAN-SPAM Act: A Compliance Guide for Business (FTC)](https://www.ftc.gov/business-guidance/resources/can-spam-act-compliance-guide-business)
- [RFC 6585 §4: 429 Too Many Requests](https://www.rfc-editor.org/rfc/rfc6585#section-4)
- [RFC 9110 §10.2.3: Retry-After](https://www.rfc-editor.org/rfc/rfc9110#field.retry-after)
- [Exponential backoff (Wikipedia)](https://en.wikipedia.org/wiki/Exponential_backoff)
- [The Idempotency-Key HTTP Header Field (IETF draft)](https://datatracker.ietf.org/doc/html/draft-ietf-httpapi-idempotency-key-header)
- [PostgreSQL: INSERT ... ON CONFLICT](https://www.postgresql.org/docs/current/sql-insert.html)
- [Vercel Cron Jobs documentation](https://vercel.com/docs/cron-jobs)
