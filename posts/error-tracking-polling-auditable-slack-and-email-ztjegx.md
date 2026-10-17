# Error Tracking Polling: Auditable Slack and Email Alerts for Import Jobs

Short answer: poll the error tracker on a fixed schedule, but treat every poll as a reconciliation run rather than a search followed immediately by a notification. Persist a compound watermark, normalize unresolved groups into stable incident keys, record each Slack and email delivery attempt, and advance state only after the evidence needed to reconstruct the decision is durable. For scheduled media imports, error state must be joined with an independent last-success signal because a job can stop producing results without emitting a new exception.

This architecture decision covers catalog imports expected to produce committed results on schedule. The tracker supplies unresolved groups and timestamps through an authenticated API; notification channels only carry messages. They never own incident truth.

## How should error tracking polling drive Slack and email alerts?

Three invariants govern the design. Replaying an observation must not create another incident or another channel delivery under the same policy. An operator must be able to recover which records were observed, why the rule fired, and whether delivery was attempted. Finally, a successful tracker request proves only that the tracker answered, not that an importer produced useful output.

That last boundary matters in media ingestion. A source may have no new items, an importer may stall before its error handler, or a downstream write may fail after a source cursor moves. Only some paths create unresolved errors. The alert rule therefore considers both recent unresolved groups and the age of a durable completion record written after the imported result and source cursor commit together.

Keep failure boundaries explicit. A tracker timeout leaves the watermark untouched. A partial page is not a complete reconciliation. A Slack timeout does not permit an email retry to change incident identity, and successful delivery does not resolve the upstream condition.

No signal is perfect.

Retries expose the truth.

## Decision and alternatives

Use a stateful reconciler with a transactional outbox. One transaction upserts observations, evaluates freshness, inserts channel-specific intents under unique keys, and moves the watermark. Separate workers deliver those intents. Duplicate cron invocations are harmless because database uniqueness, rather than scheduling luck, controls duplication.

This choice has real limitations and trade-offs. It adds a durable store, schema migrations, leases, retention work, and an on-call obligation for the outbox itself; teams that cannot operate those parts should prefer a managed alerting facility or a deliberately stateless check when its weaker guarantees are acceptable. The outbox also cannot guarantee exactly-once presentation when a destination accepts a message and the worker loses the acknowledgment. Its benefit is narrower and more defensible: it preserves enough evidence to identify that ambiguous interval, retry according to policy, and explain a possible duplicate afterward.

| Option | Duplicate control | Boundary risk | Reconstruction | Appropriate use |
| --- | --- | --- | --- | --- |
| Stateless time window | Best-effort suppression | High | Weak | Temporary, low-consequence checks |
| Watermark plus direct sends | Stable query progress | Medium | Partial | One channel where duplicates are tolerable |
| Reconciler plus outbox | Unique incident and intent keys | Low | Strong | Accountable scheduled workflows |

The watermark should be `(updated_at, group_id)`, not a timestamp alone. Equal timestamps occur; moving forward on time without a deterministic tie-breaker can skip a group. Repeat the boundary inclusively, sort deterministically, and let observation uniqueness absorb overlap. A lookback may cover indexing delay, but it is not the primary correctness mechanism.

Construct incident identity from source, job, normalized error group, and policy revision. Never include `now`, request IDs, or rendered message text. A silence condition needs a stable synthetic group such as `no_committed_result`; otherwise each poll invents a new incident.

## Critical reconciliation path

This Go sketch makes the inclusive cursor, stable identity, completeness check, and atomic commit visible. Storage and HTTP details remain behind interfaces so they can be tested independently.

```go
package alerting

import (
	"context"
	"fmt"
	"time"
)

type Cursor struct { UpdatedAt time.Time; GroupID string }
type Group struct {
	ID, JobID, SourceID string
	UpdatedAt time.Time
	Resolved bool
}
type Snapshot struct {
	Groups []Group
	LastCommitted time.Time
	SourceID, JobID string
	Next Cursor
	Complete bool
}
type Intent struct { Key, Incident, Channel string; CreatedAt time.Time }

type Reader interface {
	Read(context.Context, Cursor) (Snapshot, error)
}
type Store interface {
	Cursor(context.Context) (Cursor, error)
	Commit(context.Context, Snapshot, []Intent) error
}

type Reconciler struct {
	Reader Reader
	Store Store
	SilenceLimit time.Duration
	PolicyVersion string
	Now func() time.Time
}

func (r Reconciler) Run(ctx context.Context) error {
	cursor, err := r.Store.Cursor(ctx)
	if err != nil { return fmt.Errorf("load cursor: %w", err) }

	s, err := r.Reader.Read(ctx, cursor)
	if err != nil { return fmt.Errorf("read snapshot: %w", err) }
	if !s.Complete { return fmt.Errorf("refuse partial snapshot") }

	now := r.Now().UTC()
	intents := make([]Intent, 0)
	for _, g := range s.Groups {
		if g.Resolved { continue }
		id := fmt.Sprintf("%s:%s:%s:%s", g.SourceID, g.JobID, g.ID, r.PolicyVersion)
		intents = append(intents, channelIntents(id, now)...)
	}
	if now.Sub(s.LastCommitted) >= r.SilenceLimit {
		id := fmt.Sprintf("%s:%s:no_committed_result:%s", s.SourceID, s.JobID, r.PolicyVersion)
		intents = append(intents, channelIntents(id, now)...)
	}

	// Commit atomically inserts unique intents and advances the cursor.
	if err := r.Store.Commit(ctx, s, intents); err != nil {
		return fmt.Errorf("commit reconciliation: %w", err)
	}
	return nil
}

func channelIntents(id string, at time.Time) []Intent {
	result := make([]Intent, 0, 2)
	for _, channel := range []string{"slack", "email"} {
		result = append(result, Intent{
			Key: id + ":" + channel, Incident: id, Channel: channel, CreatedAt: at,
		})
	}
	return result
}
```

Enforce uniqueness on `Intent.Key` in storage; an in-memory check fails when cron invocations overlap. A worker claims an intent with a lease, sends it, and appends an attempt containing start time, completion time, outcome class, and any returned message identifier. It marks delivery complete only after success. If it crashes after the destination accepts a message but before local acknowledgment commits, a retry may duplicate the message unless the destination honors an idempotency key. Record that uncertainty. Do not call at-least-once delivery exactly once.

Message content should come from stored evidence: media source, import job, trigger type, and first and latest observation times. Raw stack traces, credentials, source payloads, and audience data do not belong in chat or email.

## Reconstruction, testing, and limits

An incident timeline should be reproducible from append-only facts: scheduler invocation, page request, upstream observation, freshness observation, policy evaluation, intent creation, delivery attempt, acknowledgment, and resolution. Correlation IDs connect one execution's records; stable domain keys establish identity across executions. They solve different problems.

Monitor poll age, reconciliation outcomes, pagination depth, unresolved incident age, outbox backlog age, and delivery outcomes. The Google SRE treatment of distributed-system monitoring frames latency, traffic, errors, and saturation as useful signals. Here, poll duration and delivery delay express latency; request and intent counts express traffic; read and send failures express errors; worker pressure and backlog express saturation. Keep import freshness separate because healthy infrastructure can process zero useful results.

Tests must force boundaries. Supply two groups with the same timestamp, repeat a page, crash after intent insertion, retry a leased intent, return a partial final page, and deploy old and new pollers concurrently. These cases expose whether schema compatibility and idempotency actually hold.

Compliance constraints shape retention. Keep only diagnostic content needed for response and audit, classify it, restrict access, and define deletion rules. Chat and email are poor systems of record because access, forwarding, and retention can differ. The audit trail can retain a sanitized digest and delivery metadata without copying sensitive error payloads into every channel.

Cost is a guardrail, not the thesis. Poll frequency multiplies API requests and evaluation work, while verbose or high-cardinality telemetry expands storage. Cloud log services may charge for ingestion volume, so eliminate duplicated payloads and unbounded dimensions before relaxing the response objective.

## Rejected option and its valid boundary

A stateless cron script that queries `now - interval` and sends every result directly is rejected for this workflow. Clock skew, delayed indexing, equal timestamps, overlapping runs, pagination, and crashes between send and exit make its decisions hard to reconstruct. A wider window trades misses for duplicates; it does not create a transaction between observation and notification.

The approach remains valid for a temporary internal check where duplicates are harmless, missed notices are manually recoverable, and no audit obligation exists. Document that limit and an expiration date. Once publication commitments or regulated records depend on the import, the reconciler and outbox are the smaller operational design because failure becomes explicit rather than residing in an operator's memory.

An alert is trustworthy only when its evidence, policy revision, incident identity, and delivery history remain reconstructible after retries. Unresolved errors provide one kind of evidence. Missing committed results provide another. Reconcile both, and keep notification channels outside the source of truth.

## References

- https://sre.google/sre-book/monitoring-distributed-systems/
- https://aws.amazon.com/cloudwatch/pricing/
