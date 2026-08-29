# 2026 Large AI Intake: Multipart Object Storage Beats One-Request Image Uploads Above 1 GB

Short answer: for private marketplace images above 1 GB, choose multipart object storage over a single PUT when failed transfers would otherwise restart the entire upload; keep single PUT for bounded files that can finish comfortably inside one request. The decisive metric is useful bytes committed per unit of worker time, but the design is incomplete unless completion, abort, and expiring download authorization are explicit state transitions.

This is a control-plane decision before it is a storage decision. A marketplace accepts an asset on behalf of a seller, keeps it private, and later grants a buyer or reviewer temporary access. The generated image may be large, but its byte count does not relax tenant isolation, reconciliation, or evidence requirements. Fast ingestion without a provable owner and terminal state is merely fast ambiguity.

## Throughput starts at the generator queue

Peak bandwidth is the least interesting number. Measure goodput: bytes belonging to a completed, authorized object divided by elapsed processing time, including retransmission and cleanup. A single PUT has an attractive state model because the service either accepts one request or it does not, yet a late connection loss makes the sender replay every byte. Multipart transfer divides that exposure: the sender can retry a failed part while preserving successful parts, then present the ordered part results at completion.

This changes where pressure accumulates. If a generator holds a 1 GB result in memory while one upload runs, ten concurrent jobs can pin roughly 10 GB before runtime and queue overhead; streaming limits that footprint, while bounded part concurrency prevents the supposed throughput optimization from becoming a memory incident. Don't equate more parallel requests with more useful work. Increase concurrency only until aggregate goodput stops improving or tail latency, memory, and request throttling begin to worsen.

One constraint dominates: an object must not become downloadable merely because some parts exist.

Completion is the commit.

Treat the upload record as an accounting journal with an immutable tenant, object key, expected size, creation time, expiry, and current state. `initiated` may move to `completing`, then exactly once to `complete`; cancellation or expiry moves an uncommitted record to `aborted`. Network delivery is at-least-once in practice, so exactly-once behavior belongs at the state transition: repeated completion messages must resolve to the same committed record, and repeated abort requests must not create a second outcome. This is the same distinction a ledger backend makes between receiving a request twice and posting an economic event twice.

## How can Node.js presign multipart parts and complete large image uploads?

Let the Node.js application remain the authorization and orchestration boundary, even if a browser, render worker, or native client sends bytes directly to S3-compatible object storage. After authenticating the seller, it creates the upload record and multipart session, returns short-lived presigned part URLs, records each returned part identifier, and accepts an idempotent completion command. The bearer credential used for the marketplace API does not belong on storage requests made through presigned URLs.

The following Go model is intentionally an interface, not a vendor setup guide. It captures the contract that the Node.js control plane and any storage adapter must preserve: tenant-bound commands, ordered parts, one terminal result, and an audit entry written in the same transaction as the state change.

```go
package intake

import (
	"context"
	"errors"
	"sort"
	"time"
)

type State string

const (
	Initiated State = "initiated"
	Complete  State = "complete"
	Aborted   State = "aborted"
)

type Part struct {
	Number int
	ETag   string
}

type Upload struct {
	ID        string
	TenantID  string
	ObjectKey string
	State     State
	ExpiresAt time.Time
}

type Repository interface {
	Lock(ctx context.Context, tenantID, uploadID string) (Upload, error)
	Commit(ctx context.Context, upload Upload, event string) error
}

type ObjectStore interface {
	Complete(ctx context.Context, objectKey, uploadID string, parts []Part) error
	Abort(ctx context.Context, objectKey, uploadID string) error
}

func CompleteUpload(ctx context.Context, repo Repository, store ObjectStore, tenantID, uploadID string, parts []Part) error {
	upload, err := repo.Lock(ctx, tenantID, uploadID)
	if err != nil {
		return err
	}
	if upload.State == Complete {
		return nil
	}
	if upload.State != Initiated || time.Now().After(upload.ExpiresAt) {
		return errors.New("upload is not eligible for completion")
	}

	sort.Slice(parts, func(i, j int) bool { return parts[i].Number < parts[j].Number })
	for i, part := range parts {
		if part.Number != i+1 || part.ETag == "" {
			return errors.New("parts must be contiguous and identified")
		}
	}
	if err := store.Complete(ctx, upload.ObjectKey, upload.ID, parts); err != nil {
		return err
	}

	upload.State = Complete
	return repo.Commit(ctx, upload, "marketplace.image_upload.completed")
}
```

The example makes the ordering rule visible because asynchronous workers commonly return part results out of order. Production code also needs request authentication, an idempotency key unique within the tenant, expected-size validation, a digest policy supported end to end, and transaction semantics appropriate to the repository. I'm not sure one universal part size can be defended across browsers, regional links, and render workers; staging measurements of retry bytes, p95 completion time, and resident memory should settle that choice for a particular workload.

Presigned URLs deserve separate scrutiny. They are time-limited bearer capabilities, so issue them only after authorization, constrain their lifetime, and avoid logging the query string. A private download follows the same rule: authorize the current principal first, then mint an expiring GET URL for the immutable object key. Expiry limits future use; it does not revoke a URL already copied into a log, chat, or analytics system.

## The 1 GB threshold is a workload hypothesis

The engineering choice is now narrow enough to state plainly.

| Decision factor | Single PUT | Multipart |
| --- | --- | --- |
| Retry unit | Entire object | Failed part |
| Control state | One transfer result | Initiation, recorded parts, completion, and abort |
| Memory strategy | Suitable for bounded buffers or a single stream | Suitable for streaming with bounded parallel parts |
| Reconciliation | Find missing final objects | Find stale sessions and ambiguous completion attempts |
| Best fit | Small, predictable marketplace previews | Large originals where partial retry improves goodput |

Multipart wins for the stated 1 GB marketplace path because a late retry does not require replaying the whole original, and bounded parallelism can keep the data plane busy without coupling throughput to one long request. The catch is operational state: every initiated upload must eventually be completed or aborted, every completion must use the intended ordered parts, and the database must explain which tenant authorized the object. If the team cannot operate that reconciliation loop yet, or if measurements show that files are small and single PUT consistently meets the deadline, stick with single PUT. Fewer states are an advantage when the transfer risk is already bounded.

Run the threshold experiment with the same queue and worker limits used in production. For each candidate object-size band, hold the total input bytes constant, vary bounded part concurrency, inject a connection loss at early and late positions, and record committed goodput, replayed bytes, peak resident memory, and time until an abandoned session is reconciled. Do not publish the fastest isolated run as the answer. The crossover belongs where multipart's reduction in replay work remains visible at p95 without pushing memory or reconciliation age beyond the service objective; if no such crossover appears, the system has no evidence for adding the multipart branch.

There is another boundary. Multipart is not suitable as a substitute for application-level authorization, malware policy, content moderation, retention rules, or legal holds. Those decisions surround the transfer. They don't emerge from it.

Abort stays final.

## Commit evidence survives adverse sequences

Test sequences, not isolated endpoints. Start a transfer, accept parts 1 through 7, expire the authorization, and confirm that completion is denied while cleanup remains possible. Deliver the same completion command twice and require one durable completion event. Submit part results in arrival order and verify that orchestration sorts them numerically. Cancel while workers still possess presigned URLs, then verify that the record remains terminal and cannot be resurrected by a delayed callback. Finally, request a private download as a different tenant and require denial before any URL is issued.

The useful dashboard pairs throughput with correctness: committed bytes per second, retransmitted bytes, active sessions by age, completion latency, abort latency, duplicate idempotency keys, and ledger-to-object reconciliation lag. A rising count of old initiated sessions can hide behind excellent bandwidth graphs. So can a queue that repeatedly completes the same logical command but emits multiple audit entries.

Be precise about compliance. FedRAMP is a government-wide risk and authorization program, not a declaration that a particular presigned link, bucket setting, or multipart workflow is compliant. A regulated deployment still has to map applicable controls to identity, encryption, retention, access logging, incident evidence, and the authorization boundary of every participating service. Your mileage may vary by authorization scope; the system security plan and assessor, rather than the upload code alone, determine what evidence is sufficient.

The audit trail should answer a compact set of questions months later: who initiated the upload, for which tenant, under which idempotency key, what immutable object key was committed, when download authority was granted, and why an abandoned session was aborted. Do not store full presigned URLs as evidence. Store the authorization decision and stable identifiers, because the secret-bearing query parameters add exposure without improving the historical account.

## Migrate without reinterpreting in-flight sessions

Begin with a shadow calculation that classifies objects by expected size while the existing path remains authoritative. Then enable multipart for a small tenant cohort, cap concurrent parts per upload and per tenant, and run an expiry-driven abort worker from day one. Compare goodput, retransmitted bytes, memory, and reconciliation age against the single-PUT baseline; raise the threshold or concurrency only from observed data.

Keep rollback boring. New initiations can return to single PUT, while already initiated multipart sessions continue to their recorded terminal state. Never reinterpret an in-flight upload under a different protocol. Once the data supports broader adoption, retain both paths: multipart for large originals, single PUT for small previews, and one shared authorization and audit model around them.

## Sources

- https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html
- https://www.fedramp.gov/
