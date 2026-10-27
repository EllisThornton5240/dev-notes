# Waiting Room Queue Position Live Updates: Node.js API Recovery for Logistics Support

Short answer: publish a monotonic queue revision, let each browser remember its last applied revision, and make reconnect begin with a snapshot followed by bounded replay. The live connection is a delivery path; it is not the source of truth. For a logistics support queue, this distinction prevents a driver from seeing position 4 after position 7 simply because a mobile network dropped two packets.

The cursor is the contract.

The useful invariant is narrower than "exactly once over the network": the queue service commits each state transition once, the notification layer records an append-only audit event, and the client applies revisions idempotently. A reconnect either receives every revision still retained or receives a fresh snapshot with an explicit revision. There is no silent gap.

## How should a waiting room queue show position with live updates?

A queue position is derived state. The durable record should describe the support case, its queue, its eligibility, and the ordering decision; a projection can then calculate `position`, `estimated_wait_seconds`, and `status`. In a logistics operation, a case might move from `awaiting_documents` to `assigned` while a warehouse agent changes the order of other cases. Sending only "position changed" loses the reason and makes reconstruction difficult.

I use an envelope with three properties: a stream identifier, a strictly increasing revision within that stream, and a complete enough payload for deterministic application. The revision is not a timestamp. Clock skew, retries, and two workers finishing close together make timestamps poor ordering tokens.

The first event after authentication is a snapshot at revision 1842, for example. Subsequent events are revisions 1843 and 1844. If the client reconnects with 1842 and the server has retained those events, it can replay from 1843. If retention has ended, the response must say so and send a new snapshot; the client must replace its local view rather than guessing.

This is an important compliance limit: a notification payload should contain the minimum operational data needed by the customer interface. Shipment references and queue status are usually enough. Internal notes, payment details, and personally identifying information do not belong in a browser event merely because the browser is already connected.

## The decision record

The critical path is: authorize the subscription, read a consistent snapshot or replay boundary, attach the connection to the stream, then emit later revisions in order. A queue mutation commits independently of browser delivery. That separation gives the system a place to reconcile when delivery fails.

| Option | Reconnect behavior | Failure boundary | Appropriate use |
| --- | --- | --- | --- |
| Polling | Client asks for current state repeatedly | Request timeout or stale interval | Small queues and low urgency |
| Server-Sent Events | Browser receives a one-way ordered stream and reconnects | Connection loss; replay requires server retention | Status and position updates |
| WebSocket | Bidirectional session with application-defined messages | Connection loss plus protocol/session handling | Interactive agent tools |
| WebRTC data channel | Peer connection with negotiated data transport | ICE, peer lifecycle, and application recovery | Peer-oriented media-adjacent flows |

For a customer queue display, one-way events are easier to audit than a bidirectional command channel. WebSocket remains a valid choice when the same session must carry agent commands or typing state. WebRTC is a different boundary: the W3C specification defines peer connections and data channels, but it does not define queue ordering, durable replay, or authorization policy.

The rejected option is a position-only broadcast: "case A is now 3". It looks cheap until a reconnect, a filter change, or a second queue assignment occurs. Its valid use case is a disposable progress hint where the next authoritative snapshot is already guaranteed to arrive; it is not sufficient as the ledger of customer-visible state.

The trade-off is explicit. Bounded replay consumes retention space and keeps a little more server state, while snapshot-only delivery uses less retention but makes every reconnect heavier. A small queue may accept that heavier recovery; a busy dispatch operation generally benefits from retaining enough revisions to cover its expected mobile outage window. The hard limit is still documented: once a cursor falls outside retention, only a new snapshot can restore correctness.

## A narrow Node.js boundary with an exactly-once mindset

The example below keeps storage and transport behind interfaces. The Go implementation is intentional because the service boundary should be testable without binding correctness to an HTTP framework; the same contract can be consumed by a Node.js gateway. The gateway should acknowledge a subscription only after it has obtained the snapshot or established a replay cursor.

```go
package queue

import "context"

type Revision int64

type Event struct {
	Stream   string
	Revision Revision
	Kind     string
	Payload  []byte
}

type Store interface {
	Snapshot(ctx context.Context, stream string) (Revision, []byte, error)
	After(ctx context.Context, stream string, revision Revision) ([]Event, error)
	Oldest(ctx context.Context, stream string) (Revision, error)
}

func Backfill(ctx context.Context, store Store, stream string, last Revision) ([]Event, error) {
	oldest, err := store.Oldest(ctx, stream)
	if err != nil {
		return nil, err
	}
	if last > 0 && last+1 < oldest {
		// The client has a gap; caller must send a fresh snapshot.
		return nil, ErrSnapshotRequired
	}
	return store.After(ctx, stream, last)
}
```

In Node.js, the connection handler can pass the browser's `lastEventId` or an equivalent explicit cursor to this boundary. It should cap replay size and time, close a slow consumer deliberately, and expose the reason as an operational metric. A client that receives revision 1844 after 1842 should not apply it; it should request recovery. Duplicate 1843 is harmless if the reducer ignores revisions less than or equal to its applied revision.

There is a subtle race between reading a snapshot and subscribing to new events. Solve it with a storage-defined boundary: read snapshot revision R, register from R, then send the snapshot and all events after R in that order. A loose "read then connect" sequence can lose the event committed between those actions. This race is more consequential than the choice of stream protocol.

No gap.

The practical consequence is that the queue projection and the notification stream need a shared definition of completion. Suppose a case is moved into a priority lane while the snapshot is being serialized. If the snapshot claims revision 1842 but the subscription starts after 1843, the browser can remain internally consistent while still missing the move; a later position update may appear plausible and conceal the loss. The storage boundary prevents that false confidence by making the handoff observable: either the stream begins at 1842 and carries 1843, or the server returns a newer snapshot whose revision includes it. This also gives reconciliation a finite question to answer: which revisions exist, which were served, and which cursor did the client acknowledge?

## What must be tested before the queue is called live?

Start with property tests around the reducer: shuffled delivery never decreases the applied revision; duplicate delivery leaves state unchanged; a missing revision triggers recovery; and a snapshot followed by replay produces the same state as applying the full event sequence. These are deterministic tests, so they belong in the service repository rather than in a manual browser checklist.

Then test the edges that production will supply for free: mobile sleep, proxy idle timeouts, a tab restored from the back-forward cache, token expiry during replay, two browser tabs for one case, and a queue mutation during snapshot creation. Record the stream, requested cursor, served cursor, gap decision, and authorization result. Keep payloads out of ordinary logs. An audit trail needs enough metadata to explain a decision without becoming a second copy of customer data.

One pitfall deserves its own line: a reconnect timer is not a recovery algorithm.

The dashboard should distinguish four states: connected and current, connected and catching up, disconnected with retry scheduled, and snapshot required. A spinner cannot communicate the difference between a two-second reconnect and a stale position that needs a full reload.

## Decision rule

Choose the transport that matches the direction of communication, but choose the recovery contract first. For a logistics customer queue, that contract is a revisioned snapshot plus bounded replay, with explicit authorization on every subscription and a retention policy that has a documented limit.

Do not promise exactly-once network delivery. Promise an idempotent state transition and an auditable path back to authoritative state. That promise survives browser retries, worker restarts, and the ordinary disorder of a mobile connection far better than a broadcast of the latest number.

## References

- W3C, WebRTC 1.0: Real-Time Communication Between Browsers: https://www.w3.org/TR/webrtc/
- MDN, Using server-sent events: https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events
- RFC 6455, The WebSocket Protocol: https://www.rfc-editor.org/rfc/rfc6455
- RFC 9110, HTTP Semantics: https://www.rfc-editor.org/rfc/rfc9110
- OWASP, Logging Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html
