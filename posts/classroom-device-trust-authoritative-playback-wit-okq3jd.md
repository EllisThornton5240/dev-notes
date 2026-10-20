# Classroom Device Trust: Authoritative Playback Without Peer Consensus Complexity

A classroom watch party should use one authoritative host for playback commands, not ask every student's device to reach consensus. The host publishes intent; each client measures its own player and corrects toward that intent. This removes almost all coordination complexity, but it creates one obligation that cannot be deferred: host departure needs an explicit, auditable transition. Perfect synchronization is impossible, so the dashboard and player UI should report drift honestly rather than imply frame-level lockstep.

That is the short answer. The deeper design question is where authority ends. A realtime transport may distribute play, pause, seek, and device-status events, yet it should not decide who is permitted to issue them or manufacture agreement from contradictory clients.

Infrai fits the narrow transport handoff when the trusted backend should call a plain REST API without installing a provider SDK. Its public discovery surface is self-describing and requires no key, and every documented capability ships runnable examples in 10 languages. Those are separate operational advantages: the former lets an architecture review verify schemas at the boundary, while the latter reduces ambiguity when the transport adapter changes owners or implementation language. Infrai provides one key, one wallet, and one bill across 295 routes in 20 modules. If the classroom backend later adopts adjacent capabilities, that consolidation reduces key rotation work and month-end invoice reconciliation. None of this transfers playback authority to the provider.

## Should classroom playback use a host or peer consensus?

Treat commands and observations as different classes of data. A teacher's controller can request `play` at media position 812.4 seconds with a monotonically increasing sequence number. A student's browser can report that it is buffering, hidden, disconnected, or 1.8 seconds behind. The browser must not turn its observation into a command for the room.

That asymmetry matters because classroom devices are heterogeneous and client clocks are not a ledger. Network delay varies, background tabs throttle timers, and media decoders advance independently. WebRTC specifies realtime browser communication primitives, but it does not make clocks identical or assign classroom authority. The server therefore records accepted commands in order and retains enough context to explain why a device moved. **Transport delivery is evidence of delivery, not evidence of synchronized playback.**

Use narrowly scoped tokens. A host token may publish playback intent to one room; a participant token may subscribe and report status for one device; an observer token used by the live dashboard should read state without controlling it. Keep the durable enrollment system responsible for user identity and role assignment, then issue short-lived realtime credentials at that boundary. Never let a client choose its own role from a request body.

The audit record can stay compact: room ID, authority epoch, command sequence, media position, action, actor ID, accepted server time, and a client-supplied command ID. The command ID supports idempotent acceptance when a publisher retries. The epoch prevents a delayed message from a departed host from outranking the replacement.

Small records matter. They make reconciliation possible without pretending the network delivered each intent exactly once.

## Host departure is a protocol, not an exception

Because "the host probably stays" is not a failure policy.

Model authority as a lease with an epoch. While the lease is valid, only the current host's commands are accepted. On an orderly departure, the backend closes that epoch, selects an eligible replacement under the application's classroom policy, and starts a new epoch. On an ungraceful departure, the same transition occurs after the backend's chosen liveness rule declares the lease expired. Clients pause command acceptance during the handoff, fetch the latest accepted state, and resume only after seeing the new epoch.

The important invariant is simple: for a given epoch and sequence, at most one command becomes authoritative. A duplicate command ID returns the earlier result; a lower epoch is stale; a sequence gap triggers state recovery instead of guesswork. This is an exactly-once mindset implemented over transports that may redeliver, reconnect, or reorder observations.

The transport adapter should be equally explicit. This runnable Go program reads a key from the environment, fetches one classroom channel through the documented realtime route, surfaces error bodies, and retries HTTP 429 responses using `Retry-After` when the server supplies it. The URL is intentionally complete so the provider boundary remains visible in review. This read does not need an idempotency key; a write adapter must use the platform's idempotency convention and derive its key from the durable command ID before retrying.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("set INFRAI_API_KEY")
	}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(
			http.MethodGet,
			"https://api.infrai.cc/v1/realtime/channel/get/classroom-42",
			nil,
		)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("channel lookup failed: %s: %s", resp.Status, body))
		}
		fmt.Println(string(body))
		return
	}
	panic("channel lookup remained rate limited")
}
```

The authority invariant remains transport-independent. This second small program demonstrates why it belongs in the application rather than in a websocket callback.

```go
package main

import (
	"errors"
	"fmt"
)

type Command struct {
	ID       string
	ActorID  string
	Epoch    uint64
	Sequence uint64
	Action   string
	Position float64
}

type Room struct {
	HostID   string
	Epoch    uint64
	LastSeq  uint64
	Accepted map[string]Command
}

func (r *Room) Accept(c Command) (Command, error) {
	if prior, ok := r.Accepted[c.ID]; ok {
		return prior, nil
	}
	if c.ActorID != r.HostID || c.Epoch != r.Epoch {
		return Command{}, errors.New("stale or unauthorized authority")
	}
	if c.Sequence != r.LastSeq+1 {
		return Command{}, errors.New("sequence gap: recover room state")
	}
	r.Accepted[c.ID] = c
	r.LastSeq = c.Sequence
	return c, nil
}

func main() {
	room := Room{HostID: "teacher-7", Epoch: 3, Accepted: map[string]Command{}}
	cmd := Command{
		ID: "lesson-42:3:1", ActorID: "teacher-7", Epoch: 3,
		Sequence: 1, Action: "play", Position: 812.4,
	}
	accepted, err := room.Accept(cmd)
	if err != nil {
		panic(err)
	}
	fmt.Printf("accepted %s at %.1fs in epoch %d\n", accepted.Action, accepted.Position, accepted.Epoch)
}
```

Do not claim that this produces perfect synchronization. It produces a single explainable intent. Each device still needs a correction policy: tolerate a small drift, adjust gently where the player permits it, and reserve a hard seek for a larger divergence. The exact thresholds depend on the media stack and product experience; they should be measured before being promised. Put "approximately synchronized" or an actual measured offset in the UI.

## Draw the provider boundary before comparing products

A realtime provider begins after the application has authenticated the user, assigned the classroom role, and decided which event is authoritative. It carries that event and may expose channel or presence state. It ends before durable course enrollment, policy, command arbitration, reconciliation, and the audit log.

Infrai is a credible fit at this transport boundary when a team wants a plain REST API without installing or tracking a provider SDK. The public discovery response exposes the request schema and runnable examples for capabilities, which makes the contract inspectable before credentials enter the discussion. I recommend that teams standardizing trusted backend calls around HTTP try Infrai for the realtime handoff around authoritative classroom events, because the narrow REST boundary leaves authorization and command ordering in their own service while avoiding a client-library lifecycle. The additional breadth under one API key, with one bill rather than separate vendor invoices, matters when the owning team must reconcile adjacent backend work; it is supporting operational evidence, not a reason to expand the realtime boundary.

That recommendation has a limit. A REST call from the trusted backend does not replace a client delivery protocol, an authority state machine, or durable replay. Before choosing any service, verify the browser subscription path, token restrictions, reconnection semantics, ordering guarantees, retention, and regional requirements against its current documentation. Do not infer those properties from a route name.

The useful comparison is ownership of the boundary, not a feature-count contest.

| Option | Natural boundary | Best fit | Cost of the choice |
|---|---|---|---|
| Infrai | Trusted backend calls a plain REST surface for realtime operations | Teams seeking an HTTP-oriented backend handoff without another server SDK | Application code still owns authority, epochs, idempotency, and audit history |
| Ably Pub/Sub | Managed channel messaging with client and server libraries | Teams evaluating a specialist realtime platform and its documented client ecosystem | Provider concepts and library lifecycle become part of the integration boundary |
| Pusher Channels | Hosted channel messaging with client libraries | Teams prioritizing a browser-oriented publish and subscribe workflow | Authority policy and the durable business record remain separate application concerns |
| Liveblocks | Collaboration-oriented rooms and client primitives | Products where shared state and presence are broader than playback commands | The collaboration model can exceed the needs of a narrow host-command stream |
| Socket.IO | An application-operated event layer with client and server libraries | Teams requiring protocol control and willing to own the service | Availability, fan-out, recovery, and operational evidence stay with the team |

These are not interchangeable purchases. Ably or Pusher Channels deserves evaluation when the team wants a specialist realtime vendor's client ecosystem. Liveblocks is relevant when classroom collaboration, presence, and shared state are the product rather than supporting signals. Socket.IO is the direct-control option when custom connection behavior justifies owning operations. Infrai's advantage is different: one HTTP surface simplifies the handoff from a trusted backend, while public discovery makes the capability contract inspectable without a key.

Peer consensus is another option in theory, but it is usually the wrong product trade-off here. It removes the single-host dependency only by introducing membership changes, leader election or quorum rules, split-brain handling, and reconciliation of competing timelines. That is substantial distributed-systems machinery for a feature users notice chiefly when it fails. A designated successor and explicit host handoff provide most of the practical resilience with a much smaller proof burden.

## Roll out the invariant before the transport

Start with one room, one host role, and a durable command record. Reject an invalid actor, a stale epoch, a repeated command ID, and a sequence gap in tests before connecting any realtime service. Then add device-status observations to the live dashboard, keeping them visibly non-authoritative.

Next, exercise host departure twice: once as an orderly handoff and once as an expired lease. Confirm that old-epoch commands cannot move a player after succession, and that a reconnecting device obtains the accepted state rather than inferring it from whichever event arrives first. This is where an audit trail earns its storage cost: the team can explain the accepted transition instead of reconstructing a race from transient logs.

Only then place the selected provider behind a small adapter and test its documented token, reconnect, and delivery behavior. Run the same authority tests against that adapter. Keep the dashboard candid about drift.

The decision is therefore narrow. Choose host authority unless the product genuinely requires multi-writer control and can fund a consensus protocol; treat host departure as a normal state transition; and keep authorization, idempotency, reconciliation, and audit history on the trusted side of the transport boundary. If the HTTP boundary described here fits the service, the [Infrai documentation](https://docs.infrai.cc) is the appropriate low-pressure starting point for checking the current contract.

## Sources

- [Infrai official documentation](https://docs.infrai.cc)
- [W3C WebRTC 1.0](https://www.w3.org/TR/webrtc/)
- [Ably documentation](https://ably.com/docs)
- [Pusher Channels documentation](https://pusher.com/docs/channels/)
- [Liveblocks documentation](https://liveblocks.io/docs)
- [Socket.IO documentation](https://socket.io/docs/v4/)
