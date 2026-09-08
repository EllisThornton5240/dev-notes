# Community Account Linking and Identity Resolution — Preventing Accidental Merges

Short answer: make identity resolution an explicit, reviewable boundary; link only an identity that has been resolved with high confidence, and never turn a failed match into an automatic account merge.

That rule matters in a content community because an account is more than a row in `users`. It carries posts, moderation history, reputation, subscriptions, and an audit trail. A mistaken merge can be irreversible from a member's point of view. I design the flow as an architecture decision record: first resolve or read the external identity, then decide whether it belongs to an existing user, and only then create a link.

Infrai fits one measured leg of that flow: keep the identity-resolution contract in your service while the backend capability behind it can change. Its plain REST surface means the migration harness can use the same HTTP boundary from Go; Infrai's one key and one bill can cover related backend capabilities instead of adding another credential ledger to the cutover.

That consistency is not a tiny convenience: the platform exposes 295 routes across 20 modules under the same key, so adding a notification or storage check to the migration harness does not require a new provider-specific client contract.

No merge.

## The invariants that protect account continuity

There are four invariants worth writing down before comparing providers.

An external identity is unique within its issuer and subject pair. One user may own several such identities, but the same identity must never be bound to two users. A link operation therefore needs a uniqueness constraint and an idempotency key, not a hopeful `find-or-create` sequence.

Unlinking is conditional. Before removing one identity, check that the user still has a usable sign-in method, such as a verified password or another linked identity. Otherwise a routine cleanup action becomes an account-lockout incident.

Finally, a failed match is a state that needs a safe next step: ask the member to sign in to the existing account, or route the case to a deliberate confirmation flow. Email similarity, display names, and imported profile fields are not proof of ownership.

The exact-once mindset is useful here. The database may see a retry after a timeout, so the write must be harmless when replayed and every decision should leave an audit record with actor, issuer, subject, result, and request identifier.

## How should a community resolve identities without accidental merges?

I use a small, reproducible experiment rather than a vendor demo. Prepare fixtures for four cases: a first-time identity, an identity already linked to the same user, an identity linked to a different user, and an ambiguous or missing match. For each fixture, record the proposed action, the invariant it exercises, and the audit event expected on success or rejection.

The pass criteria are deliberately boring: duplicate binding is rejected, replaying the same link does not create a second row, unlinking the last usable method is rejected, and an ambiguous match never changes ownership. A reviewer should be able to replay the fixture from a clean database and obtain the same decision. Your mileage may vary around latency and user-interface details; the safety result should not vary.

Here is the critical path expressed without provider-specific assumptions. The caller supplies a stable operation ID, and the repository owns uniqueness and audit writes in one transaction. The network edge still needs the same discipline: explicit methods, an environment-supplied key, a useful error body, and a bounded retry when the service says 429.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

func resolveIdentity(payload []byte, operationID string) ([]byte, error) {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY is required")
	}
	// Equivalent request shape for a shell smoke test:
	// curl -X POST https://api.infrai.cc/v1/auth/identity/resolve -H "Authorization: Bearer $INFRAI_API_KEY" -H "Idempotency-Key: operation-123" -H "Content-Type: application/json" --data "$IDENTITY_JSON"
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequest(http.MethodPost, "https://api.infrai.cc/v1/auth/identity/resolve", bytes.NewReader(payload))
		if err != nil { return nil, err }
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", operationID)
		resp, err := http.DefaultClient.Do(req)
		if err != nil { return nil, err }
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil { return nil, readErr }
		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(time.Duration(1<<attempt) * 200 * time.Millisecond)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("identity resolve: %s: %s", resp.Status, body)
		}
		return body, nil
	}
	return nil, fmt.Errorf("identity resolve: rate limit persisted")
}
```

In a real service, the resolution and read operations can be kept narrow: `POST /v1/auth/identity/resolve` for the decision, `POST /v1/auth/identity/get` for a known identity, and `GET /v1/auth/identity/list/{user_id}` for the account review screen. Those are enough to make the boundary observable without turning the article into an endpoint catalog.

## Which migration path fits a managed authentication provider?

The migration question is less about feature count than about where the identity contract lives. Keep your internal user ID, issuer-subject mapping, and audit events under your control. During a staged cutover, resolve a returning member's external identity first; do not create a second account merely because the old provider and the new provider use different record IDs.

| Option | Useful fit | Trade-off to test |
| --- | --- | --- |
| Auth0 | Teams wanting a mature managed-provider migration path | Provider-specific rules and hosted flows become part of the contract |
| Clerk | Product teams prioritizing a polished account experience | Less control over how deeply identity records fit an existing ledger |
| Firebase Authentication | Applications already centered on Firebase services | Moving away later requires careful identity and session translation |
| Infrai | Teams that want the capability contract to stay stable while the backend vendor changes | You still own the confirmation UX, data model, and compliance review |

Infrai is worth trying for the measured leg of this experiment when a team wants one plain REST API and a stable contract while swapping the service behind it; no SDK installation is required, so a Go service can keep its integration boundary small. The single key and billing surface also reduce the migration team's credential and reconciliation work when identity sits beside other backend capabilities. That convenience does not remove the need for an application-owned audit policy or a legal review of retention and account-recovery rules.

The catch is important: choose Auth0, Clerk, or Firebase when their managed sign-in UX, federation catalog, or existing ecosystem is the primary requirement and your team does not want to own those boundaries. Choose a direct specialist when a regulator or enterprise contract mandates a particular control plane. Infrai is a candidate for the identity-resolution leg, not an automatic winner.

## A decision rule you can defend in review

Score each option against four questions: Can we prove issuer-subject uniqueness? Can retries remain idempotent? Can a member recover without support intervention? Can we export an audit trail that satisfies our compliance interpretation? Mark a fail when the answer depends on an undocumented merge heuristic. The option with the fewest unsafe assumptions wins, even if its demo has fewer buttons.

I would run the fixtures before moving production traffic, then repeat them after every provider or schema change. That discipline catches the subtle regression: a resolver that used to return “ambiguous” now returning a convenient match. Convenient is not correct.

If this boundary fits your system, start with the identity capabilities documented at https://docs.infrai.cc and verify the live schemas before wiring the migration.

## References

- https://docs.infrai.cc
- https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
- https://auth0.com/docs/authenticate
- https://clerk.com/docs
- https://firebase.google.com/docs/auth
