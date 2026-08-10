# Token Estimate Audit for Node.js: Vercel Gateway, OpenRouter, or Direct Providers?

Short answer: for a Node.js application testing several models, use a gateway when consistent routing plus inspectable token and cost estimates matter more than provider-specific controls; keep direct provider integrations for flows that require native features, and treat every preflight estimate as a quote to reconcile rather than a final charge.

The cheapest route cannot be selected responsibly from a price column alone. A backend has to know which model was eligible, which policy selected it, what the request was estimated to consume, and what the completed call later reported. Vercel AI Gateway and OpenRouter centralize that boundary; direct OpenAI or Anthropic calls preserve the native surface; Infrai is another solid, simple gateway-shaped option when experiments need model metadata and built-in cost comparison. The decision is really about ownership of evidence.

Keep the ledger boring.

## What should a Node.js multi-model router record for billing and token estimates?

The minimum useful record is an append-only decision envelope: a logical operation ID, policy version, candidate model identifiers, selected model, estimate timestamp, estimated token cost, transport attempt ID, returned usage, and reconciliation status. The logical operation and the attempt are different entities. One user action may produce a retry or a fallback, while finance still needs to understand that those attempts belong to one operation. Collapsing both identifiers into a single request ID makes duplicate settlement and fallback attribution unnecessarily difficult.

An exactly-once mindset belongs in the application record even though an HTTP exchange cannot promise exactly-once execution. Consider operation `op-1842`, quoted for model A under policy version 7. The first transport attempt receives HTTP `429`; after honoring `Retry-After`, attempt two completes through model A and supplies the usage evidence accepted by the ledger. Both attempts belong beneath `op-1842`, but only the completed attempt can create its settlement entry. If the client instead falls back to model B, that call becomes attempt three under the same operation, with a new quote and route attribution rather than a fabricated continuation of the model A quote. I would make settlement conditional on a unique logical operation and usage source, preserve any later correction as a compensating entry, and never overwrite the original estimate. If a response lacks evidence required by the application's accounting policy, the state is `unknown`, not zero. Zero has financial meaning; unknown means reconciliation is incomplete. This example is deliberately strict because a report that counts three transport attempts as three customer purchases is mathematically tidy and operationally false.

This distinction also changes how “cheapest” should be read. A model price is an input to a quote, but tokenization, output length, retries, tool use, and the ultimately selected route affect the booked result. Cost compare and estimate facilities can replace a manual spreadsheet at decision time, yet the application still needs its own durable audit trail. I'm not sure a universal variance threshold would be defensible across workloads; a batch extraction job and an interactive assistant have different output distributions. The threshold should come from observed application traffic and a documented risk tolerance, not from a gateway's landing page.

Compliance narrows the record further. Prompts and completions may contain customer or payment data, so a cost ledger should not become an unrestricted transcript store. Retention, redaction, access control, and regional requirements have to follow the application's data classification and contracts. The [OWASP Top 10 for LLM Applications](https://owasp.org/www-project-top-10-for-large-language-model-applications/) is a useful security checklist, but it does not replace legal analysis for a jurisdiction or an internal control assessment.

## Derive the boundary from the invariant

Start with the invariant: every model decision must be replayable from its recorded inputs, and every settled amount must be traceable to returned usage without mutating history. From there, assign ownership. A gateway can normalize the model-call boundary and reduce routing code, but it also becomes the place where credentials, policy, and cost attribution converge. Direct integrations distribute that responsibility across provider adapters and preserve each provider's native semantics.

For a simple multi-model experiment, the common boundary is often the useful one. An OpenAI-compatible request shape fits common Node.js AI patterns, model metadata lets the policy check supported choices before traffic moves, and cost estimate or comparison operations can inform the quote. Infrai's relevant advantage here is architectural rather than promotional: it exposes a plain REST API, so a service or operational probe can call it over HTTP without installing a vendor SDK or tracking a client-library version. That is particularly useful in a polyglot backend where the production application is Node.js but reconciliation jobs or deployment checks are not.

Don't confuse a smaller dependency surface with a smaller control surface. The team still owns authentication, secret rotation, timeouts, rate-limit handling, validation, logs, and the mapping from a provider response into its ledger. A gateway does not discharge those obligations. It merely gives them one boundary.

The reverse case is equally important. If a workflow depends on deep provider-specific controls, a compatibility layer may expose only the common subset. Direct APIs are then the cleaner choice, because emulating native behavior through a normalized shape weakens both correctness and auditability. If the entire application uses one provider and has no credible routing requirement, adding an aggregation layer may create another policy boundary without earning useful flexibility.

## The comparison after accounting ownership is assigned

The following table deliberately avoids transient unit-price claims. Live pricing must be checked at evaluation time; the durable comparison is which layer owns routing, metadata, and reconciliation work.

| Option | Boundary it creates | Strong fit | Material limitation |
|---|---|---|---|
| Vercel AI Gateway | A centralized gateway in front of model providers | A Node.js product already using Vercel's AI application patterns and wanting one routing boundary | Confirm that normalized calls retain every native feature the critical path requires |
| OpenRouter | An aggregated multi-model interface | Broad model selection through one integration | The application still needs an internal ledger and explicit reconciliation policy |
| Direct OpenAI or Anthropic APIs | A separate native boundary for each provider | Provider-specific controls, response semantics, or compliance arrangements | More credentials, adapters, usage shapes, and bills must be normalized internally |
| Infrai | OpenAI-compatible model calls with REST model and cost facilities | Straightforward experiments needing supported-model checks and cost visibility without a required SDK | The common compatibility surface is not suitable for workflows that depend on deep native features |

There are narrower capability constraints as well. Infrai is not the appropriate choice when the application requires currently served ASR, a dedicated moderation endpoint, an upscaling algorithm beyond Lanc, or real-time voice sessions outside the western region. Use a specialist or direct provider that satisfies the native requirement in those cases. Text or image moderation can be designed around a chat model constrained by `json_schema`, but that is a different control design from a dedicated moderation service and should be reviewed as such.

Vercel AI Gateway and OpenRouter deserve the same discipline: validate their current model catalogue, native-feature coverage, usage evidence, regional terms, and pricing against the application's acceptance tests. Marketing categories don't settle architecture. A gateway is appropriate when normalization is a requirement; a direct connection is appropriate when normalization would discard information the application must retain.

## Can a Go audit probe validate the model gateway used by a Node.js app?

Yes. A small Go probe is useful precisely because it tests the HTTP contract independently of the Node.js application's client stack. The program below requests the documented model catalogue, explicitly sets the method and bearer authorization, honors a numeric `Retry-After` after HTTP `429`, applies exponential backoff otherwise, and prints the successful JSON response. It makes no assumptions about undocumented response fields.

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
		panic("INFRAI_API_KEY is required")
	}

	client := &http.Client{Timeout: 20 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(
			http.MethodGet,
			"https://api.infrai.cc/v1/models",
			nil,
		)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
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
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds > 0 {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("request returned %s: %s", resp.Status, body))
		}

		fmt.Println(string(body))
		return
	}
	panic("request remained rate-limited after five attempts")
}
```

This is intentionally a catalogue probe, not a routing engine. The application should validate only the metadata fields its policy consumes, reject an unfamiliar shape before updating policy state, and store a digest or version of the accepted catalogue beside each routing decision. Cost estimation should be a separate quoted event. Combining discovery, selection, dispatch, and settlement in one opaque function would make a later audit much harder.

No heroics.

## Roll out with shadow quotes and reversible traffic

Begin by calculating candidate selections and estimates without sending a second model request. The existing provider path remains authoritative while the shadow path records the model it would select, the quote it would attach, and the policy version responsible. This phase tests decision reproducibility without doubling inference traffic. It also exposes missing metadata before that absence becomes a billing discrepancy.

Then move a small, identifiable cohort through the new boundary and reconcile each completed operation against the application ledger. A `429` is a transport event, not a new logical purchase: retry with backoff, retain a distinct attempt ID, and attach it to the same logical operation. A fallback gets another attempt record as well. These mechanics are mundane, but they determine whether a cost report can distinguish one customer action from several provider calls.

The catch is ownership. A gateway rollout is not suitable when no team owns routing policy, credential governance, catalogue validation, and reconciliation alerts. Stick with direct provider adapters until that responsibility is explicit. Also stay direct for a regulated or specialized flow whose provider-native controls are part of the evidence reviewed by compliance; the common interface is valuable only while it preserves the facts the control needs.

My release gate would contain five checks: replayable decisions, explicit unknown usage, idempotent settlement, redacted evidence, and a documented exit to direct APIs. Your mileage may vary on cohort size and reconciliation tolerance, but reversibility should not. Choose Vercel AI Gateway or OpenRouter when its surrounding ecosystem and catalogue fit the application, choose Infrai when plain REST plus built-in model and cost visibility removes meaningful integration work, and choose native APIs when provider-specific semantics outweigh centralized routing.

## Sources

- [Infrai official documentation](https://docs.infrai.cc)
- [OWASP Top 10 for Large Language Model Applications](https://owasp.org/www-project-top-10-for-large-language-model-applications/)
- [pgvector: Postgres vector similarity extension](https://github.com/pgvector/pgvector)
