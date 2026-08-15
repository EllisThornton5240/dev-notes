# Contract Boundaries for Structured Summary JSON Output in Node.js

**Short answer:** Choose the output contract before choosing the model endpoint: for a Node.js service that must return a title, bullets, and key takeaways, the best API is the one whose schema-constrained output, refusal semantics, and retention terms survive your acceptance tests without forcing provider details into the domain layer. JSON syntax alone is insufficient. The application still owns semantic validation, idempotency, audit evidence, and reconciliation.

Parseability is cheap.

This changes the selection exercise from a feature checklist into a commit protocol. A generated summary is a candidate record, not a completed business event; it becomes authoritative only after the service verifies the declared shape, checks application invariants, and commits the result against a stable operation key. That distinction matters whenever a summary feeds a notification, customer statement, review queue, or another automated consumer rather than a person with an editor open.

## The governing constraint is the commit boundary

Start with what can go wrong after an apparently successful response. The transport may time out after the provider has produced an answer. A worker may restart between validation and persistence. Two deliveries of the same queue message may race. A newer prompt may emit a valid object whose title is empty after trimming, or whose takeaways merely repeat the source's instructions. None of these failures is solved by receiving parseable JSON.

Consider one concrete sequence. Delivery A reserves operation `sum_7f3`, sends the request, and loses its connection before receiving the response; the provider may have completed generation, but the service has no accepted record. The queue redelivers the message as delivery B. If B invents a fresh key, both deliveries can eventually publish different summaries, and an audit query now has two plausible answers for one source. If B reuses `sum_7f3`, it observes the live reservation and waits or returns an in-progress state. When the reservation lease expires, B may acquire it under a newer fencing token, generate a candidate, validate the three required fields, and commit exactly one accepted object. Delivery A can still wake later, so its stale commit must fail the ownership check. The generated calls were duplicated; the published effect was not. That sequence also explains why the audit record needs state transitions rather than a single success flag: `reserved`, `accepted`, `refused`, and `rejected_contract` support reconciliation, while a transport timeout leaves the operation unresolved until reservation recovery decides what may happen next. No provider-specific field belongs in that state machine.

The boundary therefore needs two contracts. The transport contract describes an object: required fields, types, collection bounds, string bounds, and rejection of undeclared properties. The business contract decides whether the content can be accepted: the title isn't blank, every bullet is meaningful, the summary doesn't assert an unsupported payment state, and the source was eligible for external processing. Schema-constrained generation can reduce structural variance, but it cannot prove factual accuracy or policy compliance.

For regulated or payment-adjacent data, classification comes first. Tokenize or redact account identifiers before sending text outside the approved trust boundary; retain prompts and raw responses only for the period and purpose allowed by policy; record a digest and internal source identifier when duplicating source material would enlarge the compliance surface. If an external processor, its data location, or its retention posture is prohibited, a hosted API is not suitable. Use an approved local runtime, even if its output contract requires more application-side enforcement.

Exactly-once generation is the wrong promise. Networks do not let a caller infer, from a timeout, whether remote work occurred. The defensible target is an exactly-once committed effect: derive an operation key from a canonical source digest, schema version, prompt version, and model configuration; reserve that key transactionally; and return the previously accepted result when the same operation arrives again. Don't include volatile request timestamps in the key. They turn retries into new operations.

This is the hinge.

## How should a Node.js summary API enforce JSON schema output for titles and bullets?

Own the schema in the application repository and version it independently of any provider request format. A practical first contract can require `title`, `bullets`, and `key_takeaways`, reject additional properties, place explicit upper bounds on both arrays, and forbid empty strings. The exact bounds are product decisions, not universal truths: a compact user interface may permit five bullets while an analyst report permits twelve. What matters is that the consumer, rather than the generator, defines them.

Send the same logical contract through the API's native schema-constrained mechanism when one exists. Function or tool arguments are one public form of schema-shaped generation, although a summary pipeline usually doesn't need tool dispatch; an explicit structured-output facility is conceptually cleaner when the only desired effect is data production. Keep the provider envelope inside an adapter, because supported schema keywords, refusal representation, model identifiers, and usage metadata can differ. The domain service should accept source text and a contract version, then receive either a typed candidate, an explicit refusal, or a classified failure.

Node.js static types don't validate network bytes. Parse the response, validate it at runtime against the local schema, and map it into a domain value only after validation succeeds. Apply semantic checks next. Treat a contract violation as a terminal result for that attempt, not as a generic transport error; repeated blind regeneration can multiply cost without changing the violated invariant. A single bounded regeneration may be reasonable when the failure class is known to be recoverable, but that policy needs its own metric and test corpus.

Error taxonomy is operational architecture, not naming polish. A timeout or rate limit can be retried under the same operation key with bounded backoff. `SUMMARY_SCHEMA_VIOLATION` should preserve the schema and model versions but exclude sensitive response content from ordinary logs. `SUMMARY_REFUSED` is a completed, auditable outcome rather than malformed JSON. A concurrent duplicate can return `409` while the reservation is active, then resolve to the committed record on a later read. Collapsing all four into `AI_ERROR` prevents the retry worker, on-call engineer, and reconciliation job from making consistent decisions.

I'm not sure a paper comparison can establish the best API for a particular corpus, because published documentation cannot reveal how a chosen model behaves on your malformed, multilingual, adversarial, or domain-specific inputs. It can eliminate candidates whose documented contracts or compliance posture don't meet the boundary. The remaining decision requires a replayable evaluation against representative material.

## Encode idempotency outside the provider adapter

The following Go sketch is deliberately narrower than a complete client. It shows the interface that a Node.js orchestration service can mirror: the generator proposes a value, while the service controls reservation, validation, and commit. Keeping those responsibilities apart prevents a provider SDK type from becoming the audit model.

```go
package summary

import (
	"context"
	"errors"
	"strings"
)

var (
	ErrInProgress = errors.New("summary operation is in progress")
	ErrContract   = errors.New("summary violates contract")
)

type Request struct {
	OperationKey  string
	Source        string
	SchemaVersion string
	PromptVersion string
}

type Candidate struct {
	Title        string
	Bullets      []string
	KeyTakeaways []string
	ModelID      string
}

type Generator interface {
	Generate(context.Context, Request) (Candidate, error)
}

type Store interface {
	Reserve(context.Context, string) (existing *Candidate, acquired bool, err error)
	Commit(context.Context, string, Candidate) error
}

func Validate(c Candidate) error {
	if strings.TrimSpace(c.Title) == "" {
		return ErrContract
	}
	if len(c.Bullets) < 1 || len(c.Bullets) > 7 {
		return ErrContract
	}
	if len(c.KeyTakeaways) < 1 || len(c.KeyTakeaways) > 5 {
		return ErrContract
	}
	items := append(append([]string{}, c.Bullets...), c.KeyTakeaways...)
	for _, item := range items {
		if strings.TrimSpace(item) == "" {
			return ErrContract
		}
	}
	return nil
}

func Run(ctx context.Context, store Store, gen Generator, req Request) (Candidate, error) {
	existing, acquired, err := store.Reserve(ctx, req.OperationKey)
	if err != nil {
		return Candidate{}, err
	}
	if existing != nil {
		return *existing, nil
	}
	if !acquired {
		return Candidate{}, ErrInProgress
	}

	candidate, err := gen.Generate(ctx, req)
	if err != nil {
		return Candidate{}, err
	}
	if err := Validate(candidate); err != nil {
		return Candidate{}, err
	}
	if err := store.Commit(ctx, req.OperationKey, candidate); err != nil {
		return Candidate{}, err
	}
	return candidate, nil
}
```

Production code also needs an explicit terminal transition for refusal and contract rejection, plus lease expiry or ownership recovery for abandoned reservations. The commit must be conditional on reservation ownership. Otherwise an old worker can wake after its lease expires and overwrite a result produced by its successor — a classic fencing failure that becomes especially hard to diagnose when both objects are valid JSON.

The audit row should contain the operation key, source digest, schema and prompt versions, model identifier, adapter version, timestamps, terminal state, and a reference to the accepted object. Usage metadata is useful for cost analysis. Raw prompts, raw provider envelopes, and validation excerpts belong in a more restricted store, if policy permits storing them at all; routine application logs are a poor evidence vault because access and retention are commonly broader than the underlying content warrants.

## Compare guarantees and failure ownership

No single mechanism wins every workload. The useful comparison asks where malformed structure is prevented, where it is detected, and who owns recovery.

| Mechanism | What it establishes | Application responsibility | Suitable use | Limitation |
|---|---|---|---|---|
| Schema-constrained output | Generation is bound to a documented schema subset | Local validation, semantic checks, refusal handling, versioning | Unattended typed consumers | The supported subset and refusal behavior must match the contract |
| Function or tool arguments | Structured arguments are associated with a declared operation | Argument validation, dispatch authorization, side-effect idempotency | A workflow that may invoke application capabilities | Adds orchestration concepts to a summary-only path |
| JSON-only output | A response is syntactically JSON under the documented mode | Full shape validation and a repair or rejection policy | Experiments with fluid contracts | Valid JSON can have the wrong fields and types |
| Prompted prose | The model receives formatting instructions | Parsing, ambiguity handling, human review | Editorial drafts with a person in the loop | Not suitable for unattended writes into typed systems |
| Locally hosted generation | Processing can remain inside an approved environment | Serving, capacity, model evaluation, and all contract enforcement | Restricted data or mandated locality | Operational ownership moves to the engineering team |

My default decision rule is conditional, not a ranking: prefer schema-constrained output when downstream automation requires stable fields, provided the schema subset and refusal semantics pass the corpus tests; retain JSON-only or prompted prose when a human reviews each result and the format changes frequently; choose local hosting when policy rules out the external trust boundary. The catch is that a rigid schema adds migration work. It's a poor bargain for exploratory editorial tools whose consumers can tolerate prose and whose shape changes every few days.

Cost should be measured across the system rather than quoted per call. Capture input and output volume, retries, contract rejections, human review, storage, and idle serving capacity under the same corpus. A low unit price can be erased by repair loops; local infrastructure can be uneconomic at low utilization. I wouldn't declare a winner without workload traces, and your mileage may vary as document length and concurrency change.

## Roll out through replay, shadowing, and reconciliation

Build the fixture corpus before moving traffic. Include empty and whitespace-only input, very long documents, Unicode, duplicate passages, source text containing instructions aimed at the model, documents with no defensible takeaway, and sensitive-field redaction cases. Assert transport-schema validity and domain invariants separately from factual quality. Structural success does not mean the summary is true.

Run each candidate adapter against the same immutable corpus and preserve the contract version, configuration, terminal state, latency, and usage metadata. Review unsupported schema keywords and refusal paths explicitly rather than inferring them from successful examples. Then shadow a small slice of production input into a separate result namespace: shadow output must never publish, send a message, or satisfy the primary operation key. Observe latency distributions, refusal counts, schema rejections, retry counts, output sizes, and human overrides by version. No mystery score.

Promotion should be reversible. Route a small percentage to the new adapter, retain a reader for the previous schema during the agreed migration window, and roll back by changing routing rather than rewriting accepted records. Reconciliation then compares reserved operations with terminal outcomes, checks that each published artifact refers to exactly one accepted result, and replays only states whose transition rules permit it. A client timeout is evidence of uncertainty, not evidence that generation failed.

The final selection is the API that clears this process with the least compensating machinery while meeting data-governance constraints. That answer may change as schemas, models, retention requirements, and corpus composition change; the stable asset is the application-owned contract and its audit trail.

## References

- OpenAI, "Function calling": https://platform.openai.com/docs/guides/function-calling
- ElevenLabs documentation: https://elevenlabs.io/docs
