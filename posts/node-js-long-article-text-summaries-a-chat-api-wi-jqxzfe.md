# Node.js Long-Article Text Summaries: A Chat API with Typed JSON Results

Use a Node.js text summarization API as a small, auditable job system: accept a long article, produce typed JSON through bounded chat-completions calls, and publish only after validation and an idempotent commit. The deciding constraint is not how fluent the prose sounds; it is whether a retry, a partial chunk failure, or a reconciliation query can leave two different durable answers for one input.

That is the architecture decision record. The external model call is an attempt. The database record is the effect. Keeping those two facts separate is what makes an article digest defensible in a payment or ledger environment, where an approximate “exactly once” promise is still a bug in the design.

## What should a Node.js text summarization API guarantee for long-article JSON output?

Start with four invariants. The normalized article, prompt version, and schema version determine one job key. A result is accepted only when strict decoding and semantic checks pass. A merge record names every ordered chunk that contributed to it. Publication is atomic: callers receive either a complete summary or a state they can safely poll, never a convincing prefix.

The HTTP layer can be Node.js, but it should delegate to an orchestration module with explicit states such as `claimed`, `generated`, `validated`, and `committed`. A timeout after a remote call does not prove that the call failed. On retry, load the committed job first, then claim work with a unique constraint. If another worker owns the key, return a conflict or a pollable status rather than silently starting a second logical job.

Short version: retries are normal; duplicate durable effects are not.

No shortcuts.

Validation needs more than `JSON.parse`. Reject unknown fields if the contract requires a closed shape, enforce bounded string and array lengths, and check that every citation or section identifier refers to the source material supplied to the job. Store the input digest and prompt/schema versions with the result. Compliance may require redaction before dispatch and retention limits for raw text, so the audit trail should preserve hashes and decision metadata when it cannot retain the document itself.

## Which pipeline shape survives retries and partial failures?

There are three useful execution shapes, and the right one depends on latency, context size, and audit granularity rather than on a fashionable API feature.

| Shape | Appropriate boundary | Failure to model | Evidence to retain |
|---|---|---|---|
| Single call | The measured input fits with instruction and output headroom | One attempt may finish after the client disconnects | Job key, request hash, validated result |
| Chunk, then merge | The article exceeds allowance or chunks can be retried independently | Sibling chunks can finish in different orders | Chunk index, digest, prompt version, merge lineage |
| Asynchronous batch | Backfills where a caller does not need an immediate response | Submission and collection are separate transitions | Batch identity, item status, reconciliation log |

Chunking by characters is a coarse admission check, not a token budget. Use the tokenizer and limits for the selected model, reserve room for instructions and the JSON response, and prefer paragraph or section boundaries. Stable indexes matter more than clever overlap: they let a replay prove that chunk 4 was summarized from the same bytes as the original run. Consider a 30-section annual report in which section 12 contains a risk qualification for a number introduced in section 3. If the worker retries section 12 with a different prompt revision, the merge must reject the mixed lineage or record that revision explicitly; otherwise a later auditor cannot tell whether the final sentence came from the source, the first attempt, or the retry. The same rule applies when a worker receives a cancellation after the provider has accepted the request: mark the attempt as indeterminate, reconcile it by job key, and do not infer failure from the missing HTTP response. This is a longer path through the state machine, but it is the path that prevents a plausible summary from becoming an untraceable accounting statement.

The merge prompt should preserve contradictions and provenance. A later section can qualify an earlier claim, and a naive vote across chunk summaries can erase that qualification. Keep a compact source map in the internal record even if the public JSON contains only the final headline, abstract, and key points.

Batch work is not suitable for an interactive endpoint with a strict response deadline; use a bounded synchronous path when the caller is waiting. Conversely, do not hold an HTTP connection open during a large backfill. The OpenAI Batch API guide documents the operational distinction between submission and later retrieval, which is a useful generic model for any provider-independent worker queue.

## How do you implement the critical path without double writes?

The following Go fragment represents the transaction boundary behind a Node.js-facing service. It deliberately keeps generation behind an interface, so tests can return malformed JSON, a timeout, or a valid response twice without calling a live service.

```go
package summary

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
)

type Result struct {
	Headline string   `json:"headline"`
	Abstract string   `json:"abstract"`
	Points   []string `json:"points"`
}

type Generator interface {
	Generate(context.Context, string, string, string) ([]byte, error)
}

type Store interface {
	Committed(context.Context, string) (Result, bool, error)
	Claim(context.Context, string) (bool, error)
	Commit(context.Context, string, Result) error
}

var ErrInProgress = errors.New("summary job is already in progress")

func Summarize(ctx context.Context, store Store, generator Generator, article string) (Result, error) {
	const promptVersion = "digest-v1"
	const schemaVersion = "summary-v1"
	normalized := strings.TrimSpace(strings.ReplaceAll(article, "\r\n", "\n"))
	digest := sha256.Sum256([]byte(normalized + "|" + promptVersion + "|" + schemaVersion))
	jobKey := hex.EncodeToString(digest[:])

	if result, ok, err := store.Committed(ctx, jobKey); err != nil {
		return Result{}, err
	} else if ok {
		return result, nil
	}
	claimed, err := store.Claim(ctx, jobKey)
	if err != nil {
		return Result{}, err
	}
	if !claimed {
		return Result{}, ErrInProgress
	}

	raw, err := generator.Generate(ctx, normalized, promptVersion, schemaVersion)
	if err != nil {
		return Result{}, err
	}
	var result Result
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	if err := decoder.Decode(&result); err != nil || result.Headline == "" || result.Abstract == "" || len(result.Points) == 0 {
		return Result{}, errors.New("generated JSON failed the summary contract")
	}
	if err := store.Commit(ctx, jobKey, result); err != nil {
		return Result{}, err
	}
	return result, nil
}
```

The database must enforce uniqueness on `jobKey`; an in-memory mutex is not an audit control once multiple Node.js processes or workers exist. A production contract should also record model configuration, chunk lineage, and a validation outcome, with sensitive payloads subject to the applicable retention and access policy.

## What is the rejected option, and when is it still valid?

I would reject “one giant prompt, parse whatever comes back, and overwrite the row on retry.” It has no stable boundary for partial work, makes schema drift invisible, and turns a client timeout into a duplicate write. It is acceptable only for a disposable preview whose output is explicitly non-authoritative and never enters a ledger, customer notification, or compliance record.

The catch is operational cost and complexity: chunk-and-merge creates more records, more latency variance, and more places to monitor. Stick with one bounded call when measured context headroom is ample and the result is low consequence. Choose an asynchronous worker when throughput matters more than immediate response. Your mileage may vary by document genre; legal and financial text deserve stricter provenance than casual notes.

## References

- https://platform.openai.com/docs/guides/batch
- https://www.promptingguide.ai
- https://www.rfc-editor.org/rfc/rfc9110
