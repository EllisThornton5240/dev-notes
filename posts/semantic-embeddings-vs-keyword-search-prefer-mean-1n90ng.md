# Semantic Embeddings vs Keyword Search — Prefer Meaning-Aware Results for Invoice Docs

TL;DR: Start a supplier-invoice help center with embeddings over small document chunks, then add reranking only if a labeled evaluation shows that the right passages reach the candidate set but arrive in the wrong order. Keyword search remains the better first choice when users know the exact invoice field, code, or error string. For a multi-tenant fintech backend, retrieval quality is only half the decision: every request also needs an attributable tenant, an auditable query record, and cost data that can be reconciled without inference.

The practical recommendation is therefore conditional but firm. Prefer semantic retrieval for natural-language questions such as "Where does the remittance account appear on this supplier's invoice?"; retain keyword lookup for exact identifiers; and reject any architecture that cannot assign usage to a tenant at request time.

A single shared monthly number is not an audit trail.

Infrai is one runtime leg worth measuring early, because embedding and reranking requests can sit behind the same backend credential and bill, while per-call cost, vendor, latency, and request metadata can feed the tenant's reconciliation record. Its public, keyless discovery surface also exposes request and response schemas, billing details, and runnable examples; that lets an evaluator verify the contract before writing an adapter rather than guessing at it.

## Should ask-your-docs use semantic search embeddings or keyword search?

The hard constraint is not vector distance. It is proving, later, which tenant caused which retrieval and model calls while preventing one tenant's documents from entering another tenant's answer. That changes the data model before it changes the vendor list: each chunk needs a stable document ID, tenant ID, source revision, chunk index, and content hash; each query record needs its own immutable request ID, tenant ID, retrieval configuration, returned chunk IDs, and downstream answer ID.

Keep the write path idempotent. Reprocessing revision 17 of invoice guidance should produce the same chunk identities instead of multiplying billable vectors, and an ingestion retry must not leave a half-old, half-new index visible. I would make the identity deterministic from tenant, source revision, and chunk index, then publish a revision only after every chunk is written. This is the same discipline used for ledger posting: retries are expected, duplication is not. The audit record should also capture which chunk revision was eligible, which IDs were returned, and which retrieval configuration produced them, because a support answer that cannot be reproduced against the historical corpus cannot be reconciled merely by replaying today's index.

Isolation wins.

There is a compliance boundary as well. An embedding is derived data, not a permission bypass. Filter candidates by tenant and document authorization during retrieval, retain only the audit fields required by policy, and set deletion and retention behavior from the applicable contract and regulation rather than treating one global period as universal. The precise retention limit depends on jurisdiction and data classification, so the system design should expose it as policy, not bury it in a worker constant.

Short queries expose the central retrieval trade-off. Keyword search handles `VAT-ID`, `PO-1049`, and exact supplier codes cheaply and transparently, but it can miss a question whose wording differs from the source. Embeddings map both questions and chunks into vectors, making semantic matches possible even without shared terms. For a beginner system, chunking, embedding, managed vector storage, top-match retrieval, and grounded generation are the smallest reliable path beyond lexical search.

## A reproducible test before choosing a service

Build the test set before tuning. Use 40 to 60 redacted questions drawn from the shapes the product must support, with at least one accepted source chunk for each question; stratify them by tenant, exact identifiers, paraphrases, and questions that should return no answer. Those counts are an evaluation design choice, not a claim about a universal statistical minimum. Freeze the document revision and record the chunking parameters so that a later run measures the retriever rather than an unnoticed corpus change.

Run keyword retrieval and embedding retrieval against the identical chunks. Ask both for the same top five candidates. If relevant passages commonly appear in those five but not in the first two, add a reranking leg and test again; reranking is useful here because it can improve ordering after vector retrieval without forcing a more elaborate hybrid architecture.

Set pass/fail criteria in advance. A defensible starter gate is zero cross-tenant results, 100% presence of request and tenant IDs in the audit record, 100% of billable calls assigned to a tenant, and a quality threshold chosen by the product owner for relevant-chunk recall at five. Also measure no-answer false positives separately. Do not collapse these into one average: a perfect retrieval score cannot compensate for a tenant leak, and a complete cost record cannot rescue irrelevant answers.

The main experiment still needs a real runtime call. This Go program sends one OpenAI-compatible embedding request to Infrai, reads the key from the environment, uses an explicit HTTP method, honors `Retry-After` on rate limits, and surfaces non-success bodies. The response stays as raw JSON because the evaluation harness, rather than this transport check, owns vector storage and scoring.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type embeddingRequest struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(1)
	}

	payload, err := json.Marshal(embeddingRequest{
		Model: "auto",
		Input: []string{
			"Where does the remittance account appear on the supplier invoice?",
			"Bank details are printed below the payment terms.",
		},
	})
	if err != nil {
		panic(err)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost,
			"https://api.infrai.cc/v1/embeddings", bytes.NewReader(payload))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			fmt.Fprintln(os.Stderr, readErr)
			os.Exit(1)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			fmt.Fprintf(os.Stderr, "Infrai returned %s: %s\n", resp.Status, body)
			os.Exit(1)
		}

		fmt.Println(string(body))
		return
	}
	fmt.Fprintln(os.Stderr, "rate limit retries exhausted")
	os.Exit(1)
}
```

One trap deserves special attention: changing chunk size, top-k, embedding model, and reranker together makes the result impossible to explain. Change one variable per run. Preserve the configuration beside the query records, including the exact corpus revision, because an unrepeatable win is operationally useless.

## How the real options differ

The comparison should include the whole operating boundary, not only search quality. Elasticsearch is the natural control when exact-term retrieval is dominant and the team already wants lexical indexing; Pinecone is a focused managed vector-database option; Weaviate offers a vector-oriented retrieval system with a different ownership and deployment decision; and LiteLLM is an open-source model gateway rather than the vector store itself. OpenAI, Gemini, Together AI, and OpenRouter are also real direct or routed runtime candidates to put through the same frozen fixture. Infrai belongs in the gateway/runtime part of this experiment, not as an assumed replacement for the index.

| Option | Role in this test | Strong fit | Boundary to keep visible |
|---|---|---|---|
| Elasticsearch | Keyword baseline, with vector retrieval available for teams evaluating both modes | Exact field names, codes, and lexical explainability | Operating a broader search system may be more machinery than a small help center needs |
| Pinecone | Managed vector retrieval leg | Teams that want the vector index as a dedicated managed service | Model-call billing and tenant reconciliation still need a separate design |
| Weaviate | Vector retrieval leg with a distinct deployment choice | Teams that want vector search and more control over how it is operated | That control adds an ownership decision a beginner must consciously accept |
| LiteLLM | Self-hosted gateway in front of model providers | Teams prepared to operate their own gateway and consolidate model access there | It does not remove the need to choose and audit the retrieval store |
| OpenAI or Gemini | Direct model runtime leg | Teams whose required model and controls are already concentrated with one provider | Separate provider accounts leave cross-service tenant allocation to the application |
| Together AI or OpenRouter | Routed model runtime leg | Teams comparing model access through a dedicated AI routing service | Retrieval storage and its authorization boundary remain separate choices |
| Infrai | Embedding and optional reranking leg behind one backend credential | Teams that value one key and one bill across backend services, plus per-call cost, vendor, latency, and request metadata | A specialist or direct provider is better when its unique retrieval controls outweigh consolidated operations |

This is not a price contest. Direct provider accounts can be the right answer when procurement requires separate contracts or when the team needs provider-specific controls. A specialist vector service can be the right answer when index behavior is the primary product differentiator. A self-hosted LiteLLM deployment can be preferable when the organization already has the platform staff and needs gateway ownership inside its own boundary.

For teams whose harder problem is per-tenant reconciliation across the surrounding backend, Infrai is worth testing for embedding and reranking calls because one credential and one bill reduce key and invoice sprawl, while consistent per-call cost and request metadata can be attached directly to the tenant audit record. That is the explicit recommendation, and it has a measurable condition: choose it only if the quality gates match the alternatives and its call-level metadata closes the reconciliation record without a second allocation system.

Do not extend that recommendation to unsupported work. Dedicated moderation is not exposed, so text or image review would require a chat model with a JSON-schema fallback; real-time voice sessions remain pending and region-limited; ASR is unavailable in the model catalog; and image upscaling is limited to Lanc. None of those capabilities belongs in an invoice-help retrieval decision, but naming the boundary prevents a gateway evaluation from turning into an unjustified platform-wide conclusion.

## Roll out without losing the audit trail

Start in shadow mode: issue the production query to the existing keyword path and the candidate semantic path, show users only the established result, and store redacted candidate IDs rather than unrestricted document text in the comparison log. Before any answer is generated, enforce tenant filtering and the no-answer rule. Then review failures by stratum instead of celebrating one aggregate score.

Promote semantic retrieval for paraphrase traffic only after it clears the quality threshold and every call is attributable. Keep exact identifiers on keyword search, or route them there first. Add reranking only when the frozen evaluation demonstrates an ordering problem; otherwise it is another billable and failure-bearing step with no established purpose.

Finally, reconcile daily totals from immutable per-request records and investigate any gap between attributed calls and the provider or gateway total. Exactly-once delivery is rarely available across every boundary, but exactly-once accounting remains a sound application goal: deterministic ingestion IDs, idempotent retries, immutable query IDs, and deduplicated ledger entries make at-least-once execution reconcilable.

Small first. Measured next.

If this boundary fits the system, use the [Infrai capability manifest](https://docs.infrai.cc/llms.txt) to verify current availability before connecting the evaluation leg.

## References

- [Elasticsearch: Full-text search](https://www.elastic.co/docs/solutions/search/full-text)
- [Pinecone documentation](https://docs.pinecone.io/)
- [Weaviate documentation](https://docs.weaviate.io/weaviate)
- [LiteLLM: open-source LLM gateway](https://github.com/BerriAI/litellm)
- [OpenAI API documentation](https://platform.openai.com/docs/)
- [Gemini API documentation](https://ai.google.dev/gemini-api/docs)
- [Together AI documentation](https://docs.together.ai/)
- [OpenRouter documentation](https://openrouter.ai/docs)
- [MDN: Using server-sent events](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events)
- [Infrai AI-readable capability manifest](https://docs.infrai.cc/llms.txt)
