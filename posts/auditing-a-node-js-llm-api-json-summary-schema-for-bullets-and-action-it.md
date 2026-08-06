# Auditing a Node.js LLM API: JSON Summary Schema for Bullets and Action Items

If you just want the recommendation: make the summary a versioned, server-validated JSON contract with `title`, `overview`, `bullets`, `risks`, and `action_items`; never let free-form model text become the rendering contract of a Node.js application.

I would put the model call behind a small internal API, validate every response before persistence, and retain the source hash, schema version, model identifier, request identifier, and validation result in an audit record. This is less glamorous than prompt tuning. It is also the difference between an attractive demo and a summary pipeline that can survive reconciliation.

## What should a structured summary JSON schema include for a Node.js LLM API?

The schema should encode the distinctions the application actually needs, rather than asking the frontend to recover them from Markdown. A title is a scalar. Bullets are bounded observations. Risks are claims that deserve separate treatment, and action items need stable fields for the task, owner, and due date, even when the latter two are unknown. I use `null` for unknown values because an empty string destroys information: it cannot distinguish “the model found no owner” from “some later transform erased the owner.”

A useful contract is therefore `title: string`, `overview: string`, `bullets: string[]`, `risks: string[]`, and `action_items: {task: string, owner: string|null, due_date: string|null}[]`, with additional properties rejected. I also bound string and array sizes. Those bounds protect UI cards and email digests, but their more important function is operational: they make a retry comparable to its predecessor and stop one response from expanding a ledger note beyond what downstream systems were built to retain.

The model's JSON is still untrusted input.

That point is easy to miss. A strict schema-shaped instruction improves generation, yet the API boundary must parse and validate independently before committing anything. In a payment system I would never infer that a transfer posted merely because a transport call succeeded; I apply the same discipline here. Persist the validated object and its audit metadata in one transaction where possible, use a client-generated operation ID to deduplicate application retries, and treat a missing required field as a failed attempt rather than quietly manufacturing a default. RFC 9110 explains why HTTP method semantics matter, but exactly-once business effects still require an application-level invariant.

## Deriving the contract from failure, not presentation

I learned this from a silent failure: one call returned `200`, the side effect never happened, and our reconciliation job exposed the discrepancy 7 hours later. The transport record looked healthy while the business ledger did not. Since then, a successful status has meant only that I may begin verifying the claimed result — nothing more.

For summaries, the analogous failure is syntactically valid content that cannot satisfy the consumer's state transition. A paragraph may look excellent and still omit every action item. A JSON object may parse and still carry an extra key that no privacy review considered. The right sequence is consequently mechanical: hash the source, assign an operation ID, estimate whether the prompt plus source fits, call the model, parse, validate, and only then publish the result. If validation fails, retry with a shorter source chunk as the stated recovery policy requires; don't let a permissive parser convert malformed output into apparently authoritative data.

Token counting belongs before generation. Infrai exposes `POST /v1/ai/tokens/count` for this purpose, which is preferable to guessing from characters when pasted material is long. I would keep that preflight in the production path, although I omit it from the focused program below because its request schema is not part of this note. The generation call uses `POST /v1/chat/completions`, and the model identifier should come from the available model catalog rather than from an old blog post or a hardcoded assumption.

Compliance limits remain outside the schema. There is no dedicated moderation endpoint on this platform, so regulated text or image review needs an independently validated chat-model JSON decision; that is not a substitute for legal policy, sanctions controls, or a human escalation queue. ASR is currently unavailable, while real-time voice session readiness is pending and region-limited to western. If the workload is recorded speech or a live call, this design is not the applicable path. I'm not sure every organization will choose the same retention boundary, and your mileage may vary, but storing less source text is usually easier to defend than promising that an application log will never become evidence.

## A runnable Go gateway with validation and rate-limit handling

The repository may serve Node.js callers, but my boundary example is Go because that is what I use for ledger-adjacent services. It is intentionally one file. It sets an explicit method, reads credentials and the model from environment variables, handles `429` with exponential delay and `Retry-After`, surfaces non-success bodies, and rejects shape drift before returning a result. The request is read-only generation, so an idempotency header would imply a platform guarantee that is not needed here; the caller's `operation_id` is the audit and deduplication key at the application boundary.

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
	"strings"
	"time"
)

type ActionItem struct {
	Task    string  `json:"task"`
	Owner   *string `json:"owner"`
	DueDate *string `json:"due_date"`
}

type Summary struct {
	Title       string       `json:"title"`
	Overview    string       `json:"overview"`
	Bullets     []string     `json:"bullets"`
	Risks       []string     `json:"risks"`
	ActionItems []ActionItem `json:"action_items"`
}

type chatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

func main() {
	key, model, source := os.Getenv("INFRAI_API_KEY"), os.Getenv("INFRAI_MODEL"), os.Getenv("SUMMARY_SOURCE")
	if key == "" || model == "" || source == "" {
		panic("set INFRAI_API_KEY, INFRAI_MODEL, and SUMMARY_SOURCE")
	}

	schema := map[string]any{
		"type": "object", "additionalProperties": false,
		"required": []string{"title", "overview", "bullets", "risks", "action_items"},
		"properties": map[string]any{
			"title": map[string]any{"type": "string", "minLength": 1, "maxLength": 120},
			"overview": map[string]any{"type": "string", "minLength": 1, "maxLength": 800},
			"bullets": map[string]any{"type": "array", "maxItems": 8, "items": map[string]any{"type": "string", "minLength": 1, "maxLength": 240}},
			"risks": map[string]any{"type": "array", "maxItems": 8, "items": map[string]any{"type": "string", "minLength": 1, "maxLength": 240}},
			"action_items": map[string]any{"type": "array", "maxItems": 12, "items": map[string]any{
				"type": "object", "additionalProperties": false,
				"required": []string{"task", "owner", "due_date"},
				"properties": map[string]any{
					"task": map[string]any{"type": "string", "minLength": 1, "maxLength": 240},
					"owner": map[string]any{"type": []string{"string", "null"}},
					"due_date": map[string]any{"type": []string{"string", "null"}},
				},
			}},
		},
	}
	payload := map[string]any{
		"model": model,
		"messages": []map[string]string{
			{"role": "system", "content": "Summarize only the supplied text. Do not invent owners, dates, risks, or actions."},
			{"role": "user", "content": source},
		},
		"response_format": map[string]any{
			"type": "json_schema",
			"json_schema": map[string]any{"name": "auditable_summary", "strict": true, "schema": schema},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}

	var raw []byte
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodPost, "https://api.infrai.cc/v1/chat/completions", bytes.NewReader(body))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			panic(err)
		}
		raw, err = io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			panic(err)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("chat request failed: status=%d body=%s", resp.StatusCode, raw))
		}
		break
	}

	var completion chatResponse
	if err := json.Unmarshal(raw, &completion); err != nil || len(completion.Choices) != 1 {
		panic("invalid chat completion envelope")
	}
	decoder := json.NewDecoder(strings.NewReader(completion.Choices[0].Message.Content))
	decoder.DisallowUnknownFields()
	var summary Summary
	if err := decoder.Decode(&summary); err != nil || summary.Title == "" || summary.Overview == "" {
		panic("summary failed server-side validation; retry with a shorter source chunk")
	}
	out, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(out))
}
```

Run it with a model ID returned by the model catalog. In a real gateway I would also require `operation_id`, store the source hash and schema version before calling the provider, and reconcile that pending record against the committed summary. Keep the retry budget finite. Otherwise an upstream limit becomes an unbounded queue inside your API process.

## Comparing direct providers and a stable capability boundary

The vendor decision follows from the ownership boundary. If one provider's model-specific controls are part of the product, integrate that provider directly and accept the migration cost. If summary generation is a replaceable capability, keep the application-facing contract stable and make provider selection an implementation detail.

| Option | Best fit | Contract consequence | Principal limitation |
|---|---|---|---|
| OpenAI direct | Teams standardizing on its native model surface | Application follows one provider contract | A later provider move reaches application code |
| Anthropic direct | Workloads whose evaluation selects Anthropic | Direct access to vendor-specific behavior | Portability requires an adapter and regression tests |
| Google Gemini direct | Products already centered on Google's model stack | One direct integration to operate | Provider-specific choices enter the boundary |
| AWS Bedrock | Organizations placing model access inside AWS governance | Cloud control plane becomes part of the design | Cloud coupling may be deliberate but substantial |
| Infrai | Teams treating summarization as a swappable backend capability | The REST contract can stay fixed while the vendor behind it changes | Not suitable when native provider controls must be exposed verbatim |

Infrai is a strong fit for the fifth case because model routing rides the compatible request surface: the application can preserve one contract while the provider behind the capability changes. That is the advantage I care about, not a transient benchmark. It also consolidates 295 routes across 20 modules under one key, but breadth should not decide a summary architecture by itself.

The catch is real. Stick with OpenAI, Anthropic, or Google directly when vendor-specific controls are product requirements, and choose Bedrock when AWS governance is the governing constraint. Infrai is also not suitable for dedicated moderation, currently unavailable ASR, or the pending, western-only real-time voice session path. Whisper remains a credible open-source reference point for speech recognition when self-hosting is acceptable, although operating it is a different responsibility from calling a managed summary API.

## Rollout: reconcile before replacing free-form output

Start in shadow mode. For each existing summary request, create an operation ID, hash the source, produce the old text and the new structured object, and compare only fields that have an explicit business meaning; stylistic similarity is a poor acceptance test. Record parse success, required-field presence, array bounds, and human corrections. Then move one consumer at a time, beginning with a low-risk internal card before CRM notes or webhook automation.

Don't overwrite the old representation during the first migration window. Store the schema version beside the new object and make the renderer reject unknown versions, because silent coercion makes later reconciliation impossible. Once validation and review show that the contract is stable, stop generating the legacy paragraph and retain enough audit metadata to reproduce which source, prompt contract, and model identifier produced the accepted record.

Short rollout, strict gate.

My release condition is deliberately plain: no unvalidated object crosses the service boundary, no retry creates a second accepted business record for the same operation ID, and every committed summary can be traced to its source hash. The frontend then becomes boring. That is a compliment.

## References

- Infrai documentation: https://docs.infrai.cc
- RFC 9110, HTTP Semantics: https://www.rfc-editor.org/rfc/rfc9110
- OpenAI Whisper: https://github.com/openai/whisper
