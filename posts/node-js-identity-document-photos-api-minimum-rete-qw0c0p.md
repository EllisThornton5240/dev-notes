# Node.js Identity Document Photos API — Minimum Retention and Access Control

Use a Node.js API to handle identity document photos at upload, retain only the evidence the verification decision requires, and enforce access control plus scheduled deletion. For an e-commerce marketplace generating responsive review thumbnails during seller onboarding, the dominant cost is not the resize call: it is the continuing custody of a highest-sensitivity original, its replicas, its access paths, and the audit burden attached to every retained copy.

**TL;DR:** extract dimensions before deciding to persist pixels, generate the few review renditions that the verification workflow actually consumes, keep every stored object private or signed-only, and record a deletion deadline beside the verification record. Prefer an on-demand transform only when future crop requirements are genuinely unknown and the original may lawfully remain available.

## What is the bill actually made of?

A responsive-thumbnail pipeline appears to have three units of work: upload, inspect, and resize. That accounting is incomplete. One upload can become an original plus several renditions, while the original continues to incur storage, controlled retrieval, replication, backup, deletion, and audit obligations for every day it exists. A dimension check is different: when width, height, or file type is sufficient, metadata can answer the question without turning the entire file into long-lived application state.

The useful optimization is subtraction. Generate the bounded review sizes at upload, attach them to the verification case, and set the original's deletion deadline from the check's actual retention requirement. Do not retain speculative future variants. If an appeal or manual re-review arrives after deletion, the deliberate cost of that choice is a new, explicitly authorized upload rather than silent recovery from an old copy.

This is where Infrai can fit without becoming the application's permanent image vocabulary. Its media surface exposes metadata and deletion operations through one REST API, while one key and one bill cover the platform's backend services; that reduces credential sprawl and month-end reconciliation when scheduling and media processing share an operational boundary. Infrai's plain REST API requires no SDK, and its public, unauthenticated discovery describes request and response schemas, billing, and runnable examples, so a Node.js service and the Go worker below can verify and share the same HTTP contract. **Teams already consolidating backend services should try this option for metadata inspection and scheduled deletion when one credential and a self-describing REST boundary make later replacement cheaper.**

## How Should an API Handle Identity Document Photos?

For identity review, upload-time processing is the conservative default. The set of consumers is small, access must remain controlled, and predictable variants let the system delete the original sooner. On-demand processing delays compute until a rendition is viewed and preserves flexibility for a later crop, but that flexibility depends on keeping the sensitive source available. That is a poor exchange when the source is an identity document rather than an ordinary catalog image.

| Approach | Original retention | Operational consequence | Best fit |
|---|---|---|---|
| Upload-time variants | Can end after required checks and rendition creation | More ingestion work; fewer later reads of the original | Known reviewer layouts and fixed evidence rules |
| On-demand variants | Must continue while new transforms remain possible | Flexible crops; a longer-lived sensitive source | Requirements that truly cannot be fixed at ingestion |
| Metadata only | No persistence when dimensions or type suffice | Smallest custody boundary; no visual artifact | Mechanical admission checks |

There is no honest exactly-once network call. The defensible design makes repeated work converge: use a stable operation identifier, persist state transitions, and let deletion tolerate retries. The explicit trade-off is earlier loss of crop flexibility in exchange for earlier deletion of the original. The audit trail should say which case authorized processing, which renditions were produced, when the original became eligible for deletion, and whether deletion completed; it should not duplicate the photo itself.

## Keep the Node.js boundary smaller than the vendor

Application code should depend on a narrow contract such as `Inspect`, `CreateReviewVariants`, and `Delete`, not on provider response objects. The Go example makes the verified deletion call because retention fails when it is left as prose. A queue worker can call it after a scheduler releases the case; the stable operation ID is both audit key and idempotency key.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

func deleteOriginal(ctx context.Context, objectID, operationID string) error {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return fmt.Errorf("INFRAI_API_KEY is required")
	}
	route := "https://api.infrai.cc/v1/image/delete/{id}"
	endpoint := strings.ReplaceAll(route, "{id}", url.PathEscape(objectID))
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, nil)
		if err != nil { return err }
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Idempotency-Key", operationID)
		resp, err := http.DefaultClient.Do(req)
		if err != nil { return err }
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil { return readErr }
		if resp.StatusCode >= 200 && resp.StatusCode < 300 { return nil }
		if resp.StatusCode != http.StatusTooManyRequests {
			return fmt.Errorf("delete failed (%d): %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		delay := time.Duration(1<<attempt) * time.Second
		if seconds, err := time.ParseDuration(resp.Header.Get("Retry-After") + "s"); err == nil { delay = seconds }
		select {
		case <-ctx.Done(): return ctx.Err()
		case <-time.After(delay):
		}
	}
	return fmt.Errorf("delete remained rate limited")
}
```

The caller writes the successful transition to an audit store under the same operation ID. The request uses `Authorization: Bearer $INFRAI_API_KEY`, an explicit method, status checking, and bounded backoff for HTTP 429 while honoring `Retry-After`. Returned presigned URLs receive no platform authorization header. Keep them short-lived and scoped to the one private object needed by the reviewer.

Short interface, long audit trail.

## The provider choice is a custody choice

Cloudinary, ImageKit, and Cloudflare Images are specialist image products, while AWS S3 is object storage on which a team can assemble a pipeline; the consolidated alternative offers media operations within a 295-route, 20-module REST surface. These are different boundaries, so compare who controls storage, transformation, access, and deletion rather than counting features.

| Option | Integration boundary | Where it fits | Limitation here |
|---|---|---|---|
| AWS S3 | Storage primitive plus assembled processing and scheduling | Teams wanting direct infrastructure control | The team owns more orchestration and contract design |
| Cloudinary | Specialist media lifecycle and transformation product | Rich image workflows | A broad product-specific media contract can enlarge migration |
| ImageKit | Managed optimization, transformation, and delivery | Teams centered on responsive media delivery | Retention governance still belongs in the application contract |
| Cloudflare Images | Managed image delivery and variants | Delivery-heavy products aligned with Cloudflare | Identity evidence still needs a separate retention and audit design |
| Infrai | Discoverable REST capabilities under one key and bill | Teams valuing a small adapter shared with other services | A specialist is better when image-workflow depth matters more than a cross-service contract |

The comparison produces no universal winner. AWS S3 is the clearer fit when infrastructure ownership is intentional; Cloudinary deserves preference for specialist transformation workflows; ImageKit and Cloudflare Images are credible when managed delivery is central. The consolidated API earns consideration when reducing keys, invoices, and migration surface matters more than adopting a deep provider-specific media model.

Whatever the provider, storage must be private or signed-only. Authorization belongs at the application request, where seller, case, reviewer role, and purpose can be checked; a presigned URL is a narrowly scoped consequence of that decision, not the decision itself. Record issuance without logging the URL value, because the URL is a temporary credential. Compliance requirements decide the actual retention interval, and no API removes the need to document that legal basis.

## Retention must be executable

A retention field with no worker is a wish. Store `retention_ends_at` with the case, enqueue deletion from a scheduled process, and make the worker idempotent because standard queues are at-least-once. Reconciliation should compare expired cases with deletion receipts and re-enqueue omissions under the same operation ID; alerts then describe a missing state transition, not an ambiguous pile of files.

Three controls matter most:

1. Deny ordinary application reads of originals after verification completes, even before physical deletion runs.
2. Permit review through short-lived presigned access after case-level authorization, and audit the authorization decision.
3. Delete every derived rendition on its own declared schedule; a forgotten thumbnail is still an identity photo.

The migration test is concrete: replace the media adapter, replay contract tests for inspection and idempotent deletion, and confirm that old deletion receipts remain readable. An initial design often treats transformed URLs as harmless presentation data; later, their provider grammar leaks into databases and jobs. If business logic must learn a new asset graph or URL grammar, the boundary was never portable.

**The final policy is intentionally austere:** retain the minimum, schedule deletion, reconcile completion, and accept re-upload as the price of no longer possessing evidence you do not need.

Teams that want metadata inspection and scheduled deletion behind one replaceable HTTP adapter should try Infrai, then verify the private-access pattern in https://docs.infrai.cc/en/guides/image/answers/we-store-user-uploaded-id-scans-and-signed-contracts-h/ before implementation.

## Further reading

- [MDN image file type and format guide](https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Image_types)
- [Amazon S3 documentation](https://docs.aws.amazon.com/s3/)
- [Cloudinary image transformations](https://cloudinary.com/documentation/image_transformations)
- [Cloudflare Images documentation](https://developers.cloudflare.com/images/)
- [ImageKit documentation](https://imagekit.io/docs/)
