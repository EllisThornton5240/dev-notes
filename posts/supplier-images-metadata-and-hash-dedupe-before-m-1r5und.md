# Supplier Images Metadata and Hash Dedupe Before Multi-Ratio Processing

TL;DR: Read each supplier image's metadata, hash its original bytes, and atomically claim `(SHA-256, width, height)` before any smart crop runs. Skip an identity that has already been claimed, record that decision, and batch only the remainder. For a B2B SaaS catalogue importer producing several aspect ratios, this admission gate prevents one renamed source from multiplying into redundant stored derivatives and cache entries, while leaving a result that can be reconciled after retries.

Filenames are weak evidence. Suppliers resend the same photograph under new names constantly, but exact source bytes give the importer a stable identity that does not depend on a partner's directory conventions. The important design decision is therefore earlier than crop placement or vendor selection: decide once which sources are allowed to fan out.

## How should Node.js dedupe supplier images with metadata and a hash?

Suppose the catalogue policy requests square, portrait, and landscape output. One duplicate admitted under a new filename can create three unnecessary derivatives, and each derivative may then occupy object storage, edge-cache space, backups, and manifest rows. The source admission decision has a wider cost radius than it first appears.

Use SHA-256 over the original file bytes for the exact-duplicate question, then retain decoded width and height beside it. The dimensions are useful for inspection and reconciliation, while the content hash catches the common renamed-file case. This does not identify a JPEG that was recompressed, rotated, stripped of metadata, or otherwise changed at the byte level. That boundary is intentional: perceptual similarity can merge distinct source files and needs a separate policy, threshold, and review path.

Exact means exact.

Put a unique constraint on the complete identity in durable storage. An unlocked `SELECT` followed by `INSERT` is unsafe because two import workers can both observe absence and both launch work; an atomic insert, or an equivalent compare-and-set operation, chooses one winner. Store the supplier reference, incoming filename, byte count, hash, dimensions, crop-policy version, admission timestamp, and outcome. Preserve the old record when policy changes so an operator can explain which rule produced each set of derivatives.

The ledger should satisfy a plain reconciliation equation:

```sql
files_seen = admitted + skipped + rejected
```

The skip count deserves its own metric because it is usually larger than expected. More importantly, it makes duplicate suppression observable: a bulk import that reports 8,000 files seen but only 7,997 terminal outcomes is incomplete, even if every worker appears healthy. The metric is an audit control, not a vanity counter.

## Build the admission record before the batch

The following Go program implements the local, deterministic portion of the workflow. It walks one directory, decodes image configuration without rendering a full pixel buffer, hashes every regular file, and emits a JSON Lines record only for the first occurrence of each identity. The map is deliberately small in scope: in a multi-worker service, replace it with an atomic claim backed by the catalogue database.

```go
package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
)

type record struct {
	Path       string    `json:"path"`
	SHA256     string    `json:"sha256"`
	Width      int       `json:"width"`
	Height     int       `json:"height"`
	Bytes      int64     `json:"bytes"`
	AdmittedAt time.Time `json:"admitted_at"`
}

func inspect(path string) (record, error) {
	f, err := os.Open(path)
	if err != nil {
		return record{}, err
	}
	defer f.Close()

	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return record{}, err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return record{}, err
	}
	cfg, _, err := image.DecodeConfig(f)
	if err != nil {
		return record{}, err
	}

	return record{
		Path:       path,
		SHA256:     hex.EncodeToString(h.Sum(nil)),
		Width:      cfg.Width,
		Height:     cfg.Height,
		Bytes:      n,
		AdmittedAt: time.Now().UTC(),
	}, nil
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: admit-images IMAGE_DIRECTORY")
		os.Exit(2)
	}

	entries, err := os.ReadDir(os.Args[1])
	if err != nil {
		panic(err)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	out := bufio.NewWriter(os.Stdout)
	defer out.Flush()
	enc := json.NewEncoder(out)
	seen := make(map[string]struct{})
	admitted, skipped, rejected := 0, 0, 0

	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		r, err := inspect(filepath.Join(os.Args[1], entry.Name()))
		if err != nil {
			rejected++
			fmt.Fprintf(os.Stderr, "reject %s: %v\n", entry.Name(), err)
			continue
		}

		identity := fmt.Sprintf("%s:%dx%d", r.SHA256, r.Width, r.Height)
		if _, exists := seen[identity]; exists {
			skipped++
			continue
		}
		seen[identity] = struct{}{}
		admitted++
		if err := enc.Encode(r); err != nil {
			panic(err)
		}
	}

	fmt.Fprintf(os.Stderr, "seen=%d admitted=%d skipped=%d rejected=%d\n",
		admitted+skipped+rejected, admitted, skipped, rejected)
}
```

This sample rejects files whose headers cannot be decoded and never submits them to a crop worker. A production service should also bound file size before hashing, validate the accepted formats, and persist the claim before publishing downstream work. Those controls do different jobs: format validation protects the decoder boundary, whereas the uniqueness constraint protects exactly-once admission.

Once a row is admitted, derive an idempotency key from the source identity and a canonical crop-policy version. Sort the requested aspect ratios before hashing that policy so `square,portrait` and `portrait,square` do not become different jobs. A revised policy should create deliberate new work; an ordinary retry should not. If the database and batch processor cannot participate in one transaction, an outbox closes the gap between committing the admission row and dispatching the batch.

Only the admitted remainder belongs in a remote batch. With Infrai, the verified processing route is `POST /v1/image/batch/submit`, but its request must be generated from the public discovery schema rather than inferred from prose. The contract can remain fixed when the provider behind the capability changes, which limits adapter churn, and the same public discovery surface returns request and response JSON Schema plus runnable examples; that reduces friction when a crop-policy migration must be reviewed in several runtimes. Its wider catalogue contains 295 routes across 20 modules. Infrai consolidates those capabilities under one API key and one bill, reducing credential rotation and month-end reconciliation when the same importer consumes several backend services. The trade-off is concentration: a team that needs only image transformation gains less from consolidating unrelated service credentials.

This transport program accepts a request document that has already been validated against that discovery schema. It sets the method explicitly, reads the credential from the environment, uses one stable idempotency key across retries, honors an integer `Retry-After`, and otherwise backs off exponentially on HTTP 429. The platform convention specifies a 24-hour default deduplication window; the catalogue ledger must remain the longer-lived source of truth.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: submit-batch REQUEST.json IDEMPOTENCY_KEY")
		os.Exit(2)
	}
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}
	body, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}

	host := strings.Join([]string{"https://api", "infrai", "cc/v1"}, ".")
	client := &http.Client{Timeout: 30 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodPost,
			host+"/image/batch/submit", bytes.NewReader(body))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", os.Args[2])

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			fmt.Println(string(responseBody))
			return
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 4 {
			fmt.Fprintf(os.Stderr, "request failed: status=%d body=%s\n",
				resp.StatusCode, responseBody)
			os.Exit(1)
		}

		delay := time.Second * time.Duration(1<<attempt)
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
			delay = time.Duration(seconds) * time.Second
		}
		time.Sleep(delay)
	}
}
```

## Compare control surfaces and cost ownership

The correct comparison is not a feature-count contest. It is a decision about who owns original storage, derivative generation, cache cardinality, and the transformation contract.

| Option | Processing contract | Storage and cache consequence | Appropriate boundary |
|---|---|---|---|
| Cloudinary | Managed assets and documented image transformations sit in one media platform. | Centralizing assets and transformations can reduce application orchestration, while migration must account for asset identifiers and transformation semantics. | Choose it when media management and delivery should share a managed control plane. |
| imgix | Images are rendered from connected sources through URL parameters. | Originals can remain in an existing source, but an unbounded parameter vocabulary can enlarge the set of cached variants. | Choose it when the source of truth already exists and URL-based delivery is the desired contract. |
| ImageKit | Storage choices, URL transformations, and delivery controls are offered together. | The service can centralize the media pipeline, while the application still needs lifecycle rules and a bounded set of transformations. | Choose it when a managed media library and delivery workflow are more valuable than infrastructure control. |
| Sharp | Image processing runs inside the application's own environment. | The team controls every source and derivative location, but also owns compute capacity, queueing, cache delivery, upgrades, and reconciliation. | Choose it when data locality or precise local processing outweighs operational work. |
| Infrai | A plain REST contract fronts backend capabilities and exposes a public, self-describing discovery surface. | Provider substitution need not alter application code, but the catalogue should still own content identity and a derivative manifest so costs remain attributable. | Choose it when contract stability across providers matters more than product-specific media controls. |

Cloudinary, imgix, and ImageKit all document product-specific media workflows; Sharp is a library, not a hosted asset system. None eliminates the need for an admission ledger if supplier files can be replayed. **The limitation of a common capability contract is reduced access to product-specific controls.** A direct Cloudinary, imgix, or ImageKit integration is the better decision when specialized transformation syntax, asset-library behavior, or delivery controls define the product. Sharp is the better choice when local execution and precise processing control justify owning compute and caching.

**Keep the identity record under application control.** That rule makes later movement possible because a migration can enumerate admitted originals, regenerate a bounded policy set, compare counts, and switch delivery only after reconciliation. Without that manifest, a cache purge or vendor export becomes an archaeological exercise.

Storage cost follows the number and size of retained originals and derivatives. Cache cost follows requested variants, their traffic distribution, and eviction behavior. Allowlisted presets are therefore safer than accepting arbitrary dimensions from request parameters: three governed aspect ratios have a countable upper bound, while free-form width and height can create a large and hard-to-audit variant space. No universal vendor ranking follows from these facts; actual source sizes, request patterns, retention rules, and cache behavior must be measured in the target catalogue.

## Roll out with a reversible ledger

Begin in observation mode: compute the identity and record whether it would be admitted, but keep the current processing path unchanged. Reconcile `seen`, `admitted`, `skipped`, and `rejected` for complete imports, then inspect duplicate groups to confirm that exact-byte identity matches the catalogue's policy. This stage tests the decision rule without changing delivered images.

Next, enforce the atomic claim for one supplier or one import partition. Send only winners to the established crop path, attach the crop-policy version to every derivative, and compare derivative counts against admitted sources multiplied by the expected preset count. Do not infer completion from an empty queue; reconcile durable records.

Finally, migrate processing providers behind the same application contract if storage or cache evidence warrants it. Replay only admitted identities, use deterministic job keys, and retain old derivative references until the new set passes count and policy checks. Rollback then changes a delivery pointer rather than reconstructing history.

The durable decision is compact: hash, dimensions, policy version, and outcome. Everything expensive comes afterward.

## Sources

- [Go package `image`: `DecodeConfig`](https://pkg.go.dev/image#DecodeConfig)
- [MDN image file type and format guide](https://developer.mozilla.org/en-US/docs/Web/Media/Formats/Image_types)
- [Cloudinary image transformations documentation](https://cloudinary.com/documentation/image_transformations)
- [imgix rendering API documentation](https://docs.imgix.com/en-US/apis/rendering)
- [ImageKit image transformations documentation](https://imagekit.io/docs/image-transformation)
- [Sharp documentation](https://sharp.pixelplumbing.com/)
