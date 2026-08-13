# Test suite for simple-chatgpt

A manual-but-scriptable suite for an agent that has a **Chrome DevTools MCP server** and a
**temporary OpenAI API key** supplied by the user. Everything runs by evaluating JavaScript inside
the live page, so it exercises the real code paths (streaming, IndexedDB, `localStorage`, the
Responses API) rather than a reimplementation.

Two parts:

- **Part 1 — offline** (~34 checks): a stubbed `fetch` returns synthetic SSE streams. **Zero tokens
  spent.** Run this first and completely; most regressions show up here.
- **Part 2 — live** (~9 requests): the real API. Keep it under **$0.10** by using `low` effort,
  one-word answers, and exactly one low-quality image.

`index.html` is the reference implementation. Test it fully, then check that `image.html` matches
where it has the same feature (Part 3).

---

## 1. Setup

```bash
cd /path/to/simple-chatgpt
python3 -m http.server 8901
```

Serve over HTTP, not `file://` — Chrome restricts IndexedDB on `file://`.

Open `http://localhost:8901/index.html` in a **foreground** tab via the MCP `new_page` tool, then
ask the user to type the key into the page's **API key...** field. Never ask for the key in chat and
never print it: read it only as `key.value` / `localStorage.getItem("key")` and never return it from
an `evaluate_script`.

**Validate the key before spending anything** — the field is `type="password"`, so a mistyped
sentence looks identical to a key:

```js
() => ({looksLikeKey: /^sk-/.test(localStorage.getItem("key") || ""),
        length: (localStorage.getItem("key") || "").length})
```

If `looksLikeKey` is false, clear it (`key.value = ""; localStorage.removeItem("key")`) and ask
again. (This has happened: a chat message got typed into the field and every live test 401'd.)

---

## 2. Harness

### Block A — offline stub (paste once per page load)

```js
async () => {
  window.__h = {bodies: [], calls: 0, plan: null};
  window.__realFetch = window.fetch.bind(window);
  window.fetch = async (url, opts) => {
    window.__h.bodies.push(JSON.parse(opts.body));       // never read opts.headers
    window.__h.calls += 1;
    const plan = window.__h.plan(window.__h.calls);
    if (plan === "NETWORK_ERROR") { throw new Error("simulated network failure"); }
    if (plan === "HTTP_401") {
      return new Response(JSON.stringify({error: {message: "Incorrect API key provided."}}), {status: 401});
    }
    if (plan === "HANG") {
      const first = "data: " + JSON.stringify({type: "response.output_item.done",
          item: window.__rs("rs_hang", "EH", "starting to think")}) + "\n\n";
      return new Response(new ReadableStream({
        start(c) {
          c.enqueue(new TextEncoder().encode(first));     // stream is never closed
          if (opts.signal != null) {                      // a real fetch rejects on abort; so must this
            opts.signal.addEventListener("abort", () =>
              c.error(new DOMException("The user aborted a request.", "AbortError")));
          }
        }
      }), {status: 200});
    }
    const text = plan.map(e => "data: " + JSON.stringify(e) + "\n\n").join("") + "data: [DONE]\n\n";
    return new Response(new ReadableStream({
      start(c) { c.enqueue(new TextEncoder().encode(text)); c.close(); }
    }), {status: 200});
  };
  window.__wait = async () => {
    for (let i = 0; i < 400; i += 1) {
      if (activeRequest == null) { return; }
      await new Promise(r => setTimeout(r, 25));
    }
    throw new Error("request never finished");
  };
  window.__reset = async () => {
    localStorage.setItem("history", "");                  // "image-history" in image.html
    await clearGeneratedImages().catch(() => {});
    await clearReasoningTraces().catch(() => {});
    clearAutoSaveDraft();
    await restoreHistory([]);
    window.__h.bodies.length = 0; window.__h.calls = 0;
    model.value = "gpt-5.4"; key.value = "fake"; temp.value = "1";
    reasoningEffort.value = "low";
    webSearch.checked = false; imageGeneration.checked = false;
  };
  window.__rs = (id, enc, summaryText) => ({type: "reasoning", id: id,
    summary: summaryText == null ? [] : [{type: "summary_text", text: summaryText}],
    encrypted_content: enc});
  window.__msg = (id, text) => ({type: "message", id: id, status: "completed", role: "assistant",
    content: [{type: "output_text", text: text}]});
  window.__stream = (items, textDelta) => {              // reasoning/tool items, then text, then the message
    const events = [];
    for (const it of items) { if (it.type != "message") { events.push({type: "response.output_item.done", item: it}); } }
    if (textDelta != null) { events.push({type: "response.output_text.delta", delta: textDelta}); }
    for (const it of items) { if (it.type == "message") { events.push({type: "response.output_item.done", item: it}); } }
    events.push({type: "response.completed", response: {output: items, usage: {output_tokens: 1}}});
    return events;
  };
  return "harness installed";
}
```

`__reset()` sets `key.value = "fake"` but never dispatches an `input` event, so the user's real key
stays in `localStorage` for Part 2. Restore it with `key.value = localStorage.getItem("key")`.

### Block B — live tap (install before Part 2)

```js
async () => {
  window.__tap = {bodies: [], raw: [], usage: []};
  const real = window.__realFetch;
  window.fetch = async (url, opts) => {
    window.__tap.bodies.push(JSON.parse(opts.body));
    const res = await real(url, opts);
    if (!res.ok || res.body == null) {
      const clone = res.clone();
      window.__tap.raw.push(["HTTP " + res.status + " " + (await clone.text()).slice(0, 300)]);
      return res;
    }
    const [pass, copy] = res.body.tee();
    const chunks = []; window.__tap.raw.push(chunks);
    (async () => { const r = copy.getReader(); const d = new TextDecoder();
      for (;;) { const {value, done} = await r.read(); if (done) break; chunks.push(d.decode(value, {stream: true})); } })();
    return new Response(pass, {status: res.status, statusText: res.statusText, headers: res.headers});
  };
  window.__events = i => (window.__tap.raw[i] || []).join("").split("\n")
    .filter(l => l.startsWith("data:")).map(l => l.slice(5).trim())
    .filter(d => d != "" && d != "[DONE]")
    .map(d => { try { return JSON.parse(d); } catch (e) { return {type: "RAW", raw: d.slice(0, 200)}; } });
  window.__live = i => {
    const ev = window.__events(i);
    const done = ev.find(e => e.type == "response.completed");
    const out = done?.response?.output || [];
    const u = done?.response?.usage;
    if (u != null) { window.__tap.usage.push(u); }
    return {outputTypes: out.map(x => x.type),
      reasoningSummaryLengths: out.filter(x => x.type == "reasoning").map(x => (x.summary || []).length),
      reasoningHasEncrypted: out.filter(x => x.type == "reasoning").map(x => typeof x.encrypted_content == "string"),
      errors: ev.filter(e => e.type == "error" || e.type == "response.failed").map(e => JSON.stringify(e).slice(0, 250)),
      httpError: (window.__tap.raw[i] || [])[0]?.startsWith?.("HTTP ") ? (window.__tap.raw[i] || [])[0] : null,
      usage: u == null ? null : {in: u.input_tokens, out: u.output_tokens, reason: u.output_tokens_details?.reasoning_tokens}};
  };
  window.__waitLive = async () => {
    for (let i = 0; i < 1200; i += 1) {
      if (activeRequest == null) { return; }
      await new Promise(r => setTimeout(r, 250));
    }
    throw new Error("live request never finished");
  };
  return "tap installed";
}
```

### Driving a turn

```js
messages.children[i].querySelector(".user-message").textContent = "the prompt";
await send(messages.children[i]);
await window.__wait();        // or __waitLive()
```

Read results with `messages.children[i].querySelector(".ai-message").dataset.rawText` and
`.dataset.reasoningId`. `send()` appends a fresh empty message on success, so turn *n* lives at
index *n*.

---

## 3. Part 1 — offline tests

Group them a few per `evaluate_script` call and return compact booleans/short strings; returning
whole response texts wastes context. Call `await window.__reset()` at the start of each test unless
it deliberately continues the previous conversation.

### Reasoning display

| ID | Setup | Expect |
|---|---|---|
| **A1** | stream `[__rs("rs_1","E1","Weighing options."), __msg("m_1","Yes.")]`, delta `"Yes."` | rawText is `---- Reasoning -----\n\nWeighing options.\n\n---- Response ------\n\nYes.` |
| **A2** | same but `__rs("rs_2","E2",null)` (empty summary) | **both dividers still present**, no summary text, trace saved. This is deliberate: the header is the user's signal that reasoning happened. |
| **A3** | stream `[__msg("m_3","plain")]` only | rawText is exactly `plain`; no dividers; `reasoningId` unset |

### Trace capture and replay

| ID | Setup | Expect |
|---|---|---|
| **B** | two turns, each `[__rs(...), __msg(...)]` | turn 2 `input` shapes = `["role:system","role:user","reasoning","message","role:user"]`; reasoning keys exactly `["encrypted_content","id","summary","type"]` (**no `status`** — the API rejects it); message keys `["content","id","role","type"]`; reasoning sits immediately before the message; `store === false`; `reasoning` param = `{summary:"detailed", context:"all_turns", effort:"low"}`; no `---- Reasoning -----` anywhere in `input` |
| **C** | blank effort field | `reasoning` param = `{summary:"detailed", context:"all_turns"}` (summaries are requested even with no explicit effort) |
| **D** | after B, change `model.value` to a different model | 0 reasoning items sent; assistant turns fall back to plain `{role:"assistant"}` |
| **E** | `model.value = "gpt-4.1"` | no `reasoning` key in the body at all; 0 reasoning items sent |
| **H** | two turns, then re-send turn 0 with edited text | message count drops from 3 to 2; turn 0's `reasoningId` is replaced; the old trace's `encrypted_content` does not appear in the new `input` |

### Interruption and errors

| ID | Setup | Expect |
|---|---|---|
| **F** | plan `"HANG"`, wait for partial text, then `stop(messages.children[0])` | partial reasoning text preserved; button mode goes `stop` → `send`; **no trace saved** (no `response.completed`) |
| **G1** | plan `"HTTP_401"` | rawText is `------ Error -------\n[Error: Incorrect API key provided.]`; no trace |
| **G2** | plan `"NETWORK_ERROR"` | rawText contains `------ Error -------` |

### Inputs and tools

| ID | Setup | Expect |
|---|---|---|
| **I** | `addAttachment` a `.txt` File and a `.png` File, then send | user content types `["input_file","input_image","input_text"]`; `filename` preserved; history `attachments` holds both paths |
| **J** | `imageGeneration.checked = true`, `imageQuality.value = "low"`, stream `[{type:"image_generation_call", id, result: <1x1 PNG b64>}, __msg(...)]`, then a second turn | one `img[data-image-id]` rendered; record in IndexedDB; appears in `buildImageDump`; tool sent as `image_generation` with `quality:"low"`; the next turn re-feeds it as `input_image` |
| **K** | `webSearch.checked = true`, stream `[__rs(...), {type:"web_search_call", action:{sources:[…]}}, __msg("See [a](https://example.com/a?utm_source=chatgpt.com).")]`, then a second turn | `.sources` shows `---- References ----`; hrefs have `utm_source` stripped; the body link is a live `<a>`; the source URLs are **not** in the next `input`; `tool_choice:"auto"`; `include:["web_search_call.action.sources"]` |

1×1 PNG for the image tests:
`iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=`

### Copy buttons and guards

| ID | Setup | Expect |
|---|---|---|
| **L** | `setAiMessageText` with a reasoning block plus 15 000 `x`, then `renderAiSectionButtons` | `textAfterResponseDivider` output excludes the reasoning text; one button (a section extends to the next newline, and there is none) |
| **N** | `splitTextIntoCopySections(("y".repeat(999)+"\n").repeat(30))` | 3 sections; `splitTextIntoCopySections("z".repeat(15000))` → 1 |
| **M** | call `send()` with, in turn: empty key, empty model, empty temp, temp `3`, temp `-1` | `window.__h.calls` stays `0` throughout |

### Persistence, export/import, clear

| ID | Setup | Expect |
|---|---|---|
| **O** | build 2 turns (one with a trace, one with a trace + generated image), build `{version:1, history, db: buildImageDump(...), reasoning: buildReasoningDump(...)}`, then `loadConversationFromFile` on it | no alert; messages, rawText, image, and traces all restored; `loadReasoningTrace(reasoningId)` returns the record; every `encrypted_content` is a **string** (JSON-native, no base64 wrapper needed); a further turn still replays the trace |
| **P** | load a file whose history entries have **no** `reasoningId` and no `reasoning` section | loads with no alert; `reasoningId` unset — treated as generated without reasoning |
| **Q** | load: non-JSON; `version: 2`; a `generatedImages` id missing from `db`; a `reasoning` record whose item lacks `encrypted_content` | all four alert `Format is wrong.` and leave existing state untouched |
| **R** | load a file with a `reasoningId` that has no entry in `reasoning` | still loads (never rejected over a lost trace); `loadReasoningTrace` returns `null` |
| **S** | dispatch `focus`, set 40 chars of text, dispatch `input`, wait ~50 ms | `localStorage["draft-overlay"]` = `{messageIndex, text}`; after `restoreHistory(...)` + `restoreAutoSaveDraft()` the text is back |
| **T** | click `#clear-all` | `localStorage["history"] === ""`; one message left; `draft-overlay` removed; `images` and `reasoning` object stores both count 0; the API key setting is preserved |
| **U** | reload the page (MCP `navigate_page` type `reload`) after a traced conversation | `reasoningId`s restored onto each `.ai-message`; traces still in IndexedDB; `localStorage["history"]` is small (~hundreds of bytes) and contains **no** `encrypted_content` |
| **V** | `openAppDb` upgrade path: close `appDbPromise`, `deleteDatabase`, reopen at version 1 with only an `images` store, put a record, close, then `appDbPromise = openAppDb()` | version becomes 2, stores are `["images","reasoning"]`, the legacy image record survives |

---

## 4. Part 2 — live tests

Install Block B, then `key.value = localStorage.getItem("key")`, `model.value = "gpt-5.4"`,
`temp.value = "1"`. Keep prompts to one line and demand one-word answers. Record
`window.__tap.usage` and report total input/output tokens at the end.

| ID | Effort | Prompt | Expect |
|---|---|---|---|
| **L1** | `low` | `What is 17*23? Reply with only the number.` | output types include `reasoning`; `encrypted_content` present (no `include` parameter needed when `store:false`); both dividers in rawText; trace saved |
| **L2** | `low` | `Repeat that number only.` | **HTTP 200, no `400 Unknown parameter`** — this is the regression that matters; `input` contains a `reasoning` item immediately before a `message` item; no divider text in `input` |
| **L3** | `none` | `Reply with only the word: ok` | request still carries `reasoning:{summary:"detailed",context:"all_turns",effort:"none"}`; earlier traces still replayed; record whether the response contains a `reasoning` item — if it does, the A2 header rule would show dividers with reasoning switched off, which the user does **not** want, and `appendReasoningSummary` needs a guard |
| **L4** | `low` | `web` checked; `What is the current version number of Python? One number only.` then a second turn `Repeat it.` | no orphan-reasoning 400 on the second turn; `---- References ----` populated; note whether the output ends `web_search_call → message` (then no trace is saved for that turn — by design) |
| **L5** | `low` | attach a 1-line `.txt`, ask `Quote the file's only line.` | file content reaches the model; no error |
| **L6** | `low` | `image` checked, quality `low`, size `1024x1024`, `A single red dot on white.` | an image renders and lands in IndexedDB; **the most expensive test — run it exactly once** |
| **L7** | n/a | `model.value = "gpt-4.1"`, `Reply with only: hi` | succeeds with no `reasoning` param; no dividers |
| **L8** | `low` | change `model.value` mid-conversation | no reasoning items sent, no 400 |
| **L9** | n/a | open `http://localhost:8901/index.html?q=Reply%20with%20only%3A%20hi` | auto-sends, forces effort `none`, clears prior history |

**Behavioural proof that traces are actually used** (2 extra requests, worth it once): ask a
question whose answer is terse but whose reasoning is rich, e.g.
`Find the smallest positive integer n such that n! is divisible by 2^20 but not by 2^21. Reply with ONLY the number.`
then `Referring to the reasoning you just did (do not redo it): which candidate values of n did you evaluate, and what 2-adic valuation did you compute for each?`
With the trace replayed the model lists the candidates it evaluated privately; repeat the pair with
`delete <ai-message>.dataset.reasoningId` before the follow-up and it answers that it evaluated
none. That difference is the end-to-end proof.

---

## 5. Part 3 — sibling files

`image.html` is a fork of `index.html` and shares the reasoning implementation. Verify rather than
re-derive:

```bash
# The shared helpers must be byte-identical between the two files.
python3 - <<'EOF'
names = ["textAfterResponseDivider","responseTextForApi","appendReasoningSummary",
         "sanitizeReasoningItem","trailingReasoningRecord","reasoningItemsToReplay",
         "reasoningRecordIsValid","reasoningDumpIsValid","collectReferencedReasoningIds",
         "buildReasoningDump","saveReasoningTrace","loadReasoningTrace","clearReasoningTraces",
         "saveReasoningDump","withStore","getFromStore"]
def grab(path, name):
    src = open(path).read()
    for prefix in ("      function ", "      async function "):
        i = src.find(prefix + name + "(")
        if i != -1:
            return src[i:src.find("\n      }\n", i)]
    return None
bad = [n for n in names if grab("index.html", n) != grab("image.html", n)]
print("DIFFERS: " + ", ".join(bad) if bad else "all shared helpers identical")
EOF
```

Then run the reasoning-specific offline tests (A1–A3, B–E, H) against `image.html`, remembering:

- every `localStorage` key is prefixed: `image-history`, `image-key`, `image-reasoning-effort`, …
  (`storageKey()`); the IndexedDB name is `image-simple-chatgpt-images`
- it has **no** copy-section buttons, so skip L and N
- errors *replace* the message text instead of appending an `------ Error -------` block
- `gpt-image-*` models go through `generateImage()` (the `/v1/images/generations` endpoint), not
  `send()`, so no reasoning applies there; do check that `buildImagePrompt()` embeds only the text
  after the `---- Response ------` divider, never the reasoning summary
- one live turn plus the helper diff is enough

`chinese.html`, `spanish.html`, `movies.html`, and `image-edit.html` have **no** reasoning support —
confirm with `grep -c "appendReasoningSummary\|reasoningEffort"` returning 0 and leave them alone.

---

## 6. Cleanup (do this before the user revokes the key)

```js
async () => {
  key.value = "";
  (await appDbPromise).close();
  const gone = await new Promise(res => {
    const req = indexedDB.deleteDatabase("simple-chatgpt-images");   // image-… for image.html
    req.onsuccess = () => res("deleted"); req.onerror = () => res("error"); req.onblocked = () => res("blocked");
  });
  localStorage.clear(); sessionStorage.clear();
  return {remaining: Object.keys(localStorage).length, indexedDb: gone};
}
```

Then close the tab (MCP `close_page`), `pkill -f "http.server 8901"`, and tell the user the key can
be revoked.

---

## 7. By-design behaviours — do not report these as bugs

- **An empty reasoning summary is normal.** Short reasoning often returns `summary: []` with no
  `reasoning_summary_text` events even at `summary:"detailed"`. The dividers still appear (A2);
  only the summary body is missing.
- **Only the reasoning items immediately preceding the final `message` are preserved.** A turn
  ending `reasoning → tool_call → message` saves no trace at all. This is intentional: replaying a
  reasoning item whose original follower (a `web_search_call`, say) is absent triggers
  `Item 'rs_…' of type 'reasoning' was provided without its required following item`.
- **`status` must not be sent** on an input reasoning item (`Unknown parameter: 'input[N].status'`),
  and `summary` **is required** even when empty. Item `id`s are optional. Verified by probing the API.
- **Traces are model-scoped.** A record stores the producing model and is skipped when the Model
  field differs, since another model cannot decrypt it. The record is still kept on disk.
- **Orphaned records accumulate.** Re-asking an older question leaves the previous trace (and
  generated images) in IndexedDB until the next Clear or Load; only referenced ids are exported.
- **`localStorage` must stay small.** Traces live in IndexedDB precisely so the ~5 MB quota is never
  at risk; if you ever see `encrypted_content` inside `localStorage["history"]`, that *is* a bug.

## 8. Pitfalls that have already cost time

- A stub `Response` built by hand **ignores `opts.signal`** unless you wire it up, so the abort test
  hangs forever and, worse, leaves a live `activeRequest` that makes every later `send()` return
  immediately via its guard. If tests start silently doing nothing, check `activeRequest` and reset
  it with `activeRequest = null`.
- `stop` shadows the built-in `window.stop`; confirm with
  `String(stop).includes("activeRequest")` before trusting an abort test.
- Top-level `let` bindings (`activeRequest`, `messages`, `model`, `appDbPromise`, …) are script-scoped
  but reachable from `evaluate_script` in the same realm, and assignable — that is what makes
  `activeRequest = null` and `appDbPromise = openAppDb()` possible.
- `restoreHistory([])` detaches the old message elements; a request still in flight then points at
  an orphan, and `stop(messages.children[0])` will not match it.
- Reloading the page discards Blocks A and B — reinstall them after every navigation.
