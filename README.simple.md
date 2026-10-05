# SemCache (The Plain-English Guide)

> **Think of SemCache as an instant memory booster for your AI apps: it stops your AI tools and agents from asking the exact same expensive questions twice, cuts your API bills, and answers repeated questions in less than 2 milliseconds.**

### 🛡️ The 3 Core Guarantees
1. **Zero Infrastructure:** A single self-contained binary. No Redis, no external database, no Docker cluster to manage.
2. **Zero-Trust Privacy:** 100% local on your machine. Everything lives in a private SQLite file (`semcache.db`). Never sends prompts, telemetry, or API keys outside your laptop.
3. **1-Line Integration:** Works with any OpenAI-compatible app, SDK, or framework (Python, Node, Cursor, LangChain, Claude Code) just by changing the `baseURL`.

If you build apps with tools like **LangChain, AutoGPT, Cursor, Claude Code, or write Python scripts calling OpenAI**, you've probably noticed two painful things:
1. **Your API bill gets expensive fast.**
2. **You keep waiting 3 to 10 seconds** for the AI to answer things it already answered 2 minutes ago.

You don't need to be a systems programmer or a Rust expert to use SemCache. This guide explains what it does, why it saves you real money, and how to start using it in under 60 seconds.

---

## 🛑 The Problem: AI Agents Repeat Themselves

When AI agents or automated test suites run, they don't think like humans. They loop:
- **They retry prompts over and over** while fixing small code bugs or planning next steps.
- **Multiple agents ask the same background questions** at the exact same split second.
- **Your automated tests re-run identical prompts** every time you run your CI or test suite.

Every single prompt you send over the internet:
* **Costs you money** (OpenAI and Anthropic charge per token).
* **Makes you wait** for a round-trip across the internet (2,000 to 8,000 milliseconds).
* **Pushes you toward rate limits** (`429 Too Many Requests`).

---

## ⚡ What SemCache Does (In Plain English)

SemCache sits silently on your computer between your app and OpenAI like a smart, lightning-fast memory layer.

```
+--------------------+        HTTP Call        +--------------------+        First Time        +--------------------+
|   Your App or      | ----------------------> |      SemCache      | ----------------------> |    OpenAI API      |
|   AI Agent         |                         |  (On your laptop)  |                         |  (Over internet)   |
+--------------------+                         +--------------------+                         +--------------------+
         ▲                                                │
         │                                                │ Answer saved locally in < 1.5ms!
         │                                                ▼
         │                                     +--------------------+
         +------------------------------------ | Local Fast Storage |
                 Next time you ask the         +--------------------+
                 exact same question:
                 Instant response! (0 tokens, 0 delay)
```

1. **Instant Answers (L1 Cache):** If your app asks a question it already asked before, SemCache replies directly from your computer in **less than 1.5 milliseconds** ($0.0015\text{ seconds}$). That's up to **3,000x faster** than waiting for the internet!
2. **Stops Duplicate Stampedes (Single-Flight Coalescing):** If 5 AI agents ask the exact same question at the exact same moment, SemCache only calls OpenAI **once**. As soon as the answer arrives, it instantly gives it to all 5 agents. You only pay once!
3. **100% Private & Local:** Your cache lives right on your machine in a tiny SQLite file (`semcache.db`). Nothing goes to any third-party cloud.
4. **Smart Whitespace & Formatting Handling:** AI SDKs sometimes add random spaces or rearrange fields in JSON. SemCache automatically cleans up and normalizes the format behind the scenes so identical questions always match.
5. **Doesn't Break Streaming:** If your app uses real-time token streaming (`stream: true` for that typewriter typing effect), SemCache lets it stream right through smoothly.

---

## 🤔 "Why Not Just Use Redis or Memcached?"

A lot of developers ask: *"I already have Redis. Why can't I just cache prompts there?"*

Standard caches like Redis or Memcached are built for simple web pages, not AI prompts. Here is why Redis fails for LLMs:

| What Happens In Real AI Apps | What Redis Does | What SemCache Does |
| :--- | :--- | :--- |
| **Python or LangChain re-orders keys in JSON** | ❌ **Miss!** Redis treats `{a: 1, b: 2}` and `{b: 2, a: 1}` as different prompts. You pay again. | ✅ **Hit!** SemCache parses JSON and sorts keys so matching questions always hit ($<1.5\text{ms}$). |
| **SDK adds extra spaces or a newline** | ❌ **Miss!** Redis compares exact characters. One extra space ruins the cache. | ✅ **Hit!** SemCache normalizes whitespace safely without altering your prompt meaning. |
| **5 parallel agents ask the same prompt at once** | ❌ **Thundering Herd!** All 5 requests slip past Redis before any answer is saved. You pay 5x. | ✅ **Coalesced!** SemCache holds the other 4 requests in memory, calls OpenAI once, and shares the answer. |
| **Streaming (`stream: true`)** | ❌ Redis cannot stream tokens. | ✅ SemCache passes SSE streams through safely with built-in watchdogs. |
| **Setup & Maintenance** | Requires installing Redis server, managing memory, and setting up eviction. | **Zero setup.** Just run `./semcache`. A tiny embedded SQLite database handles everything. |

---

## 🚀 How to Use SemCache in 3 Steps

### Step 1: Start SemCache
Open your terminal and run:

**For OpenAI:**
```bash
# Point upstream to OpenAI (default):
./semcache
```

**For Local LLMs (Ollama):**
```bash
# Point upstream to your local Ollama instance on port 11434:
OPENAI_UPSTREAM_URL="http://localhost:11434/v1/chat/completions" \
SEMCACHE_DEFAULT_PROVIDER="ollama" \
./semcache
```
By default, SemCache starts on `http://127.0.0.1:3000` (or `0.0.0.0:8080` if configured).

### Step 2: Change Exactly ONE Line in Your App
You don't need to rewrite your code or install special packages. You just change your **base URL** to point to SemCache instead of OpenAI directly:

#### In Python:
```python
from openai import OpenAI

# Just point base_url to your local SemCache!
client = OpenAI(
    base_url="http://localhost:3000/v1",
    api_key="your-openai-api-key"
)

response = client.chat.completions.create(
    model="gpt-4o",
    messages=[{"role": "user", "content": "What is the capital of France?"}]
)
print(response.choices[0].message.content)
```

#### In Node.js / TypeScript:
```typescript
import OpenAI from "openai";

const openai = new OpenAI({
  baseURL: "http://localhost:3000/v1",
  apiKey: process.env.OPENAI_API_KEY,
});
```

#### In Terminal / Environment Variables:
Most AI tools (like Cursor, Aider, LangChain, Claude Code) automatically respect the `OPENAI_BASE_URL` environment variable:
```bash
export OPENAI_BASE_URL="http://localhost:3000/v1"
```

### Step 3: Enjoy Free, Instant Responses!
- The first time your app asks a question, SemCache gets the answer from OpenAI and saves it.
- The next time, it returns the answer instantly for **$0 and 0ms internet delay**.

---

## 🔍 How to Tell If It's Working

Every response from SemCache includes a special header called `x-semcache-status`. You can see it in your browser, terminal, or network tab:

| Status Header | What it means |
| :--- | :--- |
| `HIT_L1` | ⚡ **Instant Hit!** Returned directly from your computer in $< 1.5\text{ms}$. Cost: **$0**. |
| `HIT_COALESCED` | 🤝 **Shared Hit!** Another agent asked this at the exact same split-second. Both got the answer, but you only paid once. |
| `MISS_UPSTREAM` | 🌐 **First Time.** SemCache called OpenAI, answered your app, and remembered the answer for next time. |
| `BYPASS_STREAM` | 🌊 **Streaming Pass.** Your app requested live streaming tokens; SemCache passed it straight through safely. |

---

## 💡 Comparison: With vs Without SemCache

| Feature | Without SemCache | With SemCache |
|---|---|---|
| **Repeated Prompt Cost** | You pay full price every time 💸 | **$0.00 (100% saved)** 💰 |
| **Response Latency** | 2,000ms – 6,000ms ⏳ | **0.8ms – 1.5ms** ⚡ |
| **5 Concurrent Identical Prompts** | 5 separate paid API calls | **1 call, 4 free instantaneous replays** |
| **Hitting 429 Rate Limits** | Common during agent loops & CI | **Dramatically reduced** |
| **Setup Hassle** | N/A | **Change 1 line of code (`baseURL`)** |
| **Data Privacy** | Depends on cloud gateway | **100% local on your hard drive** |

---

## 🧪 Zero-Risk Trial: How Teams Use It in Staging & CI/CD

Enterprise engineers and team leads often ask: *"How can we test this without risking our production traffic?"*

Here is the exact low-risk path recommended for teams:

1. **Trial in CI/CD Test Runners First:**
   - Point your integration test suites or synthetic eval benchmarks to SemCache in GitHub Actions / GitLab CI.
   - Run 1 establishes your baseline. Runs 2 through $N$ replay prompt hits in **$<1.5\text{ms}$** and cost **$0.00 in API tokens**.
2. **Trial in Local AI Development:**
   - Developers running Cursor, Claude Code, or agent prototypes set `export OPENAI_BASE_URL="http://localhost:3000/v1"`.
   - Protects your team from hitting provider rate limits (`HTTP 429`) or blowing up team monthly credits.
3. **Graduate to Staging Environments:**
   - Add SemCache as a 1-container sidecar in your staging `docker-compose.yml`.
   - Your internal staging apps immediately benefit from shared agent caching and single-flight deduplication.

---

## 🐝 Real-World Proof: The MiroFish Multi-Agent Swarm Test

SemCache was stress-tested against **MiroFish**—a multi-agent swarm intelligence framework running dozens of simulated AI personas (executives, engineers, market analysts) debating and writing reports over Neo4j knowledge graphs.

- **100+ High-Concurrency Requests:** MiroFish agents fired parallel prompts simultaneously through SemCache to a local Ollama LLM (`qwen2.5-coder:7b` on Apple Metal).
- **Zero Dropped Frames / 0 Crashes:** SemCache handled the burst traffic smoothly without a single connection reset or timeout.
- **$<1.5\text{ms}$ Instant Replays:** Repeated persona queries and survey questions hit L1 cache with zero GPU re-compute.
- **Enterprise Survey Verdict:** When simulated enterprise executive personas were asked if they'd adopt SemCache after learning about its local zero-trust privacy and 1-line integration, the consensus was **"Yes, test in staging / internal apps first"**.

---

## ❓ Frequently Asked Questions (FAQ)

#### Q: Does SemCache steal or save my OpenAI API key?
**No.** SemCache never writes your API keys to disk or sends them anywhere. It only uses a cryptographic fingerprint of your key in memory so that two different users on the same machine never accidentally see each other's answers.

#### Q: Will this fill up my laptop's hard drive?
**No.** SemCache has a built-in 2 GB storage limit. Once it reaches the limit, it automatically cleans out the oldest entries in the background so it never clutters your computer.

#### Q: What if I don't want a specific question to be cached?
Just add the standard HTTP header `"Cache-Control: no-store"` to your request, and SemCache will skip saving it.

#### Q: Does it work with other LLMs like Ollama or Azure?
**Yes!** SemCache supports any OpenAI-compatible API, including local models running on Ollama, vLLM, or Azure OpenAI.

#### Q: Does SemCache work with multi-agent frameworks (LangGraph, CrewAI, AutoGPT)?
**Yes.** In fact, multi-agent swarms benefit the most because agents frequently repeat system instructions, persona definitions, and common evaluation steps. SemCache automatically coalesces and caches these duplicate calls.

