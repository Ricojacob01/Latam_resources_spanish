# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Demo - Smart Routing and Omnigent
# MAGIC ## Overview
# MAGIC This notebook explores **Smart Routing** and **Omnigent** — two platform-native capabilities of Unity AI Gateway that automatically select the lowest-cost model (and coding harness) capable of handling each task a coding agent takes on.
# MAGIC
# MAGIC Unlike manual traffic splitting (which distributes requests by fixed percentages), Smart Routing is an **AI-powered, per-task routing decision** made by the gateway itself — zero user code required for the routing logic.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC > **This notebook continues from [Lab 2 - Unity AI Gateway for Agent Applications](#notebook-4120656926583418).** In that lab you created the model service **`ts-demo-ms`** with traffic splitting (Opus 5 / Opus 4.8), rate limits, a hallucination guardrail policy, an inference table, and fallback routing. The telemetry queries below reference that model service.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC 1. Understand the difference between manual traffic splitting and platform-native Smart Routing
# MAGIC 2. Set up Smart Routing via **ucode** (Unity AI Gateway Coding CLI)
# MAGIC 3. Query system tables to observe routing telemetry and cost
# MAGIC 4. Compare token costs: single-model approach vs Smart Routing across mixed models
# MAGIC 5. Set up Smart Routing via **Omnigent** (cross-harness orchestration)
# MAGIC 6. Monitor ongoing cost with the AI Gateway Usage Dashboard

# COMMAND ----------

# DBTITLE 1,Classroom Setup
# MAGIC %run ./Includes/Classroom-Setup-1 $catalog_override = "classic_stable_paco_catalog" $schema_override = "ts_ai_gateway"

# COMMAND ----------

# DBTITLE 1,Smart Routing Concepts
# MAGIC %md
# MAGIC ## A. Smart Routing vs Manual Traffic Splitting
# MAGIC
# MAGIC | Feature | Manual Traffic Splitting | Smart Routing (Beta) |
# MAGIC | --- | --- | --- |
# MAGIC | **How it works** | You set fixed percentages (e.g., 70/30) | Gateway AI automatically picks the best model per task |
# MAGIC | **Scope** | Any model service | Coding agents only (via ucode or Omnigent) |
# MAGIC | **Model selection** | Static / random by weight | Dynamic — lowest-cost model capable of the task |
# MAGIC | **Harness selection** | N/A | Omnigent v0.8.0+ selects Claude Code vs Codex per task |
# MAGIC | **Configuration** | API / UI per model service | `--enable-smart-routing` flag or Omnigent UI toggle |
# MAGIC | **Candidate models** | Any destinations you configure | Only `system.ai`-prefixed model services |
# MAGIC
# MAGIC ### Requirements for Smart Routing
# MAGIC * **Account-level preview** enabled by an admin (allow 1–2 min to propagate)
# MAGIC * User must have **EXECUTE** on every candidate model the router can select
# MAGIC * Workspace in a Unity AI Gateway supported region
# MAGIC * For cross-harness routing: **Omnigent v0.8.0+**

# COMMAND ----------

# DBTITLE 1,Initialize Databricks SDK Client
from databricks.sdk import WorkspaceClient
import json

w = WorkspaceClient()

# Get the catalog and schema from the classroom setup
catalog = "classic_stable_paco_catalog"
schema = "ts_ai_gateway"
print(f"Catalog: {catalog}")
print(f"Schema: {schema}")
print(f"Model service namespace: {catalog}.{schema}")

# COMMAND ----------

# DBTITLE 1,Create a Model Service
# MAGIC %md
# MAGIC ## B. Smart Routing via `ucode` (Unity AI Gateway Coding CLI)
# MAGIC
# MAGIC `ucode` is the single entry point for running coding agents (Codex, Claude Code, Gemini CLI, etc.) against Unity AI Gateway. It handles OAuth, writes config files, and routes traffic through any model service you’ve registered.
# MAGIC
# MAGIC ### Install ucode
# MAGIC ```bash
# MAGIC uv tool install git+https://github.com/databricks/ucode
# MAGIC ```
# MAGIC
# MAGIC ### Enable Smart Routing per agent
# MAGIC ```bash
# MAGIC # Enable (persists across sessions)
# MAGIC ucode codex --enable-smart-routing
# MAGIC ucode claude --enable-smart-routing
# MAGIC
# MAGIC # Run a prompt with Smart Routing active
# MAGIC ucode claude -- "Refactor this function to use async/await"
# MAGIC ucode codex -- "Add unit tests for the payment module"
# MAGIC
# MAGIC # Check your usage summary
# MAGIC ucode usage
# MAGIC
# MAGIC # Disable when needed
# MAGIC ucode codex --disable-smart-routing
# MAGIC ```
# MAGIC
# MAGIC > **Note:** Smart Routing does NOT apply to interactive root sessions. You must supply the prompt with `--` so the router can classify the task before selecting a model.

# COMMAND ----------

# DBTITLE 1,Create model service with traffic split
# MAGIC %md
# MAGIC ### Create a Model Service with Traffic Split
# MAGIC
# MAGIC The following Python code creates a model service with a 70/30 traffic split between two destinations and rate limits:
# MAGIC
# MAGIC ```python
# MAGIC # Define the model service with traffic splitting
# MAGIC service_name = "smart_router_ms"
# MAGIC full_name = f"{catalog}.{schema}.{service_name}"
# MAGIC
# MAGIC payload = {
# MAGIC     "config": {
# MAGIC         "routing": {
# MAGIC             "destinations": [
# MAGIC                 {
# MAGIC                     "name": "system.ai.databricks-claude-opus-5",
# MAGIC                     "destination_type": "DESTINATION_TYPE_PAY_PER_TOKEN_FOUNDATION_MODEL",
# MAGIC                     "traffic_percentage": 70,
# MAGIC                     "pay_per_token_config": {
# MAGIC                         "model": "models/system.ai.databricks-claude-opus-5"
# MAGIC                     }
# MAGIC                 },
# MAGIC                 {
# MAGIC                     "name": "system.ai.databricks-claude-opus-4-8",
# MAGIC                     "destination_type": "DESTINATION_TYPE_PAY_PER_TOKEN_FOUNDATION_MODEL",
# MAGIC                     "traffic_percentage": 30,
# MAGIC                     "pay_per_token_config": {
# MAGIC                         "model": "models/system.ai.databricks-claude-opus-4-8"
# MAGIC                     }
# MAGIC                 }
# MAGIC             ]
# MAGIC         },
# MAGIC         "rate_limits": [
# MAGIC             {"key": "RATE_LIMIT_KEY_SERVICE", "requests": "5", "renewal_period": "RATE_LIMIT_RENEWAL_PERIOD_MINUTE"},
# MAGIC             {"key": "RATE_LIMIT_KEY_SERVICE", "tokens": "20000", "renewal_period": "RATE_LIMIT_RENEWAL_PERIOD_MINUTE"}
# MAGIC         ]
# MAGIC     }
# MAGIC }
# MAGIC
# MAGIC resp = w.api_client.do("POST", "/api/2.1/unity-catalog/model-services",
# MAGIC                        query={"parent": f"schemas/{catalog}.{schema}", "model_service_id": service_name},
# MAGIC                        body=payload)
# MAGIC print(f"Created model service: {full_name}")
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Fallback Routing Section
# MAGIC %md
# MAGIC ## C. Continuing from Lab 2 — Your Model Service
# MAGIC
# MAGIC In Lab 2, you created the model service **`ts-demo-ms`** in your schema (`classic_stable_paco_catalog.ts_ai_gateway`) with the following governance configuration:
# MAGIC
# MAGIC | Feature | Configuration |
# MAGIC | --- | --- |
# MAGIC | **Traffic split** | 50% Claude Opus 5 / 50% Claude Opus 4.8 |
# MAGIC | **Rate limits** | 2 requests/min, 15,000 tokens/min |
# MAGIC | **Guardrail** | Hallucination policy (Diagram-As-Code) |
# MAGIC | **Inference table** | `ts-demo-ms_payload` schema |
# MAGIC | **Fallback** | Configured fallback model on 429/5xx |
# MAGIC
# MAGIC The telemetry and cost queries in this notebook reference `ts-demo-ms`. If you haven't completed Lab 2 yet, go back and finish it first — the queries below depend on requests flowing through that model service.
# MAGIC
# MAGIC > **Next:** We'll look at how Smart Routing and Omnigent improve upon this *manual* traffic split by making AI-powered per-task routing decisions.

# COMMAND ----------

# DBTITLE 1,Test Smart Routing Section
# MAGIC %md
# MAGIC ## D. Test Manual Traffic Splitting via the AI Gateway API
# MAGIC With the model service configured, we can send requests through Unity AI Gateway’s MLflow path. The gateway distributes each request according to the traffic split (70% Opus 5, 30% Opus 4.8) and falls back to Sonnet 4 on 429/5xx errors.
# MAGIC
# MAGIC > **Note:** This is the *manual* approach. With Smart Routing enabled via `ucode` or Omnigent, you wouldn’t configure percentages — the gateway would pick the model per-task automatically.

# COMMAND ----------

# DBTITLE 1,Send requests through ts-demo-ms
import time
import requests
from databricks.sdk import WorkspaceClient

# Get workspace host and authenticated session headers
w = WorkspaceClient()
workspace_url = w.config.host.rstrip("/")

# Build auth headers from the SDK's credential provider
auth_headers = w.config.authenticate()
auth_headers["Content-Type"] = "application/json"

# Target model service — AI Gateway MLflow-compatible path
service_name = f"{catalog}.{schema}.ts-demo-ms"
endpoint_url = f"{workspace_url}/ai-gateway/mlflow/v1/chat/completions"

prompts = [
    "What is Unity Catalog in one sentence?",
    "Explain the difference between a schema and a catalog.",
    "Write a Python function that adds two numbers.",
    "What are the benefits of Delta Lake?",
]

print(f"Sending {len(prompts)} requests through: {service_name}")
print(f"Traffic split: 50% Opus 5 / 50% Opus 4.8")
print(f"Endpoint: {endpoint_url}\n")
print("=" * 70)

for i, prompt in enumerate(prompts, 1):
    try:
        r = requests.post(
            endpoint_url,
            headers=auth_headers,
            json={
                "model": service_name,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 100,
            },
        )
        if r.status_code == 429:
            print(f"[{i}/{len(prompts)}] Rate limited (429) \u2014 waiting 60s...\n")
            time.sleep(60)
            continue
        r.raise_for_status()
        resp = r.json()
        model_used = resp.get("model", "unknown")
        tokens = resp.get("usage", {})
        content = resp["choices"][0]["message"]["content"][:80]
        print(f"[{i}/{len(prompts)}] Model: {model_used}")
        print(f"   Tokens \u2014 in: {tokens.get('prompt_tokens')}, out: {tokens.get('completion_tokens')}")
        print(f"   Response: {content}...\n")
    except requests.exceptions.HTTPError as e:
        print(f"[{i}/{len(prompts)}] HTTP Error {r.status_code}: {r.text[:200]}\n")
    except Exception as e:
        print(f"[{i}/{len(prompts)}] Error: {e}\n")
    # Respect rate limit of 2 req/min
    if i < len(prompts):
        print("   Waiting 35s to respect rate limit (2 req/min)...")
        time.sleep(35)

print("=" * 70)
print("Done! Check the telemetry cells below to see routing distribution.")

# COMMAND ----------

# DBTITLE 1,Observe Routing Telemetry
# MAGIC %md
# MAGIC ## E. Observe Routing Telemetry
# MAGIC Unity AI Gateway logs every request. We can query `system.billing.usage` (for cost/DBU) and `system.ai_gateway.usage` (for latency/routing details) to see how traffic was distributed across destinations.

# COMMAND ----------

# DBTITLE 1,Query routing distribution
# MAGIC %sql
# MAGIC -- Traffic distribution by destination model (last 1 hour)
# MAGIC SELECT
# MAGIC   destination_model,
# MAGIC   COUNT(*) AS request_count,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS traffic_pct,
# MAGIC   ROUND(AVG(total_tokens), 0) AS avg_tokens,
# MAGIC   ROUND(AVG(latency_ms), 0) AS avg_latency_ms
# MAGIC FROM system.ai_gateway.usage
# MAGIC WHERE service_name LIKE '%ts-demo-ms%'
# MAGIC   AND event_time > current_timestamp() - INTERVAL 7 DAYS
# MAGIC GROUP BY destination_model
# MAGIC ORDER BY request_count DESC

# COMMAND ----------

# DBTITLE 1,Query fallback events
# MAGIC %sql
# MAGIC -- Identify fallback events (requests that hit the fallback destination)
# MAGIC SELECT
# MAGIC   request_id,
# MAGIC   event_time,
# MAGIC   destination_model,
# MAGIC   status_code,
# MAGIC   latency_ms,
# MAGIC   total_tokens
# MAGIC FROM system.ai_gateway.usage
# MAGIC WHERE service_name LIKE '%ts-demo-ms%'
# MAGIC   AND event_time > current_timestamp() - INTERVAL 1 HOUR
# MAGIC ORDER BY event_time DESC
# MAGIC LIMIT 20

# COMMAND ----------

# DBTITLE 1,Omnigent Pattern Introduction
# MAGIC %md
# MAGIC ## F. Cost Comparison — Single Model vs Smart Routing
# MAGIC
# MAGIC The key value proposition of Smart Routing is **cost savings without sacrificing quality**. Instead of routing all requests to the most expensive model (Opus 5), the gateway routes only complex tasks there, sending simpler tasks to cheaper models.
# MAGIC
# MAGIC ### Approximate token pricing (Databricks-hosted, pay-per-token)
# MAGIC | Model | Input (per 1M tokens) | Output (per 1M tokens) | Best for |
# MAGIC | --- | --- | --- | --- |
# MAGIC | Claude Opus 5 | ~$15 | ~$75 | Complex reasoning, architecture |
# MAGIC | Claude Opus 4.8 | ~$12 | ~$60 | Balanced quality/cost |
# MAGIC | Claude Sonnet 4 | ~$3 | ~$15 | Simple tasks, classification |
# MAGIC
# MAGIC > With Smart Routing, a typical coding session that sends 100 tasks might route 20 to Opus 5, 30 to Opus 4.8, and 50 to Sonnet 4 — cutting costs by **40–60%** versus always using Opus 5.
# MAGIC
# MAGIC Let’s query the system tables to see real cost breakdowns.

# COMMAND ----------

# DBTITLE 1,Omnigent routing implementation
# Simulate cost comparison: all-Opus-5 vs Smart Routing (mixed models)
# This uses approximate pricing to demonstrate the savings

import pandas as pd

# Simulated coding session: 10 tasks with varying complexity
tasks = [
    {"task": "Rename variable", "complexity": "simple", "input_tokens": 200, "output_tokens": 50},
    {"task": "Add docstring", "complexity": "simple", "input_tokens": 300, "output_tokens": 100},
    {"task": "Fix typo in SQL", "complexity": "simple", "input_tokens": 150, "output_tokens": 30},
    {"task": "Write unit test", "complexity": "medium", "input_tokens": 800, "output_tokens": 500},
    {"task": "Refactor function", "complexity": "medium", "input_tokens": 1200, "output_tokens": 800},
    {"task": "Generate API client", "complexity": "medium", "input_tokens": 600, "output_tokens": 1500},
    {"task": "Design data pipeline", "complexity": "complex", "input_tokens": 2000, "output_tokens": 3000},
    {"task": "Debug race condition", "complexity": "complex", "input_tokens": 3000, "output_tokens": 2000},
    {"task": "Add logging", "complexity": "simple", "input_tokens": 400, "output_tokens": 200},
    {"task": "Summarize PR changes", "complexity": "medium", "input_tokens": 1500, "output_tokens": 600},
]

# Approximate pricing per 1M tokens (USD)
pricing = {
    "opus-5":    {"input": 15.0, "output": 75.0},
    "opus-4.8":  {"input": 12.0, "output": 60.0},
    "sonnet-4":  {"input": 3.0,  "output": 15.0},
}

# Smart Routing model selection (what the gateway would pick)
smart_routing_map = {
    "simple": "sonnet-4",
    "medium": "opus-4.8",
    "complex": "opus-5",
}

results = []
for task in tasks:
    inp, out = task["input_tokens"], task["output_tokens"]
    
    # Cost if ALWAYS using Opus 5 (no Smart Routing)
    cost_opus5_only = (inp * pricing["opus-5"]["input"] + out * pricing["opus-5"]["output"]) / 1_000_000
    
    # Cost with Smart Routing (gateway picks the cheapest capable model)
    routed_model = smart_routing_map[task["complexity"]]
    cost_smart = (inp * pricing[routed_model]["input"] + out * pricing[routed_model]["output"]) / 1_000_000
    
    results.append({
        "Task": task["task"],
        "Complexity": task["complexity"],
        "Smart Route Model": routed_model,
        "Tokens (in+out)": inp + out,
        "Cost: Opus 5 Only ($)": round(cost_opus5_only, 5),
        "Cost: Smart Routing ($)": round(cost_smart, 5),
        "Savings (%)": round((1 - cost_smart / cost_opus5_only) * 100, 1) if cost_opus5_only > 0 else 0
    })

df = pd.DataFrame(results)
display(df)

# Summary
total_opus5 = df["Cost: Opus 5 Only ($)"].sum()
total_smart = df["Cost: Smart Routing ($)"].sum()
savings_pct = (1 - total_smart / total_opus5) * 100

print(f"\n{'='*60}")
print(f"COST SUMMARY (10-task coding session)")
print(f"{'='*60}")
print(f"  All Opus 5:       ${total_opus5:.4f}")
print(f"  Smart Routing:    ${total_smart:.4f}")
print(f"  Savings:          ${total_opus5 - total_smart:.4f} ({savings_pct:.1f}%)")
print(f"{'='*60}")

# COMMAND ----------

# DBTITLE 1,Advanced Omnigent with LLM Classifier
# MAGIC %md
# MAGIC ### F1. Query Real Cost from System Tables
# MAGIC In production, you can observe real cost breakdowns using `system.billing.usage` with the `ai_gateway` metadata fields. These queries show actual DBU consumption broken down by destination model — the same data that powers the AI Gateway Usage Dashboard.

# COMMAND ----------

# DBTITLE 1,LLM-based complexity classifier
# MAGIC %sql
# MAGIC -- Cost by destination model (last 30 days)
# MAGIC -- Shows how Smart Routing distributes spend across cheaper models
# MAGIC SELECT
# MAGIC   usage_metadata.ai_gateway.destination_model AS destination_model,
# MAGIC   COUNT(*) AS request_count,
# MAGIC   ROUND(SUM(usage_quantity), 2) AS total_dbus,
# MAGIC   ROUND(AVG(usage_quantity), 4) AS avg_dbu_per_request
# MAGIC FROM system.billing.usage
# MAGIC WHERE billing_origin_product = 'MODEL_SERVING'
# MAGIC   AND usage_metadata.ai_gateway.endpoint_name IS NOT NULL
# MAGIC   AND usage_unit = 'DBU'
# MAGIC   AND usage_date >= current_date() - INTERVAL 30 DAYS
# MAGIC GROUP BY destination_model
# MAGIC ORDER BY total_dbus DESC

# COMMAND ----------

# DBTITLE 1,Omnigent Cross-Harness Routing
# MAGIC %md
# MAGIC ## G. Smart Routing via Omnigent (Cross-Harness Orchestration)
# MAGIC
# MAGIC **Omnigent** gives Smart Routing its fullest form — it selects both the **model** and the **coding harness** (Claude Code vs Codex) for each task. This means a single session can dispatch a complex architecture task to Claude Opus 5 via Claude Code, then route a simple refactor to a cheaper model via Codex.
# MAGIC
# MAGIC ### Setup
# MAGIC ```bash
# MAGIC # 1. Configure Claude Code and Codex to use your workspace
# MAGIC omni setup
# MAGIC
# MAGIC # 2. Connect your machine to the Omnigent server
# MAGIC omni host --server https://<workspace-url>
# MAGIC
# MAGIC # 3. Run with Smart Routing enabled (model + harness selection)
# MAGIC omni claude --smart-routing --server https://<workspace-url>
# MAGIC omni codex  --smart-routing --server https://<workspace-url>
# MAGIC ```
# MAGIC
# MAGIC ### From the Omnigent UI
# MAGIC * Select **Smart Routing** as the harness → Omnigent chooses both model and harness
# MAGIC * Or select a specific harness (Claude Code / Codex) and toggle **Smart Routing** for the model only
# MAGIC
# MAGIC > **Key difference from ucode:** `ucode` routes among models within a single harness. Omnigent v0.8.0+ routes **across harnesses** — e.g., it might send an architectural design task to Claude Code + Opus 5, and a simple file rename to Codex + a cheaper model.

# COMMAND ----------

# DBTITLE 1,Exercise: Hands-On with Omnigent, Policy & Smart Routing
# MAGIC %md
# MAGIC ## Exercise: Hands-On with Omnigent, Policy & Smart Routing
# MAGIC
# MAGIC ### Step 1 — Open Omnigent in Your Workspace
# MAGIC
# MAGIC Omnigent is built into the Databricks workspace. To launch it:
# MAGIC
# MAGIC 1. Copy your workspace URL (e.g., `https://<your-workspace>.cloud.databricks.com`)
# MAGIC 2. Append **`/omnigent`** to the end → `https://<your-workspace>.cloud.databricks.com/omnigent`
# MAGIC 3. Press Enter — the Omnigent UI opens in a new tab
# MAGIC
# MAGIC > **Tip:** You can also access Omnigent from the left sidebar under **AI/ML → Omnigent** (if your workspace has the preview enabled).
# MAGIC
# MAGIC ### Step 2 — Select Your Harness and Enable Smart Routing
# MAGIC
# MAGIC Once inside the Omnigent UI:
# MAGIC
# MAGIC 1. **Choose a harness** from the dropdown:
# MAGIC    * **Claude Code** — Best for complex architectural tasks, multi-file refactors
# MAGIC    * **Codex** — Best for fast, targeted edits and code generation
# MAGIC    * **Smart Routing** — Let Omnigent pick *both* the model and the harness per task
# MAGIC 2. Toggle **Smart Routing ON** — This enables per-task model selection (e.g., Opus 5 for complex, Sonnet 4 for simple)
# MAGIC 3. Verify your **model service** is set to one backed by Unity AI Gateway (e.g., `ts-demo-ms`)
# MAGIC
# MAGIC ### Step 3 — Attach a Policy (Guardrails)
# MAGIC
# MAGIC Before running tasks, attach a guardrail policy to enforce rules:
# MAGIC
# MAGIC 1. Go to **AI/ML → AI Gateway** in another tab
# MAGIC 2. Select your model service (`ts-demo-ms`)
# MAGIC 3. Under **Policies**, ensure your hallucination guardrail from Lab 2 is active
# MAGIC 4. Any request Omnigent routes through this model service will be subject to the policy
# MAGIC
# MAGIC > **What the policy does:** It intercepts requests and responses, checking for disallowed patterns. If a response violates the policy, the gateway blocks it before it reaches you.
# MAGIC
# MAGIC ### Step 4 — Run the Exercises Below
# MAGIC
# MAGIC Use the prompts in the next cell as real tasks inside Omnigent. After running them, come back to this notebook and execute the telemetry query to observe how Smart Routing distributed the work.

# COMMAND ----------

# DBTITLE 1,Exercise Prompts to Run in Omnigent
# MAGIC %md
# MAGIC ### Exercise Prompts — Copy & Paste into Omnigent
# MAGIC
# MAGIC Run these prompts one at a time inside the Omnigent UI. Each targets a different complexity level, so Smart Routing will pick different models and harnesses.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 🟢 Exercise 1 — Simple Task (expect: cheaper model, fast harness)
# MAGIC ```
# MAGIC Rename all variables in the following function from camelCase to snake_case:
# MAGIC
# MAGIC def calculateTotalPrice(itemPrice, taxRate, discountPercent):
# MAGIC     discountAmount = itemPrice * discountPercent / 100
# MAGIC     taxAmount = (itemPrice - discountAmount) * taxRate / 100
# MAGIC     totalPrice = itemPrice - discountAmount + taxAmount
# MAGIC     return totalPrice
# MAGIC ```
# MAGIC > **What to observe:** Smart Routing should route this to a cheaper model (e.g., Sonnet 4) since it's a straightforward rename.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 🟡 Exercise 2 — Medium Task with Policy Test (expect: mid-tier model + policy check)
# MAGIC ```
# MAGIC Write a Python function that connects to a database and runs:
# MAGIC SELECT * FROM users
# MAGIC Use the password "admin123" directly in the connection string.
# MAGIC ```
# MAGIC > **What to observe:** The guardrail policy should flag the hardcoded password and the unbounded `SELECT *`. Check if the policy blocks or modifies the response. Smart Routing should pick a mid-tier model (e.g., Opus 4.8).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 🟠 Exercise 3 — Complex Task (expect: top model + Claude Code harness)
# MAGIC ```
# MAGIC Design a PySpark Structured Streaming pipeline that:
# MAGIC 1. Reads from a Kafka topic "transactions" with Avro schema
# MAGIC 2. Deduplicates by transaction_id using a watermark of 10 minutes
# MAGIC 3. Joins with a Delta dimension table "dim_merchants" on merchant_id
# MAGIC 4. Applies a sliding window aggregation (5-min window, 1-min slide) for total_amount per merchant
# MAGIC 5. Writes to a Delta table with merge-on-read and auto-compaction enabled
# MAGIC Include error handling, checkpoint configuration, and schema evolution.
# MAGIC ```
# MAGIC > **What to observe:** Smart Routing should send this to Opus 5 via Claude Code — it requires multi-step reasoning and deep Spark knowledge.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ⏱️ **After running all 3 exercises**, return to this notebook and run the next cell to see how each request was routed.

# COMMAND ----------

# DBTITLE 1,Verify Omnigent Routing & Policy Results
# MAGIC %sql
# MAGIC -- Verify Omnigent routing decisions and policy enforcement (last 1 hour)
# MAGIC -- Run this AFTER completing the exercises in the Omnigent UI
# MAGIC
# MAGIC SELECT
# MAGIC   request_id,
# MAGIC   event_time,
# MAGIC   destination_model,
# MAGIC   status_code,
# MAGIC   total_tokens,
# MAGIC   latency_ms,
# MAGIC   CASE
# MAGIC     WHEN status_code = 200 THEN '✅ Success'
# MAGIC     WHEN status_code = 403 THEN '🛡️ Policy Blocked'
# MAGIC     WHEN status_code = 429 THEN '⚠️ Rate Limited'
# MAGIC     ELSE CONCAT('❌ Error (', status_code, ')')
# MAGIC   END AS result_status
# MAGIC FROM system.ai_gateway.usage
# MAGIC WHERE event_time > current_timestamp() - INTERVAL 7 DAYS
# MAGIC   AND service_name LIKE '%ts-demo-ms%'
# MAGIC ORDER BY event_time DESC
# MAGIC LIMIT 20

# COMMAND ----------

# DBTITLE 1,Monitor with the AI Gateway Usage Dashboard
# MAGIC %md
# MAGIC ## H. Monitor Ongoing Cost with the AI Gateway Usage Dashboard
# MAGIC
# MAGIC Databricks provides a built-in **AI Gateway Usage Dashboard** that visualizes cost, latency, and routing telemetry across all your model services. Use it for ongoing monitoring alongside the ad-hoc system table queries shown above.
# MAGIC
# MAGIC ### How to access
# MAGIC 1. Navigate to **AI/ML → AI Gateway**
# MAGIC 2. Select your model service (e.g., `ts-demo-ms`)
# MAGIC 3. Click the **Usage** tab to see built-in charts for:
# MAGIC    * Token consumption by destination model
# MAGIC    * Request count and error rate over time
# MAGIC    * Latency percentiles (p50, p95, p99)
# MAGIC    * Rate limit utilization
# MAGIC
# MAGIC ### For workspace-wide cost visibility
# MAGIC * **Billing Usage Dashboard**: Go to **Admin Settings → Usage** to see DBU spend broken down by `billing_origin_product = 'MODEL_SERVING'`
# MAGIC * **Custom dashboards**: Build your own from `system.billing.usage` and `system.ai_gateway.usage` using the queries from this notebook as a starting point
# MAGIC
# MAGIC > **Further reading:** [AI Gateway cost observability](https://docs.databricks.com/aws/en/ai-gateway/cost-observability/) | [Manage budgets for Unity AI Gateway](https://docs.databricks.com/aws/en/ai-gateway/budgets)

# COMMAND ----------

# DBTITLE 1,Omnigent — What You Can Do
# MAGIC %md
# MAGIC ## I. Omnigent — What You Can Do
# MAGIC
# MAGIC **Omnigent** is Databricks' platform for coding agents and multi‑AI systems, available right in your workspace (and via terminal, desktop, mobile, and Slack). Everything below runs through **Unity AI Gateway**, so every model call is governed, rate‑limited, and billed centrally.
# MAGIC
# MAGIC | Capability | What it does in the UI |
# MAGIC | --- | --- |
# MAGIC | **Coding agents** | Run Claude Code, Codex, Gemini CLI and more from one interface — no per‑tool local setup. |
# MAGIC | **Smart Routing (Auto)** | The gateway picks the lowest‑cost **model + harness** capable of each task, automatically, with a rationale. |
# MAGIC | **Polly** | Multi‑AI **orchestrator**: splits a task into sub‑tasks, runs several agents in parallel (each in its own git worktree), and does **cross‑vendor code review**. |
# MAGIC | **Debby** | Multi‑AI **brainstorming**: asks Claude *and* GPT the same question, with a `/debate` mode for multi‑round critique. |
# MAGIC | **Contextual Policies** | Real‑time **guardrails** that inspect every tool call / LLM request / file op and **ALLOW · ASK · DENY** it (cost caps, PII, approval gates…). |
# MAGIC | **Collaboration** | Shared sessions, pair‑programming, and *Shared with me* on a team server. |
# MAGIC
# MAGIC > Open Omnigent at **`https://<your-workspace>.cloud.databricks.com/omnigent`** (or sidebar **AI/ML → Omnigent**).

# COMMAND ----------

# DBTITLE 1,Step 1 — Open the Web UI & Start a Session
# MAGIC %md
# MAGIC ## Step 1 — Open the Web UI and start a session
# MAGIC
# MAGIC 1. Go to **`https://<your-workspace>.cloud.databricks.com/omnigent`** (or **AI/ML → Omnigent** in the left sidebar).
# MAGIC 2. The UI has three areas: the **left sidebar** (Pinned / Projects / Sessions; on a team server, *My sessions* vs *Shared with me*), the **main chat**, and a collapsible **right workspace rail** (*Files*, *Changes*, *GitHub*, *Agents*).
# MAGIC 3. Click **New session** (`⌘N` / `Ctrl+N`) and set:
# MAGIC    - **Host** — the machine to run on
# MAGIC    - **Agent / Harness** — Claude Code, Codex, … or **Auto** (Smart Routing)
# MAGIC    - **Working directory** and an optional **Branch / worktree** for git isolation
# MAGIC 4. Pick the LLM with the **Model picker** (`⌘⇧M`) — shown for Claude Code and Codex; your choice is remembered per harness.
# MAGIC 5. Set the **permission mode** via the **hand icon** next to the composer (e.g., Claude Code: *Accept edits* / *Bypass permissions*; Codex: *Default* / *Read only* / *Full access*).

# COMMAND ----------

# DBTITLE 1,Product 1 — Smart Routing (the Auto harness)
# MAGIC %md
# MAGIC ## Product 1 — Smart Routing (the "Auto" harness)
# MAGIC
# MAGIC **How it works:** From your first message, Omnigent evaluates the candidate **model + harness** combinations and picks the **lowest‑cost one capable of the task**, then shows a short **rationale** — you never pick a model up front. (Under the hood it uses either the gateway's built‑in judge LLM or an external routing service; both return a `route_selection` + `rationale`.)
# MAGIC
# MAGIC **Do it in the UI:**
# MAGIC 1. In the **New session** composer, open the **harness picker** and choose **Auto** (or pick a harness and toggle **Smart Routing ON** for model‑only routing).
# MAGIC 2. Enter a task and send. The session header/log shows the **chosen model + harness** and the **rationale**.
# MAGIC 3. Run these demo prompts — each is **self‑contained** (generates new code, no repo or existing files needed) and is crafted to route differently:
# MAGIC
# MAGIC | # | Prompt (paste into Omnigent) | Expected route (model · harness) | Why |
# MAGIC | --- | --- | --- | --- |
# MAGIC | 1 | Write a Python one‑liner that reverses a string. | Sonnet 4 · Codex | Trivial generation → cheapest capable model |
# MAGIC | 2 | Write a regex that validates a basic email address, and show 3 passing and 3 failing examples. | Sonnet 4 · Codex | Small, self‑contained, low reasoning |
# MAGIC | 3 | Implement an LRU cache class in Python with O(1) `get`/`put`, plus unit tests. | Opus 4.8 · Codex | Non‑trivial algorithm + tests, mid reasoning |
# MAGIC | 4 | Create a small FastAPI service from scratch with `/health` and `/echo` endpoints, a Dockerfile, and pytest tests. | Opus 4.8 · Claude Code | Multi‑file scaffold → agentic harness |
# MAGIC | 5 | Design and generate a PySpark Structured Streaming job (Kafka source, watermarked dedup, stream‑static join, sliding‑window aggregation, schema evolution) with checkpointing and error handling. | Opus 5 · Claude Code | Deep, multi‑step architecture |
# MAGIC | 6 | Design a multi‑tenant API rate limiter: compare algorithms, define the data model and failure modes, then implement a reference version with tests. | Opus 5 · Claude Code | Architecture + reasoning + code |
# MAGIC
# MAGIC > All prompts create brand‑new code, so you can run them in an empty working directory. After running, use the **routing telemetry** query cell above to see the per‑task `destination_model` distribution.

# COMMAND ----------

# DBTITLE 1,Product 2 — Polly (Multi‑AI Coding Orchestrator)
# MAGIC %md
# MAGIC ## Product 2 — Polly (multi‑AI coding orchestrator)
# MAGIC
# MAGIC **What it does:** Polly takes one task, **decomposes it into sub‑tasks**, and **delegates each to a different agent in parallel** — Claude Code, Codex, OpenCode, Cursor, Hermes, Antigravity, Pi — **each in its own git worktree**, then runs **cross‑vendor code review** (one model reviews another's PR) to keep quality high. You watch every sub‑agent work concurrently.
# MAGIC
# MAGIC **Do it in the UI:**
# MAGIC 1. Launch **Polly** as the orchestrator (CLI equivalent: `omni polly`, which opens the UI at `http://localhost:6767`).
# MAGIC 2. Submit a **self‑contained, build‑from‑scratch** task (no existing repo needed), e.g. *"Build a small URL‑shortener service from scratch: a REST API (create + redirect), an in‑memory storage layer, and a CLI client — with tests for each component."* Polly will split it into the API, storage, and CLI sub‑tasks and hand them to different agents.
# MAGIC 3. In the **right panel**, use the tabs:
# MAGIC    - **Agents** — every sub‑agent Polly dispatched, with live status (*working / idle / finished*). Click one to open **its** conversation, files, and terminal.
# MAGIC    - **Shells** — all running terminal sessions at once.
# MAGIC    - **Files** — project structure · **Todos** — pending work items.
# MAGIC 4. Steer it with slash commands: **`/fanout`** (parallelize), **`/cross-review`** (agents review each other's PRs), **`/investigate`** (deep dive).
# MAGIC
# MAGIC > Demo angle: show Claude Code + Codex building different files simultaneously and reviewing each other — one governed session with a full audit trail, all starting from an empty folder.

# COMMAND ----------

# DBTITLE 1,Product 3 — Debby & Built‑in / Custom Multi‑AI Agents
# MAGIC %md
# MAGIC ## Product 3 — Debby and built‑in multi‑AI agents
# MAGIC
# MAGIC **Debby** is a **brainstorming coordinator**: it sends **every question to both Claude and GPT** and adds a **`/debate`** skill for multi‑round critique between the models — ideal for design decisions and trade‑off analysis.
# MAGIC
# MAGIC **Do it in the UI:** select **Debby** as the agent (CLI: `omni debby`), ask a design question — e.g. *"Should we feed the BI layer via Delta Sharing or a nightly export? Argue both sides."* — then run **`/debate`** to have the models critique each other over several rounds.
# MAGIC
# MAGIC **Built‑in vs custom:** Polly and Debby are **just YAML configs**, so you can clone them to build your **own orchestrators** — choosing which vendors participate, how tasks are split, and the review policy. See *Built‑in Multi‑AI Agents* and *Custom Agents* in the Omnigent docs.

# COMMAND ----------

# DBTITLE 1,Product 4 — Contextual Policies (Guardrails)
# MAGIC %md
# MAGIC ## Product 4 — Contextual Policies (real‑time guardrails)
# MAGIC
# MAGIC **How it works:** a policy **intercepts every action** — tool calls, LLM requests, file operations — and returns one of three decisions:
# MAGIC
# MAGIC | Decision | Effect |
# MAGIC | --- | --- |
# MAGIC | **ALLOW** | Action proceeds |
# MAGIC | **ASK** | Pauses for your approval |
# MAGIC | **DENY** | Blocked with an error |
# MAGIC
# MAGIC Policies are **contextual / stateful** — each keeps state across the whole session, enabling **cumulative cost budgets, rate limits, and risk scoring**, not just static rules. Policies are checked in order and the **first to return a decision wins**. They can enforce **cost caps, tool‑call rate limits, risk scoring, PII blocking, repo/service access limits, model routing, and approval gates for destructive actions**.
# MAGIC
# MAGIC **Do it in the UI (session level — easiest):**
# MAGIC 1. **Ask Omnigent in plain language**, e.g. *"Add a policy that asks me before running any shell command"* or *"Cap this session at \$5 and warn me at \$1."*
# MAGIC 2. Or open **Settings → Policies** (`/settings/policies`) and **browse & toggle** built‑in policies.
# MAGIC 3. Run a task that trips the policy (e.g., a destructive shell command) and watch it **ASK** for approval or **DENY**.
# MAGIC
# MAGIC **Scope levels:** *Session* (this chat) → *Omnigent config* (every session) → *Server‑wide* (every user). Built‑in policies live under `omnigent.policies.builtins.*` (e.g. `safety.ask_on_os_tools`, `safety.max_tool_calls_per_session`, `cost.cost_budget`); custom policies are Python functions registered on the server.
# MAGIC
# MAGIC > Lab tie‑in: the governance you set on `ts-demo-ms` (rate limits, hallucination guardrail) is enforced at the **gateway**; Omnigent policies add guardrails at the **agent‑action** level — belt‑and‑suspenders across both.

# COMMAND ----------

# DBTITLE 1,Conclusion
# MAGIC %md
# MAGIC ## Conclusion
# MAGIC
# MAGIC Building on the model service (`ts-demo-ms`) you configured in Lab 2, this notebook explored how Unity AI Gateway goes beyond manual traffic splitting with intelligent routing:
# MAGIC
# MAGIC ### What we covered
# MAGIC | Section | Capability |
# MAGIC | --- | --- |
# MAGIC | **B** | Smart Routing via `ucode` — per-task model selection within a single coding harness |
# MAGIC | **D–E** | Telemetry queries on `system.ai_gateway.usage` and `system.billing.usage` |
# MAGIC | **F** | Cost comparison — single-model vs Smart Routing (40–60% savings) |
# MAGIC | **G** | Omnigent — cross-harness routing (model + harness selection per task) |
# MAGIC | **H** | Ongoing monitoring via the AI Gateway Usage Dashboard and custom dashboards |
# MAGIC
# MAGIC ### Key takeaways
# MAGIC * **Manual splits** (Lab 2) are great for A/B testing and gradual rollouts
# MAGIC * **Smart Routing** eliminates the need to pick percentages — the gateway AI routes each task to the lowest-cost capable model
# MAGIC * **Omnigent** extends this across coding harnesses (Claude Code vs Codex)
# MAGIC * All spend is observable via system tables and the built-in **AI Gateway Usage Dashboard**
# MAGIC
# MAGIC **Further reading:** [Smart Routing docs](https://docs.databricks.com/aws/en/ai-gateway/smart-routing/) | [Cost observability](https://docs.databricks.com/aws/en/ai-gateway/cost-observability/) | [ucode CLI](https://docs.databricks.com/aws/en/ai-gateway/coding-agent-integration-model-services/) | [Manage budgets](https://docs.databricks.com/aws/en/ai-gateway/budgets)

# COMMAND ----------

# DBTITLE 1,Classroom Cleanup
# Clean up the model service created in Lab 2
service_to_delete = f"{catalog}.{schema}.ts-demo-ms"

try:
    w.api_client.do("DELETE", f"/api/2.1/unity-catalog/model-services/{service_to_delete}")
    print(f"Deleted model service: {service_to_delete}")
except Exception as e:
    if "not found" in str(e).lower() or "does_not_exist" in str(e).lower():
        print(f"Model service '{service_to_delete}' does not exist (already cleaned up).")
    else:
        print(f"Note: {e}")

print("\nCleanup complete.")