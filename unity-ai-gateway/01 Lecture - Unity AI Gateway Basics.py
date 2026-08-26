# Databricks notebook source
# MAGIC %md
# MAGIC ![DB Academy Logo](https://files.training.databricks.com/binder/prod_main/unity-ai-gateway-en_us-1.0.0/images/20260821T112203Z/Includes/images/databricks_academy_logo.png)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Lecture - Unity AI Gateway Basics
# MAGIC ## Overview
# MAGIC **Unity AI Gateway** extends Unity Catalog governance to AI traffic. Unity Catalog already governs your models, MCP services, functions, and connections as securable objects; Unity AI Gateway adds a central control plane over the traffic to them — a governance feature set on your model and MCP serving endpoints — that enforces routing, rate limits, budgets, usage tracking, and **service policies** (guardrails) on every request and response. The result is one consistent governance layer for AI (the same model you already use for tables and volumes) spanning three dimensions: **asset governance** (Unity Catalog), **traffic governance** (the gateway), and **behavior governance** (service policies).
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lecture, you will be able to:
# MAGIC 1. **Explain** what Unity AI Gateway is and how asset, traffic, and behavior governance combine into a single control plane for AI.
# MAGIC 2. **Distinguish** Unity Catalog (which governs AI assets as securables) from Unity AI Gateway (which governs the live traffic to them), and describe how model services fit into Unity Catalog.
# MAGIC 3. **Describe** how the gateway manages routing and traffic (including endpoint fallback, rate limits, and budgets) across multiple models and providers.
# MAGIC 4. **Differentiate** access policies (who can reach a service, via UC grants) from guardrails (what content is allowed, via service policies), and explain the allow / require-approval / deny outcomes and fail-closed error semantics.
# MAGIC 5. **Identify** how to monitor usage, cost, and risk using inference tables and usage tracking, and where that data lives for auditing and analysis.
# MAGIC
# MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC     <div>
# MAGIC       <strong style="color: #0d47a1; font-size: 1.1em;">Note</strong>
# MAGIC       <p style="margin: 8px 0 0 0; color: #333;">
# MAGIC         Unity AI Gateway is now <a href="https://www.databricks.com/blog/unity-ai-gateway-generally-available" style="color: #1976d2; text-decoration: underline;">Generally Available</a>.
# MAGIC       </p>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. What is Unity AI Gateway?
# MAGIC
# MAGIC <div style="max-width: 1060px; margin: 0 auto;">
# MAGIC <svg width="100%" viewBox="0 0 1160 640" style="font-family: -apple-system, 'Segoe UI', Roboto, sans-serif;">
# MAGIC   <defs>
# MAGIC     <marker id="b1-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
# MAGIC       <path d="M0 0L10 5L0 10z" fill="#FF5F46"/>
# MAGIC     </marker>
# MAGIC   </defs>
# MAGIC   <rect x="275" y="105" width="565" height="400" rx="10" fill="#FFEDE6" stroke="none"/>
# MAGIC   <text x="295" y="128" font-size="20" font-weight="500" fill="#1B1B1B">Databricks</text>
# MAGIC
# MAGIC   <rect x="20" y="30" width="210" height="210" rx="4" fill="#fff" stroke="#B8B6B0" stroke-width="1.5" stroke-dasharray="5 4"/>
# MAGIC   <text x="34" y="52" font-size="18" fill="#6B6A66">coding agent config</text>
# MAGIC   <text x="34" y="76" font-size="18" fill="#1B1B1B" font-family="ui-monospace, 'SF Mono', Menlo, monospace">{</text>
# MAGIC   <text x="46" y="94" font-size="18" fill="#1B1B1B" font-family="ui-monospace, 'SF Mono', Menlo, monospace">"mcpServers": {</text>
# MAGIC   <text x="58" y="112" font-size="18" fill="#1B1B1B" font-family="ui-monospace, 'SF Mono', Menlo, monospace">"sql": "https://..."</text>
# MAGIC   <text x="46" y="130" font-size="18" fill="#1B1B1B" font-family="ui-monospace, 'SF Mono', Menlo, monospace">},</text>
# MAGIC   <text x="46" y="148" font-size="18" fill="#1B1B1B" font-family="ui-monospace, 'SF Mono', Menlo, monospace">"baseUrl": "https://...",</text>
# MAGIC   <text x="46" y="166" font-size="18" fill="#1B1B1B" font-family="ui-monospace, 'SF Mono', Menlo, monospace">"baseModel":</text>
# MAGIC   <text x="58" y="184" font-size="18" fill="#1B1B1B" font-family="ui-monospace, 'SF Mono', Menlo, monospace">"claude-opus-4-8"</text>
# MAGIC   <text x="34" y="202" font-size="18" fill="#1B1B1B" font-family="ui-monospace, 'SF Mono', Menlo, monospace">}</text>
# MAGIC   <circle cx="20" cy="322" r="8" fill="none" stroke="#FF5F46" stroke-width="1.5"/>
# MAGIC   <path d="M 4 348 Q 20 332 36 348 L 36 356 L 4 356 Z" fill="none" stroke="#FF5F46" stroke-width="1.5" stroke-linejoin="round"/>
# MAGIC   <text x="20" y="378" text-anchor="middle" font-size="18" fill="#6B6A66">User</text>
# MAGIC   <path d="M 38 344 L 77 344" stroke="#FF5F46" stroke-width="1.5" fill="none" marker-end="url(#b1-arrow)"/>
# MAGIC   <rect x="80" y="320" width="140" height="54" rx="6" fill="#FFD5CC" stroke="#FF5F46" stroke-width="1.5"/>
# MAGIC   <text x="150" y="347" text-anchor="middle" dominant-baseline="central" font-size="20" fill="#1B1B1B">Coding agent</text>
# MAGIC   <path d="M 220 344 L 317 344" stroke="#FF5F46" stroke-width="2" fill="none" marker-end="url(#b1-arrow)"/>
# MAGIC   <circle cx="269" cy="344" r="13" fill="#FF5F46"/>
# MAGIC   <text x="269" y="344" text-anchor="middle" dominant-baseline="central" font-size="17" font-weight="700" fill="#fff">1</text>
# MAGIC   <rect x="320" y="316" width="100" height="66" rx="6" fill="#FFD5CC" stroke="#FF5F46" stroke-width="1.5"/>
# MAGIC   <text x="370" y="339" text-anchor="middle" dominant-baseline="central" font-size="19" fill="#1B1B1B">Unified</text>
# MAGIC   <text x="370" y="359" text-anchor="middle" dominant-baseline="central" font-size="19" fill="#1B1B1B">auth</text>
# MAGIC   <path d="M 420 344 L 467 344" stroke="#FF5F46" stroke-width="2" fill="none" marker-end="url(#b1-arrow)"/>
# MAGIC   <circle cx="440" cy="344" r="13" fill="#FF5F46"/>
# MAGIC   <text x="440" y="344" text-anchor="middle" dominant-baseline="central" font-size="17" font-weight="700" fill="#fff">2</text>
# MAGIC   <rect x="470" y="145" width="60" height="330" rx="6" fill="#FFD5CC" stroke="#FF5F46" stroke-width="1.5"/>
# MAGIC   <text x="500" y="310" text-anchor="middle" font-size="20" fill="#1B1B1B" transform="rotate(-90 500 310)">Unity AI Gateway</text>
# MAGIC   <rect x="580" y="145" width="215" height="120" rx="6" fill="#FFD5CC" stroke="#FF5F46" stroke-width="1.5"/>
# MAGIC   <text x="688" y="205" text-anchor="middle" dominant-baseline="central" font-size="21" fill="#1B1B1B">Unity Catalog</text>
# MAGIC   <path d="M 530 175 L 578 175" stroke="#FF5F46" stroke-width="2" fill="none" marker-end="url(#b1-arrow)"/>
# MAGIC   <circle cx="554" cy="158" r="13" fill="#FF5F46"/>
# MAGIC   <text x="554" y="158" text-anchor="middle" dominant-baseline="central" font-size="17" font-weight="700" fill="#fff">3</text>
# MAGIC   <rect x="570" y="390" width="240" height="100" rx="6" fill="#fff" stroke="#B8B6B0" stroke-width="1.5" stroke-dasharray="5 4"/>
# MAGIC   <text x="580" y="384" font-size="18" fill="#6B6A66">MCPs</text>
# MAGIC   <rect x="585" y="405" width="80" height="66" rx="5" fill="#FFD5CC" stroke="#FF5F46" stroke-width="1.5"/>
# MAGIC   <text x="625" y="438" text-anchor="middle" dominant-baseline="central" font-size="18" fill="#1B1B1B">DBSQL</text>
# MAGIC   <rect x="675" y="405" width="58" height="66" rx="5" fill="#FFD5CC" stroke="#FF5F46" stroke-width="1.5"/>
# MAGIC   <text x="704" y="438" text-anchor="middle" dominant-baseline="central" font-size="18" fill="#1B1B1B">Genie</text>
# MAGIC   <rect x="743" y="405" width="58" height="66" rx="5" fill="#FFD5CC" stroke="#FF5F46" stroke-width="1.5"/>
# MAGIC   <text x="772" y="438" text-anchor="middle" dominant-baseline="central" font-size="18" fill="#1B1B1B">...</text>
# MAGIC   <path d="M 530 435 L 570 435" stroke="#FF5F46" stroke-width="2" fill="none" marker-end="url(#b1-arrow)"/>
# MAGIC   <circle cx="550" cy="418" r="13" fill="#FF5F46"/>
# MAGIC   <text x="550" y="418" text-anchor="middle" dominant-baseline="central" font-size="17" font-weight="700" fill="#fff">4</text>
# MAGIC   <path d="M 530 355 L 878 355" stroke="#FF5F46" stroke-width="2" fill="none" marker-end="url(#b1-arrow)"/>
# MAGIC   <circle cx="590" cy="355" r="13" fill="#FF5F46"/>
# MAGIC   <text x="590" y="355" text-anchor="middle" dominant-baseline="central" font-size="17" font-weight="700" fill="#fff">4</text>
# MAGIC   <path d="M 685 265 L 685 390" stroke="#B8B6B0" stroke-width="1.5" stroke-dasharray="5 4" fill="none"/>
# MAGIC   <text x="695" y="325" font-size="18" fill="#6B6A66" font-style="italic">registered</text>
# MAGIC   <path d="M 685 145 L 685 80 L 880 80" stroke="#B8B6B0" stroke-width="1.5" stroke-dasharray="5 4" fill="none"/>
# MAGIC   <text x="730" y="72" font-size="18" fill="#6B6A66" font-style="italic">registered</text>
# MAGIC   <rect x="880" y="25" width="255" height="540" rx="8" fill="#fff" stroke="#D8D6D0" stroke-width="1"/>
# MAGIC   <text x="898" y="52" font-size="22" font-weight="600" fill="#1B1B1B">External systems</text>
# MAGIC   <text x="898" y="78" font-size="18" fill="#6B6A66">MCP servers</text>
# MAGIC   <rect x="898" y="88" width="220" height="40" rx="5" fill="#fff" stroke="#D8D6D0" stroke-width="1"/>
# MAGIC   <text x="1008" y="108" text-anchor="middle" dominant-baseline="central" font-size="19" fill="#1B1B1B">Github</text>
# MAGIC   <rect x="898" y="142" width="220" height="40" rx="5" fill="#fff" stroke="#D8D6D0" stroke-width="1"/>
# MAGIC   <text x="1008" y="162" text-anchor="middle" dominant-baseline="central" font-size="19" fill="#1B1B1B">Slack</text>
# MAGIC   <rect x="898" y="196" width="220" height="40" rx="5" fill="#fff" stroke="#D8D6D0" stroke-width="1"/>
# MAGIC   <text x="1008" y="216" text-anchor="middle" dominant-baseline="central" font-size="19" fill="#1B1B1B">Jira</text>
# MAGIC   <rect x="898" y="250" width="220" height="40" rx="5" fill="#fff" stroke="#E8E7E2" stroke-width="1"/>
# MAGIC   <text x="1008" y="270" text-anchor="middle" dominant-baseline="central" font-size="19" fill="#B8B6B0">‹your_sys›</text>
# MAGIC   <text x="898" y="324" font-size="18" fill="#6B6A66">LLM providers</text>
# MAGIC   <rect x="898" y="338" width="220" height="40" rx="5" fill="#fff" stroke="#D8D6D0" stroke-width="1"/>
# MAGIC   <text x="1008" y="358" text-anchor="middle" dominant-baseline="central" font-size="19" fill="#1B1B1B">Anthropic Claude</text>
# MAGIC   <rect x="898" y="392" width="220" height="40" rx="5" fill="#fff" stroke="#D8D6D0" stroke-width="1"/>
# MAGIC   <text x="1008" y="412" text-anchor="middle" dominant-baseline="central" font-size="19" fill="#1B1B1B">OpenAI GPT</text>
# MAGIC   <rect x="898" y="446" width="220" height="40" rx="5" fill="#fff" stroke="#D8D6D0" stroke-width="1"/>
# MAGIC   <text x="1008" y="466" text-anchor="middle" dominant-baseline="central" font-size="19" fill="#1B1B1B">Google Gemini</text>
# MAGIC   <rect x="898" y="500" width="220" height="40" rx="5" fill="#fff" stroke="#E8E7E2" stroke-width="1"/>
# MAGIC   <text x="1008" y="520" text-anchor="middle" dominant-baseline="central" font-size="19" fill="#B8B6B0">‹your_model›</text>
# MAGIC   <circle cx="214" cy="595" r="13" fill="#FF5F46"/>
# MAGIC   <text x="214" y="595" text-anchor="middle" dominant-baseline="central" font-size="17" font-weight="700" fill="#fff">1</text>
# MAGIC   <text x="232" y="599" font-size="19" fill="#1B1B1B">Authenticate</text>
# MAGIC   <circle cx="360" cy="595" r="13" fill="#FF5F46"/>
# MAGIC   <text x="360" y="595" text-anchor="middle" dominant-baseline="central" font-size="17" font-weight="700" fill="#fff">2</text>
# MAGIC   <text x="378" y="599" font-size="19" fill="#1B1B1B">OBO → True</text>
# MAGIC   <circle cx="510" cy="595" r="13" fill="#FF5F46"/>
# MAGIC   <text x="510" y="595" text-anchor="middle" dominant-baseline="central" font-size="17" font-weight="700" fill="#fff">3</text>
# MAGIC   <text x="528" y="599" font-size="19" fill="#1B1B1B">Authorize</text>
# MAGIC   <circle cx="640" cy="595" r="13" fill="#FF5F46"/>
# MAGIC   <text x="640" y="595" text-anchor="middle" dominant-baseline="central" font-size="17" font-weight="700" fill="#fff">4</text>
# MAGIC   <text x="658" y="599" font-size="19" fill="#1B1B1B">Execute</text>
# MAGIC </svg>
# MAGIC </div>
# MAGIC
# MAGIC <details style="margin: 8px 0;">
# MAGIC <summary style="background: linear-gradient(135deg, #1B5162, #4299E0); color: white; padding: 12px 18px; cursor: pointer; font-weight: 600; font-size: 12pt; border-radius: 8px; user-select: none;">
# MAGIC Three dimensions of AI governance
# MAGIC </summary>
# MAGIC <div style="border: 2px solid #1B5162; border-top: none; border-radius: 0 0 8px 8px; padding: 16px 20px; background: #F9F7F4; font-size: 12pt; line-height: 1.7; color: #333;">
# MAGIC <p>Unity Catalog is the foundation for AI governance on Databricks. It governs your AI assets as securables, the same way it governs your data. Unity AI Gateway is the control plane for the traffic to those assets, and service policies (ABAC policies scoped to AI services) govern each request and response based on who is calling and what it contains. AI governance spans three dimensions:</p>
# MAGIC <ul style="margin: 12px 0; padding-left: 24px;">
# MAGIC <li><strong>Asset governance</strong>: Unity Catalog manages every model, MCP server, function, and connection as a securable object, governed with the same privileges and Attribute-Based Access Control (ABAC) grant policies (<a href="https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/grant-policies" style="color: #1976d2; text-decoration: underline;">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/abac/grant-policies" style="color: #1976d2; text-decoration: underline;">Azure</a> | <a href="https://docs.databricks.com/gcp/en/data-governance/unity-catalog/abac/grant-policies" style="color: #1976d2; text-decoration: underline;">GCP</a>) you use for tables and volumes.</li>
# MAGIC <li><strong>Traffic governance</strong>: Unity AI Gateway routes every model service and MCP service request from a central control plane, and enforces rate limits, budgets, and usage tracking.</li>
# MAGIC <li><strong>Behavior governance</strong>: Service policies (<a href="https://docs.databricks.com/aws/en/data-governance/unity-catalog/service-policies/" style="color: #1976d2; text-decoration: underline;">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/service-policies/" style="color: #1976d2; text-decoration: underline;">Azure</a> | <a href="https://docs.databricks.com/gcp/en/data-governance/unity-catalog/service-policies/" style="color: #1976d2; text-decoration: underline;">GCP</a>) are ABAC policies scoped to AI services that allow, deny, or require approval for individual requests and responses, based on who is calling and what the request and response contain.</li>
# MAGIC </ul>
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ### A1. Unity Catalog vs Unity AI Gateway
# MAGIC
# MAGIC <br></br>
# MAGIC
# MAGIC <div class="b1-pillar-overview" style="font-family: sans-serif; max-width: 1180px; margin: 0 auto; color: #0b2026;">
# MAGIC
# MAGIC <style>
# MAGIC .b1-pillar-overview * { box-sizing: border-box; }
# MAGIC
# MAGIC .b1-pillar-frame {
# MAGIC   border-radius: 14px;
# MAGIC   overflow: hidden;
# MAGIC   box-shadow: 0 6px 22px rgba(27,49,57,0.10);
# MAGIC   background: #F8F9FC;
# MAGIC }
# MAGIC
# MAGIC .b1-pillar-hero {
# MAGIC   background: linear-gradient(135deg, #1B5162 0%, #0b2026 100%);
# MAGIC   color: #ffffff;
# MAGIC   padding: 26px 36px 20px 36px;
# MAGIC   text-align: center;
# MAGIC }
# MAGIC .b1-pillar-hero-eyebrow { font-size: 14pt; letter-spacing: 3px; text-transform: uppercase; color: #B7CDD5; font-weight: 600; margin-bottom: 8px; }
# MAGIC .b1-pillar-hero-title { font-size: 28pt; font-weight: 800; letter-spacing: 1px; line-height: 1.1; margin-bottom: 8px; }
# MAGIC .b1-pillar-hero-tagline { font-size: 15pt; font-weight: 500; color: #E5EEF1; line-height: 1.45; max-width: 860px; margin: 0 auto; }
# MAGIC
# MAGIC .b1-pillar-row-hint { background: #F8F9FC; padding: 14px 24px 0 24px; font-size: 14pt; color: #5E7077; text-align: center; font-style: italic; }
# MAGIC
# MAGIC .b1-pillar-card-row { background: #F8F9FC; padding: 12px 24px 22px 24px; display: flex; justify-content: center; gap: 2%; width: 100%; box-sizing: border-box; }
# MAGIC
# MAGIC .b1-pillar-card {
# MAGIC   display: inline-block;
# MAGIC   vertical-align: top;
# MAGIC   width: 45%;
# MAGIC   font-size: 14pt;
# MAGIC   min-height: 260px;
# MAGIC   background: #F9F7F4;
# MAGIC   border-top: 8px solid #FF5F46;
# MAGIC   border-left: 2px solid transparent;
# MAGIC   border-right: 2px solid transparent;
# MAGIC   border-bottom: 2px solid transparent;
# MAGIC   border-radius: 10px;
# MAGIC   padding: 14px;
# MAGIC   position: relative;
# MAGIC   cursor: pointer;
# MAGIC   user-select: none;
# MAGIC   box-shadow: 0 2px 8px rgba(27,49,57,0.06);
# MAGIC   transition: transform 0.12s, box-shadow 0.12s;
# MAGIC }
# MAGIC .b1-pillar-card-inner { display: flex; flex-direction: column; gap: 8px; height: 100%; }
# MAGIC .b1-pillar-card:hover { transform: translateY(-2px); box-shadow: 0 6px 14px rgba(27,49,57,0.10); }
# MAGIC .b1-pillar-card.active { background: #ffffff; border-left-color: #FF5F46; border-right-color: #FF5F46; border-bottom-color: #FF5F46; }
# MAGIC .b1-pillar-card.active::after { content: ""; position: absolute; bottom: -12px; left: 50%; transform: translateX(-50%); width: 0; height: 0; border-left: 12px solid transparent; border-right: 12px solid transparent; border-top: 12px solid #FF5F46; z-index: 2; }
# MAGIC
# MAGIC .b1-pillar-card-title { font-size: 18pt; font-weight: 700; color: #0b2026; }
# MAGIC .b1-pillar-card-subtitle { font-size: 14pt; font-weight: 600; color: #5E7077; font-style: italic; line-height: 1.2; }
# MAGIC .b1-pillar-card-tagline { font-size: 14pt; line-height: 1.5; color: #0b2026; margin-top: auto; }
# MAGIC .b1-pillar-card-cta { font-size: 14pt; font-weight: 600; color: #5E7077; margin-top: 6px; }
# MAGIC .b1-pillar-card.active .b1-pillar-card-cta { color: #FF5F46; }
# MAGIC
# MAGIC .b1-pillar-detail-wrap { background: #F8F9FC; padding: 0 24px; overflow: hidden; max-height: 0; opacity: 0; transition: max-height 0.35s ease, opacity 0.28s ease, padding 0.28s ease; }
# MAGIC .b1-pillar-detail-wrap.open { max-height: 1800px; opacity: 1; padding: 4px 24px 24px 24px; }
# MAGIC
# MAGIC .b1-pillar-detail-card { background: #ffffff; border-radius: 10px; padding: 22px 24px; border-top: 8px solid #FF5F46; box-shadow: 0 2px 10px rgba(27,49,57,0.08); }
# MAGIC
# MAGIC .b1-pillar-detail-title { font-size: 18pt; font-weight: 800; color: #FF5F46; margin-bottom: 4px; line-height: 1.25; }
# MAGIC .b1-pillar-detail-sub { font-size: 14pt; color: #5E7077; font-style: italic; margin-bottom: 16px; }
# MAGIC
# MAGIC .b1-pillar-fact-list { margin-bottom: 14px; }
# MAGIC .b1-pillar-fact-item { display: grid; grid-template-columns: 220px 1fr; gap: 20px; padding: 14px 0; border-bottom: 1px solid #EEEDE9; align-items: start; }
# MAGIC .b1-pillar-fact-item:first-child { padding-top: 4px; }
# MAGIC .b1-pillar-fact-item:last-child { border-bottom: none; padding-bottom: 4px; }
# MAGIC .b1-pillar-fact-key { font-size: 14pt; font-weight: 700; letter-spacing: 1px; text-transform: uppercase; line-height: 1.3; color: #FF5F46; }
# MAGIC .b1-pillar-fact-val { font-size: 14pt; color: #0b2026; line-height: 1.55; }
# MAGIC .b1-pillar-fact-val ul { margin: 0; padding-left: 20px; }
# MAGIC .b1-pillar-fact-val li { margin-bottom: 6px; }
# MAGIC .b1-pillar-fact-val li:last-child { margin-bottom: 0; }
# MAGIC
# MAGIC
# MAGIC .b1-pillar-foundation { background: linear-gradient(135deg, #1B5162 0%, #0b2026 100%); color: #ffffff; text-align: center; padding: 14px 24px; font-size: 14pt; border-top: 4px solid #FF5F46; }
# MAGIC .b1-pillar-foundation-text { font-size: 15pt; font-weight: 600; color: #ffffff; }
# MAGIC </style>
# MAGIC
# MAGIC <div class="b1-pillar-frame">
# MAGIC
# MAGIC   <div class="b1-pillar-hero">
# MAGIC     <div class="b1-pillar-hero-eyebrow">AI Governance</div>
# MAGIC     <div class="b1-pillar-hero-title">UC vs UAIG</div>
# MAGIC     <div class="b1-pillar-hero-tagline">Unity Catalog and Unity AI Gateway are complementary layers of AI Governance.</div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div class="b1-pillar-row-hint">
# MAGIC     Click any card to expand details.
# MAGIC   </div>
# MAGIC
# MAGIC   <div class="b1-pillar-card-row"><div class="b1-pillar-card" data-id="0" onclick="b1Sel(0)"><div class="b1-pillar-card-inner">
# MAGIC       <div class="b1-pillar-card-title">Unity Catalog</div>
# MAGIC       <div class="b1-pillar-card-subtitle">Asset governance</div>
# MAGIC       <div class="b1-pillar-card-tagline">Defines what AI assets exist and who can access them using securables, privileges, and policy-based governance.</div>
# MAGIC       <div class="b1-pillar-card-cta">Click to expand &rarr;</div>
# MAGIC     </div></div><div class="b1-pillar-card" data-id="1" onclick="b1Sel(1)"><div class="b1-pillar-card-inner">
# MAGIC       <div class="b1-pillar-card-title">Unity AI Gateway</div>
# MAGIC       <div class="b1-pillar-card-subtitle">Traffic governance</div>
# MAGIC       <div class="b1-pillar-card-tagline">Controls how requests flow to model and MCP services, and applies runtime policies, limits, and tracking.</div>
# MAGIC       <div class="b1-pillar-card-cta">Click to expand &rarr;</div>
# MAGIC     </div></div></div>
# MAGIC
# MAGIC   <div class="b1-pillar-detail-wrap" id="b1-pillar-detail-wrap">
# MAGIC     <div class="b1-pillar-detail-card" id="b1-pillar-detail-card"></div>
# MAGIC   </div>
# MAGIC
# MAGIC   <div class="b1-pillar-foundation">
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC var B1_PILLARS = [
# MAGIC   {
# MAGIC     title: "Unity Catalog",
# MAGIC     sub: "The asset governance layer for AI assets.",
# MAGIC     facts: [
# MAGIC       { k: "What it governs", v: "AI assets as securable objects in Unity Catalog, including models, agents, MCP services and MCP servers, HTTP connections, custom tools implemented as Unity Catalog functions, and model services." },
# MAGIC       { k: "What it gives you", v: "Standard privileges and centralized governance for who can discover, use, and manage those assets across workspaces." },
# MAGIC       { k: "Why it matters", v: "It gives AI assets the same governance model as data: consistent permissions, discoverability, and policy-based access control." },
# MAGIC       { k: "Think of it as", v: "The system that answers <strong>what exists</strong> and <strong>who is allowed to use it</strong>." }
# MAGIC     ]
# MAGIC   },
# MAGIC   {
# MAGIC     title: "Unity AI Gateway",
# MAGIC     sub: "The control plane for AI traffic and runtime governance.",
# MAGIC     facts: [
# MAGIC       { k: "What it governs", v: "The request path to model services and MCP services, including routing, rate limits, usage tracking, budgets, and service-policy enforcement." },
# MAGIC       { k: "What it gives you", v: "Centralized control over how traffic reaches governed AI assets, plus runtime controls for requests and responses." },
# MAGIC       { k: "Why it matters", v: "It lets you manage behavior and traffic from one place instead of scattering controls across individual endpoints and tools." },
# MAGIC       { k: "Think of it as", v: "The system that answers <strong>how requests flow</strong>, <strong>what runtime rules apply</strong>, and <strong>what gets recorded</strong>." }
# MAGIC     ]
# MAGIC   }
# MAGIC ];
# MAGIC
# MAGIC var b1Current = null;
# MAGIC
# MAGIC function b1Sel(id) {
# MAGIC   var wrap = document.getElementById('b1-pillar-detail-wrap');
# MAGIC   var card = document.getElementById('b1-pillar-detail-card');
# MAGIC   var d = B1_PILLARS[id];
# MAGIC
# MAGIC   document.querySelectorAll('.b1-pillar-card').forEach(function(b) {
# MAGIC     var isActive = parseInt(b.dataset.id, 10) === id;
# MAGIC     b.classList.toggle('active', isActive);
# MAGIC   });
# MAGIC
# MAGIC   if (b1Current === id) {
# MAGIC     wrap.classList.remove('open');
# MAGIC     document.querySelectorAll('.b1-pillar-card').forEach(function(b) {
# MAGIC       b.classList.remove('active');
# MAGIC     });
# MAGIC     b1Current = null;
# MAGIC     return;
# MAGIC   }
# MAGIC
# MAGIC   b1Current = id;
# MAGIC
# MAGIC   var html = '<div class="b1-pillar-detail-title">' + d.title + '</div>'
# MAGIC     + '<div class="b1-pillar-detail-sub">' + d.sub + '</div>'
# MAGIC     + '<div class="b1-pillar-fact-list">';
# MAGIC
# MAGIC   for (var i = 0; i < d.facts.length; i++) {
# MAGIC     var f = d.facts[i];
# MAGIC     html += '<div class="b1-pillar-fact-item">'
# MAGIC       + '<div class="b1-pillar-fact-key">' + f.k + '</div>'
# MAGIC       + '<div class="b1-pillar-fact-val">' + f.v + '</div>'
# MAGIC       + '</div>';
# MAGIC   }
# MAGIC
# MAGIC   html += '</div>';
# MAGIC
# MAGIC   card.innerHTML = html;
# MAGIC   wrap.classList.add('open');
# MAGIC }
# MAGIC </script>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A2. Workspace-Scoped Endpoints and UC-Native Services for LLM Use-Cases
# MAGIC
# MAGIC <div style="display: flex; align-items: center; gap: 20px; flex-wrap: wrap; justify-content: center; margin: 16px 0; font-family: sans-serif;">
# MAGIC   <div style="border: 1.5px solid #618794; border-radius: 12px; padding: 20px 24px; width: 260px; background: #ffffff;">
# MAGIC     <div style="font-size: 16pt; font-weight: 700; color: #0b2026; text-align: center;">Legacy Model Serving Endpoints</div>
# MAGIC     <div style="font-size: 14pt; color: #618794; text-align: center; margin-top: 6px;">Workspace-scoped AI Gateway</div>
# MAGIC     <div style="border: 1px solid #618794; border-radius: 6px; background: #ffffff; padding: 6px 10px; text-align: center; margin-top: 14px; font-size: 14pt; color: #0b2026;">Endpoint in Workspace A</div>
# MAGIC     <div style="border: 1px solid #618794; border-radius: 6px; background: #ffffff; padding: 6px 10px; text-align: center; margin-top: 4px; font-size: 14pt; color: #0b2026;">Duplicate in Workspace B</div>
# MAGIC     <div style="border: 1px dashed #2272B4; border-radius: 6px; background: rgba(34,114,180,0.08); padding: 4px 10px; text-align: center; margin-top: 4px; font-size: 14pt; color: #0b2026;">Per-workspace permissions</div>
# MAGIC   </div>
# MAGIC   <div style="display: flex; flex-direction: column; align-items: center; gap: 8px; min-width: 140px;">
# MAGIC     <div style="font-size: 14pt; color: #618794; text-align: center; line-height: 1.4;">Define once,<br/>govern centrally</div>
# MAGIC     <svg width="120" height="20" viewBox="0 0 120 20" style="display: block;">
# MAGIC       <defs><marker id="cmp-arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse"><path d="M2 1L8 5L2 9" fill="none" stroke="#1B3139" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></marker></defs>
# MAGIC       <path d="M5 10 L110 10" fill="none" stroke="#1B3139" stroke-width="1.8" marker-end="url(#cmp-arrow)"/>
# MAGIC     </svg>
# MAGIC   </div>
# MAGIC   <div style="border: 2px solid #FF5F46; border-radius: 12px; padding: 20px 24px; width: 280px; background: #F9F7F4;">
# MAGIC     <div style="font-size: 16pt; font-weight: 700; color: #FF5F46; text-align: center;">Model Services<span style="display: inline-block; vertical-align: middle; margin-left: 8px; background: rgba(0,169,114,0.12); border: 1px solid #00A972; color: #00734d; font-family: sans-serif; font-size: 9pt; font-weight: 700; letter-spacing: 0.06em; padding: 2px 8px; border-radius: 999px; line-height: 1.4;">GA</span></div>
# MAGIC     <div style="font-size: 14pt; color: #618794; text-align: center; margin-top: 4px;">Unity Catalog securable object</div>
# MAGIC     <div style="border: 1px solid #618794; border-radius: 6px; background: #ffffff; padding: 6px 10px; text-align: center; margin-top: 10px; font-size: 14pt; color: #0b2026;">Defined once in catalog.schema</div>
# MAGIC     <div style="border: 1px solid #618794; border-radius: 6px; background: #ffffff; padding: 6px 10px; text-align: center; margin-top: 4px; font-size: 14pt; color: #0b2026;">Shared across the metastore</div>
# MAGIC     <div style="border: 1px solid #618794; border-radius: 6px; background: #ffffff; padding: 6px 10px; text-align: center; margin-top: 4px; font-size: 14pt; color: #0b2026;">Discoverable in Catalog Explorer</div>
# MAGIC     <div style="border: 1px solid #618794; border-radius: 6px; background: #ffffff; padding: 6px 10px; text-align: center; margin-top: 4px; font-size: 14pt; color: #0b2026;">Unity AI Gateway controls</div>
# MAGIC     <div style="border: 1px dashed #2272B4; border-radius: 6px; background: rgba(34,114,180,0.08); padding: 4px 10px; text-align: center; margin-top: 6px; font-size: 14pt; color: #0b2026;">Centralized UC privileges</div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <details style="margin: 8px 0;">
# MAGIC <summary style="background: linear-gradient(135deg, #1B5162, #4299E0); color: white; padding: 12px 18px; cursor: pointer; font-weight: 600; font-size: 12pt; border-radius: 8px; user-select: none;">
# MAGIC When and why model services are recommended
# MAGIC </summary>
# MAGIC <div style="border: 2px solid #1B5162; border-top: none; border-radius: 0 0 8px 8px; padding: 16px 20px; background: #F9F7F4; font-size: 12pt; line-height: 1.7; color: #333;">
# MAGIC
# MAGIC <p>Model serving endpoints are not legacy for all AI workloads. However, for most governed LLM use cases, Databricks recommends moving to model services because model services shift LLM access from workspace-scoped endpoints into Unity Catalog, where the service can be defined once, shared across workspaces, permissioned centrally, and managed through Unity AI Gateway controls.</p>
# MAGIC
# MAGIC <ul style="margin: 12px 0; padding-left: 24px;">
# MAGIC <li>Standardize approved models across multiple workspaces</li>
# MAGIC <li>Centralize access control instead of managing permissions workspace by workspace</li>
# MAGIC <li>Apply governance controls such as rate limits, service policies, and guardrails consistently</li>
# MAGIC <li>Track usage, token consumption, and costs in a system-level way</li>
# MAGIC </ul>
# MAGIC
# MAGIC </div>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A3. Legacy Callout
# MAGIC
# MAGIC
# MAGIC <!-- AI Gateway admonition (standalone) -->
# MAGIC <div style="display: flex; align-items: stretch; gap: 0; margin: 16px 0; border: 1px solid #FFBDB4; border-radius: 12px; overflow: hidden; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;">
# MAGIC   <div style="background: #FF3621; display: flex; align-items: center; justify-content: center; padding: 24px 28px; min-width: 60px;">
# MAGIC     <svg viewBox="0 0 24 24" style="width: 40px; height: 40px; fill: none; stroke: #fff; stroke-width: 2; stroke-linecap: round; stroke-linejoin: round;">
# MAGIC       <circle cx="12" cy="12" r="10"/>
# MAGIC       <line x1="12" y1="8" x2="12" y2="12"/>
# MAGIC       <line x1="12" y1="16" x2="12.01" y2="16"/>
# MAGIC     </svg>
# MAGIC   </div>
# MAGIC   <div style="background: #FFF4F2; padding: 18px 22px; display: flex; flex-direction: column; justify-content: center; flex: 1;">
# MAGIC     <span style="font-size: 14px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.08em; color: #D4321C; margin-bottom: 4px;">AI Gateway is Legacy</span>
# MAGIC     <p style="margin: 0; color: #1B3139; font-size: 14px; line-height: 1.6;">
# MAGIC       <strong>AI Gateway for Model Serving</strong>, now the legacy <strong>AI Gateway</strong>, is a workspace-scoped legacy approach. It provides endpoint permissions, rate limiting, payload logging/inference tables, usage tracking, traffic splitting, and limited fallback routing. However, support is endpoint-type dependent: fallbacks are limited to external-model endpoints, and legacy guardrails are not supported for custom model endpoints. It lacks <strong>Unity AI Gateway's</strong> Unity Catalog model-service architecture, centralized cross-workspace governance, native service policies, unified cost controls and budgets, and broader cross-service observability/tracing.  
# MAGIC     </p>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. Route and Traffic Management
# MAGIC
# MAGIC Unity AI Gateway routes requests to your model and MCP services from a central control plane, so you can manage capacity, availability, and spend across providers.
# MAGIC
# MAGIC <div style="max-width: 900px; margin: 0 auto;">
# MAGIC <svg width="100%" viewBox="0 0 680 290" role="img" style="font-family: sans-serif;">
# MAGIC   <title>Unity AI Gateway Endpoint fallback routing</title>
# MAGIC   <desc>Requests route from a client to Model 1, falling back to Model 2 or Model 3 on 429/5xx errors, with all outcomes logged to inference and usage tracking tables.</desc>
# MAGIC   <defs>
# MAGIC     <marker id="fb-green" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
# MAGIC       <path d="M2 1L8 5L2 9" fill="none" stroke="#00A972" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>
# MAGIC     </marker>
# MAGIC     <marker id="fb-red" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
# MAGIC       <path d="M2 1L8 5L2 9" fill="none" stroke="#98102A" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>
# MAGIC     </marker>
# MAGIC     <marker id="fb-gray" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
# MAGIC       <path d="M2 1L8 5L2 9" fill="none" stroke="#618794" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>
# MAGIC     </marker>
# MAGIC   </defs>
# MAGIC
# MAGIC   <!-- Gateway container -->
# MAGIC   <rect x="120" y="20" width="540" height="258" rx="16" fill="#F9F7F4" stroke="#1B3139" stroke-width="1.5" stroke-dasharray="7 4" stroke-opacity="0.4"/>
# MAGIC   <text x="390" y="46" text-anchor="middle" font-size="13" fill="#618794">Unity AI Gateway Endpoint</text>
# MAGIC
# MAGIC   <!-- Log box -->
# MAGIC   <rect x="275" y="60" width="230" height="56" rx="8" fill="#EEEDE9" stroke="#00A972" stroke-width="1.5"/>
# MAGIC   <text x="390" y="84" text-anchor="middle" font-size="13" font-weight="600" fill="#0b2026">Request logged</text>
# MAGIC   <text x="390" y="102" text-anchor="middle" font-size="11" fill="#618794">Inference &amp; usage tracking table</text>
# MAGIC
# MAGIC   <!-- Routing group -->
# MAGIC   <rect x="135" y="146" width="520" height="102" rx="12" fill="#2272B4" fill-opacity="0.08" stroke="#2272B4" stroke-width="1.5" stroke-dasharray="6 3"/>
# MAGIC   <text x="649" y="162" text-anchor="end" font-size="11" font-style="italic" fill="#2272B4">Routing targets</text>
# MAGIC
# MAGIC   <!-- Model 1 -->
# MAGIC   <rect x="148" y="158" width="132" height="78" rx="8" fill="#ffffff" stroke="#1B5162" stroke-width="1.5"/>
# MAGIC   <text x="214" y="184" text-anchor="middle" font-size="14" font-weight="600" fill="#0b2026">Model 1</text>
# MAGIC   <rect x="155" y="193" width="118" height="18" rx="4" fill="#FFAB00"/>
# MAGIC   <text x="214" y="202" text-anchor="middle" font-size="10" font-weight="600" fill="#0b2026" dominant-baseline="central">Original request</text>
# MAGIC   <text x="214" y="224" text-anchor="middle" font-size="11" fill="#618794">Primary route</text>
# MAGIC
# MAGIC   <!-- Model 2 -->
# MAGIC   <rect x="340" y="178" width="130" height="56" rx="8" fill="#ffffff" stroke="#1B5162" stroke-width="1.5"/>
# MAGIC   <text x="405" y="202" text-anchor="middle" font-size="14" font-weight="600" fill="#0b2026">Model 2</text>
# MAGIC   <text x="405" y="221" text-anchor="middle" font-size="11" fill="#618794">First fallback</text>
# MAGIC
# MAGIC   <!-- Model 3 -->
# MAGIC   <rect x="530" y="178" width="110" height="56" rx="8" fill="#ffffff" stroke="#1B5162" stroke-width="1.5"/>
# MAGIC   <text x="585" y="202" text-anchor="middle" font-size="14" font-weight="600" fill="#0b2026">Model 3</text>
# MAGIC   <text x="585" y="221" text-anchor="middle" font-size="11" fill="#618794">Last fallback</text>
# MAGIC
# MAGIC   <!-- Green: Model 1 to Log -->
# MAGIC   <path d="M214 158 L310 116" fill="none" stroke="#00A972" stroke-width="1.8" marker-end="url(#fb-green)"/>
# MAGIC   <text x="218" y="131" text-anchor="end" font-size="11" fill="#00A972">200 (success)</text>
# MAGIC
# MAGIC   <!-- Red: Model 1 to Model 2 -->
# MAGIC   <path d="M280 205 L340 205" fill="none" stroke="#98102A" stroke-width="1.8" marker-end="url(#fb-red)"/>
# MAGIC   <text x="310" y="198" text-anchor="middle" font-size="11" fill="#98102A">429/5xx</text>
# MAGIC
# MAGIC   <!-- Green: Model 2 to Log -->
# MAGIC   <path d="M405 178 L390 116" fill="none" stroke="#00A972" stroke-width="1.8" marker-end="url(#fb-green)"/>
# MAGIC   <text x="406" y="131" text-anchor="start" font-size="11" fill="#00A972">200</text>
# MAGIC
# MAGIC   <!-- Red: Model 2 to Model 3 -->
# MAGIC   <path d="M470 205 L530 205" fill="none" stroke="#98102A" stroke-width="1.8" stroke-dasharray="6 3" marker-end="url(#fb-red)"/>
# MAGIC   <text x="500" y="198" text-anchor="middle" font-size="11" fill="#98102A">429/5xx</text>
# MAGIC
# MAGIC   <!-- Green: Model 3 to Log -->
# MAGIC   <path d="M585 178 L470 116" fill="none" stroke="#00A972" stroke-width="1.8" marker-end="url(#fb-green)"/>
# MAGIC   <text x="505" y="131" text-anchor="start" font-size="11" fill="#00A972">200</text>
# MAGIC
# MAGIC   <!-- Client -->
# MAGIC   <rect x="28" y="186" width="64" height="38" rx="8" fill="#ffffff" stroke="#618794" stroke-width="1.5"/>
# MAGIC   <text x="60" y="205" text-anchor="middle" font-size="13" fill="#0b2026" dominant-baseline="central">Client</text>
# MAGIC   <path d="M92 205 L148 205" fill="none" stroke="#618794" stroke-width="1.8" marker-end="url(#fb-gray)"/>
# MAGIC </svg>
# MAGIC </div>
# MAGIC
# MAGIC <details style="margin: 8px 0;">
# MAGIC <summary style="background: linear-gradient(135deg, #1B5162, #4299E0); color: white; padding: 12px 18px; cursor: pointer; font-weight: 600; font-size: 12pt; border-radius: 8px; user-select: none;">
# MAGIC Why route through Unity AI Gateway
# MAGIC </summary>
# MAGIC <div style="border: 2px solid #1B5162; border-top: none; border-radius: 0 0 8px 8px; padding: 16px 20px; background: #F9F7F4; font-size: 12pt; line-height: 1.7; color: #333;">
# MAGIC <p>Without Unity AI Gateway, each agent or application would have to implement its own access control, guardrails, and logging, making safety, compliance, debugging, and cost control much harder to do reliably at scale (especially across multiple models and providers). The diagram below illustrates how Unity AI Gateway handles endpoint fallback routing.</p>
# MAGIC </div>
# MAGIC </details>
# MAGIC
# MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC <div>
# MAGIC <strong style="color: #0d47a1; font-size: 1.1em;">Note</strong>
# MAGIC <p style="margin: 8px 0 0 0; color: #333;">
# MAGIC Databricks Foundation Model APIs integrate with Unity AI Gateway, but you enable Unity AI Gateway features (like usage tracking and inference tables) per endpoint. In the new Unity AI Gateway, usage tracking is on by default for Gateway endpoints, once the feature is enabled and the endpoint is created.
# MAGIC See: Unity AI Gateway documentation (<a href="https://docs.databricks.com/aws/en/ai-gateway/" style="color: #1976d2; text-decoration: underline;">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/ai-gateway/" style="color: #1976d2; text-decoration: underline;">Azure</a> | <a href="https://docs.databricks.com/gcp/en/ai-gateway/" style="color: #1976d2; text-decoration: underline;">GCP</a>)
# MAGIC </p>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. Setting Guardrails and Access Policies
# MAGIC <div class="scn" style="max-width: 1180px; margin: 0 auto; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;"><style>.scn .card{background:transparent;border-radius:10px;padding:22px;box-shadow:none;box-sizing:border-box;border:none;}.scn .caption{color:#5A6F77;font-size:16px;line-height:1.4;margin-top:10px;}.scn svg text{fill:#0b2026;}.scn .muted{fill:#618794;}</style><div class="card"><svg viewBox="0 0 1240 270" role="img" style="width:100%; height:auto; display:block; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;"><title>Guardrails and access policies in Unity AI Gateway</title><desc>A caller passes an access-policy check (Unity Catalog grant), then request guardrails, reaches the model service, and the response passes response guardrails before returning. Service policies resolve each call as allow, require approval, or deny, and fail closed.</desc><defs><marker id="d-teal" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M2 1L8 5L2 9" fill="none" stroke="#1B5162" stroke-width="2.0" stroke-linecap="round" stroke-linejoin="round"/></marker><marker id="d-muted" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="8" markerHeight="8" orient="auto-start-reverse"><path d="M2 1L8 5L2 9" fill="none" stroke="#618794" stroke-width="2.0" stroke-linecap="round" stroke-linejoin="round"/></marker></defs><rect x="20" y="40" width="190" height="200" rx="8" fill="#F9F7F4" stroke="#618794" stroke-width="1.6"/><text x="115" y="84" text-anchor="middle" font-size="20" font-weight="800" fill="#0b2026">Caller</text><rect x="36" y="96" width="158" height="32" rx="6" fill="#ffffff" stroke="#EEEDE9" stroke-width="1.4"/><text x="115" y="117" text-anchor="middle" font-size="16" fill="#0b2026">User</text><rect x="36" y="134" width="158" height="32" rx="6" fill="#ffffff" stroke="#EEEDE9" stroke-width="1.4"/><text x="115" y="155" text-anchor="middle" font-size="16" fill="#0b2026">Databricks App</text><rect x="36" y="172" width="158" height="32" rx="6" fill="#ffffff" stroke="#EEEDE9" stroke-width="1.4"/><text x="115" y="193" text-anchor="middle" font-size="16" fill="#0b2026">Agent</text><path d="M210 140 L258 140" fill="none" stroke="#1B5162" stroke-width="2.6" marker-end="url(#d-teal)"/><rect x="262" y="40" width="250" height="200" rx="10" fill="#F9F7F4" stroke="#618794" stroke-width="1.6"/><text x="387" y="86" text-anchor="middle" font-size="20" font-weight="800" fill="#0b2026">Unity Catalog grant</text><text x="387" y="116" text-anchor="middle" font-size="16" fill="#5A6F77">"Can they reach it?"</text><rect x="292" y="140" width="190" height="34" rx="6" fill="#ffffff" stroke="#EEEDE9" stroke-width="1.4"/><text x="387" y="162" text-anchor="middle" font-size="16" fill="#0b2026">EXECUTE on securable</text><text x="387" y="214" text-anchor="middle" font-size="16" font-style="italic" fill="#618794">who can call the asset</text><path d="M512 140 L560 140" fill="none" stroke="#1B5162" stroke-width="2.6" marker-end="url(#d-teal)"/><rect x="564" y="40" width="320" height="200" rx="10" fill="rgba(255,95,70,0.07)" stroke="#FF5F46" stroke-width="2.8"/><text x="724" y="82" text-anchor="middle" font-size="20" font-weight="800" fill="#0b2026">Service policy</text><text x="724" y="108" text-anchor="middle" font-size="16" fill="#5A6F77">request &amp; response</text><rect x="580" y="122" width="288" height="28" rx="6" fill="#ffffff" stroke="#EEEDE9" stroke-width="1.3"/><text x="724" y="141" text-anchor="middle" font-size="16" fill="#0b2026">PII exposure</text><rect x="580" y="154" width="288" height="28" rx="6" fill="#ffffff" stroke="#EEEDE9" stroke-width="1.3"/><text x="724" y="173" text-anchor="middle" font-size="16" fill="#0b2026">Prompt injection · jailbreak</text><rect x="580" y="186" width="288" height="28" rx="6" fill="#ffffff" stroke="#EEEDE9" stroke-width="1.3"/><text x="724" y="205" text-anchor="middle" font-size="16" fill="#0b2026">Unsafe content · topics</text><text x="724" y="230" text-anchor="middle" font-size="16" font-style="italic" fill="#618794">built-in or custom guardrails</text><path d="M884 128 L980 128" fill="none" stroke="#1B5162" stroke-width="2.6" marker-end="url(#d-teal)"/><text x="932" y="118" text-anchor="middle" font-size="16" fill="#618794">request</text><path d="M980 152 L884 152" fill="none" stroke="#618794" stroke-width="2.6" stroke-dasharray="6 3" marker-end="url(#d-muted)"/><text x="936" y="172" text-anchor="middle" font-size="16" fill="#618794">response</text><rect x="984" y="40" width="246" height="200" rx="10" fill="#F9F7F4" stroke="#618794" stroke-width="1.6"/><text x="1107" y="125" text-anchor="middle" font-size="20" font-weight="800" fill="#0b2026">Model &amp; MCP</text><text x="1107" y="150" text-anchor="middle" font-size="20" font-weight="800" fill="#0b2026">service</text><text x="1107" y="180" text-anchor="middle" font-size="16" fill="#618794">catalog.schema.name</text></svg></div></div><details style="margin: 8px 0;"><summary style="background: linear-gradient(135deg, #1B5162, #4299E0); color: white; padding: 12px 18px; cursor: pointer; font-weight: 600; font-size: 12pt; border-radius: 8px; user-select: none;">Access policies vs. guardrails</summary><div style="border: 2px solid #1B5162; border-top: none; border-radius: 0 0 8px 8px; padding: 16px 20px; background: #F9F7F4; font-size: 12pt; line-height: 1.7; color: #333;"><p>In Unity AI Gateway, <strong>access policies</strong> decide <em>who can reach an AI service in the first place</em>, and <strong>guardrails</strong> are the content checks on <em>what actually flows through</em>. Unity Catalog grants determine whether a principal can call a service; service policies then govern how each request and response proceeds (allow, deny, or require approval).</p><ul style="margin: 12px 0; padding-left: 24px;"><li><strong>Access policies</strong> (the Unity Catalog permissions layer): they control <em>who can call which AI asset</em> (models, MCP services, model provider services, and related tools), using the same UC-style privilege model you already use for data assets.</li><li><strong>Guardrails</strong>: the checks for risks like PII exposure, prompt injection, jailbreaks, and unsafe content. In Unity AI Gateway they're implemented through <strong>service policies</strong>, which can use built-in Databricks guardrails or custom logic, and they evaluate <strong>both the request and the response</strong>.</li></ul><p>The diagram above shows how the two layers combine into a single guarded path, and the three ways a service policy can resolve a call.</p></div></details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## D. Monitor Usage, Cost, and Risk
# MAGIC <div class="scn" role="region" aria-label="Monitor Usage, Cost, and Risk" style="max-width:1100px;margin:16px auto;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;color:#0b2026;">
# MAGIC <style>
# MAGIC .scn .title{font-size:22pt;font-weight:800;color:#0b2026;margin:0 0 12px 0;}
# MAGIC .scn .subtitle{font-size:14pt;color:#5A6F77;margin:0 0 16px 0;line-height:1.5;}
# MAGIC .scn .grid{display:flex;gap:16px;flex-wrap:wrap;}
# MAGIC .scn .cardbtn{appearance:none;border:none;background:transparent;padding:0;margin:0;cursor:pointer;flex:1;min-width:260px;text-align:left;display:flex;flex-direction:column;}
# MAGIC .scn .card{background:#F9F7F4;border-radius:10px;padding:18px 20px;box-shadow:0 2px 8px rgba(27,49,57,0.06);border:1px solid #EEEDE9;position:relative;overflow:hidden;min-height:118px;flex:1;}
# MAGIC .scn .card::before{content:"";position:absolute;left:0;top:0;height:8px;width:100%;background:#4299E0;}
# MAGIC .scn .card.cost::before{background:#00A972;}
# MAGIC .scn .card.risk::before{background:#FF5F46;}
# MAGIC .scn .card h3{margin:2px 0 8px 0;font-size:16pt;font-weight:800;color:#0b2026;}
# MAGIC .scn .card p{margin:0;font-size:14pt;color:#5A6F77;line-height:1.5;}
# MAGIC .scn .cardbtn:focus-visible .card{outline:3px solid #2272B4;outline-offset:2px;}
# MAGIC .scn .cardbtn[aria-selected="true"] .card{border-color:#1B5162;box-shadow:0 2px 10px rgba(27,49,57,0.10);}
# MAGIC .scn .cardbtn[aria-selected="true"] .card p{color:#0b2026;}
# MAGIC .scn .panelwrap{margin-top:14px;}
# MAGIC .scn .panel{display:none;background:#F9F7F4;border:2px solid #1B5162;border-radius:10px;box-shadow:0 2px 8px rgba(27,49,57,0.06);padding:16px 18px;}
# MAGIC .scn .panel.active{display:block;}
# MAGIC .scn .panel .hdr{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:10px;}
# MAGIC .scn .panel .hdrl{display:flex;align-items:center;gap:10px;}
# MAGIC .scn .badge{font-size:14pt;font-weight:800;color:white;background:#1B5162;padding:6px 10px;border-radius:8px;}
# MAGIC .scn .badge.usage{background:#4299E0;}
# MAGIC .scn .badge.cost{background:#00A972;}
# MAGIC .scn .badge.risk{background:#FF5F46;}
# MAGIC .scn .closebtn{appearance:none;border:1px solid #EEEDE9;background:white;color:#0b2026;border-radius:8px;padding:8px 10px;font-size:14pt;cursor:pointer;}
# MAGIC .scn .closebtn:focus-visible{outline:3px solid #2272B4;outline-offset:2px;}
# MAGIC .scn .panel p{margin:10px 0 0 0;font-size:14pt;color:#0b2026;line-height:1.6;}
# MAGIC .scn .panel ul{margin:10px 0 0 0;padding-left:22px;font-size:14pt;color:#0b2026;line-height:1.6;}
# MAGIC .scn .panel li{margin-bottom:10px;}
# MAGIC .scn a{color:#2272B4;text-decoration:underline;}
# MAGIC .scn .note{margin-top:16px;background:#F8F9FC;border:2px solid #1B5162;border-radius:10px;padding:14px 16px;}
# MAGIC .scn .note .nt{font-size:14pt;font-weight:800;color:#1B5162;margin:0 0 6px 0;}
# MAGIC .scn .note p{margin:0;font-size:14pt;color:#0b2026;line-height:1.6;}
# MAGIC .scn .sr{position:absolute;width:1px;height:1px;padding:0;margin:-1px;overflow:hidden;clip:rect(0,0,0,0);white-space:nowrap;border:0;}
# MAGIC </style>
# MAGIC
# MAGIC <div class="subtitle">Click a card to reveal details. Click it again (or press Close) to hide.</div>
# MAGIC <div class="grid" role="tablist" aria-label="Monitoring categories">
# MAGIC <button class="cardbtn" id="tab-usage" role="tab" aria-controls="panel-usage" aria-selected="false">
# MAGIC <div class="card usage" aria-hidden="true">
# MAGIC <h3>Usage</h3>
# MAGIC <p>Which principals, apps, and models are driving traffic: request counts, tokens, and latency, tracked centrally per endpoint.</p>
# MAGIC </div>
# MAGIC </button>
# MAGIC <button class="cardbtn" id="tab-cost" role="tab" aria-controls="panel-cost" aria-selected="false">
# MAGIC <div class="card cost" aria-hidden="true">
# MAGIC <h3>Cost</h3>
# MAGIC <p>Spend attributed back to teams and use cases, with rate limits and budgets to cap runaway consumption before it becomes a bill.</p>
# MAGIC </div>
# MAGIC </button>
# MAGIC <button class="cardbtn" id="tab-risk" role="tab" aria-controls="panel-risk" aria-selected="false">
# MAGIC <div class="card risk" aria-hidden="true">
# MAGIC <h3>Risk</h3>
# MAGIC <p>An auditable record of what was requested, what was returned, and which service-policy decisions (allow / deny / require approval) fired.</p>
# MAGIC </div>
# MAGIC </button>
# MAGIC </div>
# MAGIC <div class="panelwrap">
# MAGIC <div class="panel" id="panel-usage" role="tabpanel" aria-labelledby="tab-usage" aria-hidden="true">
# MAGIC <div class="hdr"><div class="hdrl"><div class="badge usage">Usage</div><div style="font-size:16pt;font-weight:800;color:#0b2026;">What the gateway captures</div></div><button class="closebtn" type="button" data-close="panel-usage">Close</button></div>
# MAGIC <p>Governance isn't only about blocking bad calls at request time; it's also about observability after the fact: knowing who called what, how much it cost, and whether anything was blocked. Because every request flows through the central control plane, Unity AI Gateway can record each one to Unity Catalog automatically, giving you an auditable record for cost management, debugging, and compliance without any per-application logging code.</p>
# MAGIC <ul>
# MAGIC <li><strong>Usage tracking</strong>: per-request accounting of volume, tokens, and the underlying model that served the call. This powers rate limits and budget enforcement, and answers “how much is each team/app/model costing us?” Usage tracking is on by default for gateway endpoints once the feature is enabled.</li>
# MAGIC <li><strong>Inference tables</strong>: Delta tables, managed by Unity Catalog, that log the requests and responses for auditing and analysis. Because they're governed Delta tables, you can query them with SQL, build dashboards, and join them to other data.</li>
# MAGIC </ul>
# MAGIC </div>
# MAGIC <div class="panel" id="panel-cost" role="tabpanel" aria-labelledby="tab-cost" aria-hidden="true">
# MAGIC <div class="hdr"><div class="hdrl"><div class="badge cost">Cost</div><div style="font-size:16pt;font-weight:800;color:#0b2026;">Budgets: turning usage tracking into cost control</div></div><button class="closebtn" type="button" data-close="panel-cost">Close</button></div>
# MAGIC <p><strong>Budgets</strong> build on usage tracking to cap Unity AI Gateway spend before it becomes a surprise bill. A budget can be <strong>workspace-scoped</strong> (single workspace) or <strong>account-wide</strong> (spanning the account), and you set monthly limits against billing records:</p>
# MAGIC <ul>
# MAGIC <li><strong>Shared threshold</strong>: a single monthly limit that applies across everyone the budget covers.</li>
# MAGIC <li><strong>Per-user thresholds</strong>: a monthly limit applied to each user, with optional overrides for specific groups.</li>
# MAGIC <li><strong>Hard caps (usage blocking)</strong>: when a threshold is reached, the gateway can block further requests.</li>
# MAGIC </ul>
# MAGIC <p>Budgets are enforced on a near-real-time estimate of spend and block future requests once a limit is hit; they are not a guarantee of an absolute cap on your final billed amount. See <a href="https://docs.databricks.com/aws/en/ai-gateway/budgets">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/ai-gateway/budgets">Azure</a> | <a href="https://docs.databricks.com/gcp/en/ai-gateway/budgets">GCP</a>.</p>
# MAGIC </div>
# MAGIC <div class="panel" id="panel-risk" role="tabpanel" aria-labelledby="tab-risk" aria-hidden="true">
# MAGIC <div class="hdr"><div class="hdrl"><div class="badge risk">Risk</div><div style="font-size:16pt;font-weight:800;color:#0b2026;">Auditability and policy decisions</div></div><button class="closebtn" type="button" data-close="panel-risk">Close</button></div>
# MAGIC <p>Risk monitoring focuses on having an auditable record: what was requested, what was returned, and which service-policy decisions fired (allow/deny/require approval). Because logs land in governed Unity Catalog tables, you can support investigations, compliance reporting, and debugging with consistent access control and lineage.</p>
# MAGIC <ul>
# MAGIC <li><strong>Who/what</strong>: principal identity, app, endpoint, and model.</li>
# MAGIC <li><strong>What happened</strong>: response status, latency, and any denials/approvals.</li>
# MAGIC <li><strong>Evidence</strong>: queryable, governed records for downstream analysis and reporting.</li>
# MAGIC </ul>
# MAGIC </div>
# MAGIC </div>
# MAGIC <script>
# MAGIC (function(){const root=document.currentScript.parentElement;const tabs=[...root.querySelectorAll('.cardbtn[role="tab"]')];const panels=[...root.querySelectorAll('.panel[role="tabpanel"]')];function setActive(id){tabs.forEach(t=>{const on=t.getAttribute('aria-controls')===id;t.setAttribute('aria-selected',on?'true':'false');});panels.forEach(p=>{const on=p.id===id;p.classList.toggle('active',on);p.setAttribute('aria-hidden',on?'false':'true');});}function clearAll(){tabs.forEach(t=>t.setAttribute('aria-selected','false'));panels.forEach(p=>{p.classList.remove('active');p.setAttribute('aria-hidden','true');});}tabs.forEach(t=>{t.addEventListener('click',()=>{const id=t.getAttribute('aria-controls');const isOn=t.getAttribute('aria-selected')==='true';if(isOn){clearAll();}else{setActive(id);}});t.addEventListener('keydown',(e)=>{if(e.key==='Enter'||e.key===' '){e.preventDefault();t.click();}if(e.key==='Escape'){e.preventDefault();clearAll();t.focus();}});});root.querySelectorAll('[data-close]').forEach(b=>{b.addEventListener('click',()=>{clearAll();const pid=b.getAttribute('data-close');const tab=root.querySelector('[aria-controls="'+pid+'"]');if(tab) tab.focus();});});})();
# MAGIC </script>
# MAGIC </div>
# MAGIC
# MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC <div>
# MAGIC <strong style="color: #0d47a1; font-size: 1.1em;">Note</strong>
# MAGIC <p style="margin: 8px 0 0 0; color: #333;">
# MAGIC Usage tracking and inference tables are enabled per endpoint (usage tracking is on by default for gateway endpoints once the feature is enabled). Because the logged data lands in Unity Catalog, the same permissions and lineage that govern your other data assets govern your AI audit trail too. See Unity AI Gateway docs: (<a href="https://docs.databricks.com/aws/en/ai-gateway/" style="color: #1976d2; text-decoration: underline;">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/ai-gateway/" style="color: #1976d2; text-decoration: underline;">Azure</a> | <a href="https://docs.databricks.com/gcp/en/ai-gateway/" style="color: #1976d2; text-decoration: underline;">GCP</a>).
# MAGIC </p>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; <span id="dbx-year"></span> Databricks, Inc. All rights reserved.
# MAGIC Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
# MAGIC <script>
# MAGIC   document.getElementById("dbx-year").textContent = new Date().getFullYear();
# MAGIC </script>
# MAGIC