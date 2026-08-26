# Databricks notebook source
# MAGIC %md
# MAGIC ![DB Academy](https://files.training.databricks.com/binder/prod_main/unity-ai-gateway-en_us-1.0.0/images/20260821T112203Z/Includes/images/databricks_academy_logo.png)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Unity AI Gateway
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC **Unity AI Gateway** extends Unity Catalog governance to AI traffic — providing a central control plane over your model and MCP serving endpoints that enforces routing, rate limits, budgets, usage tracking, and **service policies** (guardrails) on every request and response. In this course, you will learn the fundamentals of Unity AI Gateway and then configure it end-to-end for an agentic application that generates Databricks-native diagrams as code. 
# MAGIC
# MAGIC ## Terminal Objectives
# MAGIC
# MAGIC By the end of this course, you will be able to:
# MAGIC
# MAGIC - Explain what Unity AI Gateway is and how asset, traffic, and behavior governance combine into a single control plane for AI
# MAGIC - Distinguish Unity Catalog (which governs AI assets as securables) from Unity AI Gateway (which governs the live traffic to them)
# MAGIC - Create and configure model services in Unity AI Gateway for agentic applications
# MAGIC - Grant execution permissions on model services to application service principals
# MAGIC - Configure rate limits, traffic splitting, and fallback routing across multiple models
# MAGIC - Apply output guardrails (service policies) to govern agent responses
# MAGIC - Set up inference tables for request/response logging and observability
# MAGIC - Query system catalog tables (`system.ai`, `system.ai_gateway`) for usage telemetry

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Prerequisites
# MAGIC
# MAGIC Before starting this course, learners should be comfortable with the following:
# MAGIC
# MAGIC - Basic familiarity with **Unity Catalog** concepts (catalogs, schemas, securables, grants)
# MAGIC - Understanding of **model serving endpoints** and the OpenAI-compatible API format
# MAGIC - Basic Python programming and familiarity with REST APIs
# MAGIC - General awareness of LLM-based applications and agentic architectures

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. Workspace Setup Information
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="border-left: 4px solid #f44336; background: #ffebee; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC <div>
# MAGIC <strong style="color: #c62828; font-size: 1.1em;">Prerequisites</strong>
# MAGIC <p style="margin: 8px 0 0 0; color: #333;">This content assumes you have:</p>
# MAGIC
# MAGIC - <strong>CAN USE access on at least one serverless SQL warehouse (2X-Small is sufficient) and Serverless Compute.</strong> 
# MAGIC - <strong>Unity Catalog</strong> enabled in your workspace
# MAGIC - Permission to <strong>create a catalog</strong> in your workspace
# MAGIC - Account admin has enabled the following <strong>Previews</strong>: **Service policies**, **Managed MLflow Prompt Registry**
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B1. Databricks Provided Vocareum Workspace (Recommended)
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #1976d2;
# MAGIC   background: #e3f2fd;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC - If you are running this notebook in a <strong>Databricks Academy provided Vocareum workspace</strong>, your Unity Catalog catalog is already created for you.
# MAGIC
# MAGIC - Your catalog name matches your Vocareum username and looks like: <strong>labuser12345</strong> (series of unique numbers)
# MAGIC
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B2. Databricks Free Edition or Outside Workspaces (*as is*)
# MAGIC
# MAGIC ##### Databricks Free Edition or Other Workspaces may work for this, but it is provided **as is** and support is not guaranteed.
# MAGIC
# MAGIC Some features may not be available depending on the capabilities of Databricks Free Edition or your Workspace.
# MAGIC
# MAGIC Please read below to setup your environment
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #1976d2;
# MAGIC   background: #e3f2fd;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC <div style="color:#333;">
# MAGIC
# MAGIC #### Catalog Information
# MAGIC
# MAGIC - If you are running this notebook in your own Databricks workspace or Databricks Free Edition, the setup will <strong>create a Unity Catalog catalog and schema for you</strong>.
# MAGIC
# MAGIC - The <strong>Create Catalog</strong> permission is required.
# MAGIC
# MAGIC - The catalog name is derived from your Databricks username and follows this pattern: <strong>labuser_YOUR_ID</strong>
# MAGIC
# MAGIC <br></br>
# MAGIC #### Access Marketplace Data
# MAGIC
# MAGIC Marketplace data is not required.
# MAGIC
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #ff9800;
# MAGIC   background: #fff3e0;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC
# MAGIC   <strong style="display:block; color:#e65100; margin-bottom:6px; font-size: 1.1em;">
# MAGIC     Troubleshooting Setup - Your Workspace Can't Create Catalogs
# MAGIC   </strong>
# MAGIC <details>
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC If you do not have permission to create a new catalog but already have one available, you can explicitly specify an existing catalog by updating the following:
# MAGIC
# MAGIC - In the demo notebook (`02 Demo`), specify your catalog in the `%run` setup cell:
# MAGIC   - `%run ./Includes/Classroom-Setup-1 $catalog_override = "YOUR_CATALOG_NAME"`
# MAGIC
# MAGIC   </div>
# MAGIC </details>
# MAGIC </div>
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #f44336;
# MAGIC   background: #ffebee;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC <strong style="display:block; color:#c62828; margin-bottom:6px; font-size: 1.1em;">Do Not Run in Production Environments</strong>
# MAGIC
# MAGIC <div style="color:#333;">
# MAGIC <ul>
# MAGIC <li>Only run this course in <strong>development or sandbox workspaces</strong>.</li>
# MAGIC <li>Do not run in production environments. The setup scripts creates catalogs, schemas and pipelines in your workspace.</li>
# MAGIC </ul>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. Course Agenda
# MAGIC
# MAGIC
# MAGIC <div style="max-width: 1200px; margin: 0 auto; font-family: sans-serif;">
# MAGIC <div style="background: #F9F7F4; border-radius: 10px; padding: 20px 24px; box-shadow: 0 2px 8px rgba(27,49,57,0.06);">
# MAGIC
# MAGIC <style>
# MAGIC .agenda-table td, .agenda-table th {
# MAGIC   font-size: 14pt !important;
# MAGIC }
# MAGIC </style>
# MAGIC
# MAGIC <table class="agenda-table" style="width: 100%; border-collapse: collapse; line-height: 1.5;">
# MAGIC   <thead>
# MAGIC     <tr style="background: #1B5162; color: white;">
# MAGIC       <th style="padding: 10px 14px; text-align: center; border: 1px solid #EEEDE9; width: 50px;">#</th>
# MAGIC       <th style="padding: 10px 14px; text-align: center; border: 1px solid #EEEDE9; width: 80px;">Type</th>
# MAGIC       <th style="padding: 10px 14px; text-align: left; border: 1px solid #EEEDE9;">Module Name</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr style="background: white;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">1</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center;"><span style="background: #fff3e0; color: #e65100; padding: 2px 8px; border-radius: 4px; font-weight: 600;">Lecture</span></td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Unity AI Gateway Basics</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: #F9F7F4;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">2</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center;"><span style="background: #e3f2fd; color: #1976d2; padding: 2px 8px; border-radius: 4px; font-weight: 600;">Demo</span></td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Unity AI Gateway for Agent Applications</td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC
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