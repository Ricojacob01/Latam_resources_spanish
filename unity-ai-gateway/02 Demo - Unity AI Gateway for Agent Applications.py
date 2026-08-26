# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ![DB Academy Logo](https://files.training.databricks.com/binder/prod_main/unity-ai-gateway-en_us-1.0.0/images/20260821T112203Z/Includes/images/databricks_academy_logo.png)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Demo - Unity AI Gateway for Agent Applications
# MAGIC ## Overview
# MAGIC In this demo, you will configure Unity AI Gateway (UAIG) to govern an agentic application that generates Databricks-native diagrams as code. You will walk through the full lifecycle of setting up a model service, starting from initial creation and permissions to production-grade governance with traffic splitting, rate limiting, hallucination guardrails, and fallback routing. Along the way, you will use inference tables and system catalog telemetry to observe how requests flow through the gateway.
# MAGIC ## Learning Objectives
# MAGIC By the end of this demo, you will be able to:
# MAGIC 1. Create and configure a model service in Unity AI Gateway for an agentic application
# MAGIC 1. Grant execution permissions on model services to application service principals
# MAGIC 1. Set up inference tables for request/response logging and observability
# MAGIC 1. Configure rate limits to control token and request consumption
# MAGIC 1. Apply output guardrails (hallucination policy) to govern agent responses
# MAGIC 1. Configure traffic splitting and fallback routing across multiple models
# MAGIC 1. Query system catalog tables (`system.ai`, `system.ai_gateway`) for usage telemetry
# MAGIC
# MAGIC <div style="border-left: 4px solid #f44336; background: #ffebee; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC <div>
# MAGIC <strong style="color: #c62828; font-size: 1.1em;">Beta Features</strong>
# MAGIC <p style="margin: 8px 0 0 0; color: #333;">This notebook uses Databricks Beta Features. While this notebook has been thoroughly tested, it's worth noting that full functionality is not guaranteed. Unity AI Gateway and model services are Generally Available, but the following features used in this training are still in Beta and must be enabled by an account admin under <strong>Previews</strong>:</p>
# MAGIC <li> <strong>Service policies</strong>
# MAGIC </li>
# MAGIC <li> <strong>Managed MLflow Prompt Registry</strong>
# MAGIC </li>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Classroom Setup
# MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC   <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC     <div>
# MAGIC       <strong style="color: #0d47a1; font-size: 1.1em;">Note</strong>
# MAGIC       <p style="margin: 8px 0 0 0; color: #333;">
# MAGIC         If you have restricted access to creating a catalog or schema, you can override either or both using widget parameters after the path in the next cell. For example, 
# MAGIC       </p>
# MAGIC       <p>
# MAGIC       <code>%run ./Includes/Classroom-Setup-1 $catalog_override = "serverless_stable_cskrx6_catalog" $schema_override = "my_schema"</code>
# MAGIC       </p>
# MAGIC       <p>
# MAGIC       Running this notebook outside Voareum assumes you have typical sandbox permissions on your catalog (you can create a schema, etc.). The following setup script (which is idempotent) will take a few minutes to complete, as it is deploying the application and checking the deployment status. <strong>This setup takes ~3-4 minutes.</strong>
# MAGIC       </p>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-1 $catalog_override = "academy_rico" $schema_override = "ts_ai_gateway"

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. Scenario
# MAGIC
# MAGIC <div style="background: #1B5162; color: white; padding: 20px 24px; border-radius: 10px; margin: 12px 0;">
# MAGIC   <div style="font-size: 11pt; text-transform: uppercase; letter-spacing: 1px; opacity: 0.7; margin-bottom: 6px;">Scenario</div>
# MAGIC   <div style="font-size: 14pt; line-height: 1.5; margin-bottom: 16px;">
# MAGIC     The App Team has asked you to setup Unity AI Gateway for an agent you created as a part of a POC. Your task is to successfully route the agentic application through Unity AI Gateway, grant proper permissions to additional model services, setup rate limit, grant policies, and configuring fallback routing. The application builds Databricks-native diagrams as code for documentation in Databricks notebooks with visibility to the agent. 
# MAGIC   </div>
# MAGIC
# MAGIC <script>
# MAGIC function showTab(name) {
# MAGIC   ['supervisor', 'analyst', 'business'].forEach(function(t) {
# MAGIC     var tab = document.getElementById('tab-' + t);
# MAGIC     var btn = document.getElementById('btn-' + t);
# MAGIC     if (t === name) {
# MAGIC       tab.style.display = 'block';
# MAGIC       btn.style.background = 'rgb(240,243,248)';
# MAGIC       btn.style.color = '#1B5162';
# MAGIC     } else {
# MAGIC       tab.style.display = 'none';
# MAGIC       btn.style.background = '#1B5162';
# MAGIC       btn.style.color = 'rgb(240,243,248)';
# MAGIC     }
# MAGIC   });
# MAGIC }
# MAGIC </script>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. Use Genie Code to Investigate the Application
# MAGIC As a part of the on-boarding process to the application, you can use Genie Code to help you get up to speed. Below is a sample prompt to get you started with understanding what the application does. Take a moment to locate the agent's code and application code to help understand where things are located in the folder `agent-app`.
# MAGIC <div style="width: 100%; font-family: sans-serif;">
# MAGIC <div style="background: #F9F7F4; border-radius: 10px; padding: 24px 28px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); border-top: 6px solid #FF5F46;">
# MAGIC   <div style="margin-bottom: 10px;"><svg viewBox="0 0 806 200" fill="none" xmlns="http://www.w3.org/2000/svg" style="height:64px; width:auto; display:block;"><path d="M81.7909 159.789C75.0244 159.789 69.5392 166.599 69.5392 175H126.368C126.368 166.599 120.883 159.789 114.116 159.789H81.7909Z" fill="#FF5F46"/>
# MAGIC <path d="M179.614 80.7871C167.525 100.208 149.376 126.933 141.272 133.79C133.167 140.647 121.149 153.009 93.2907 153.009C73.7741 153.009 56.9864 140.474 49.5352 122.498C49.5353 122.519 49.5356 122.54 49.5358 122.561C48.4799 119.938 45.9132 118.085 42.9121 118.085C38.969 118.085 35.7721 121.282 35.7721 125.225C35.7722 126.15 35.9484 126.997 36.2689 127.752C37.984 131.793 42.5469 131.965 42.5469 144.569C42.5469 143.103 42.5469 146.404 42.5469 144.569C30.6947 144.569 21.0865 134.96 21.0863 123.108C21.0863 111.256 30.6945 101.647 42.5469 101.647H45.4249V101.644H79.5153C82.062 101.644 84.5067 102.722 86.0978 104.71C88.8856 108.194 91.0222 111.438 92.5969 114.114C94.056 116.594 98.4255 116.618 99.8699 114.13C101.393 111.506 103.416 108.337 105.98 104.929C107.573 102.813 110.103 101.644 112.752 101.644H125.427C143.951 101.644 154.105 92.7623 159.289 86.3612C161.972 83.049 165.782 80.7871 170.045 80.7871H179.614Z" fill="#FABFBA"/>
# MAGIC <path d="M96.3843 111.768C96.3842 87.8629 77.3194 68.4739 53.768 68.3843C77.3196 68.2949 96.3845 48.9049 96.3845 25C96.3845 25 96.3845 25 96.3845 25C96.3845 48.9602 115.538 68.3838 139.165 68.3838C115.538 68.3838 96.3844 87.8075 96.3843 111.768Z" fill="#FF5F46"/>
# MAGIC <path d="M274.824 139.867C267.307 139.867 260.701 138.158 255.006 134.742C249.388 131.325 245.022 126.579 241.909 120.505C238.796 114.355 237.239 107.18 237.239 98.9793C237.239 90.855 238.834 83.6798 242.023 77.4537C245.212 71.1516 249.767 66.2543 255.69 62.7615C261.688 59.1929 268.787 57.4086 276.988 57.4086C286.251 57.4086 293.92 59.6485 299.994 64.1283C306.144 68.5321 310.13 74.6823 311.952 82.5788H296.577C295.438 78.6306 293.198 75.5555 289.857 73.3535C286.516 71.1516 282.189 70.0507 276.874 70.0507C271.483 70.0507 266.851 71.2276 262.979 73.5813C259.182 75.8592 256.259 79.2 254.209 83.6039C252.235 87.9318 251.248 93.0949 251.248 99.0932C251.248 105.092 252.235 110.255 254.209 114.583C256.259 118.911 259.144 122.213 262.865 124.491C266.585 126.693 270.951 127.794 275.962 127.794C283.403 127.794 289.022 125.706 292.819 121.53C296.615 117.354 298.855 111.925 299.538 105.243H278.924V94.8792H313.319V138.5H300.791L299.652 127.566C297.906 130.148 295.894 132.388 293.616 134.286C291.338 136.108 288.642 137.513 285.529 138.5C282.492 139.411 278.924 139.867 274.824 139.867ZM351.07 139.867C345.375 139.867 340.364 138.652 336.036 136.222C331.708 133.717 328.33 130.224 325.9 125.744C323.47 121.264 322.255 116.101 322.255 110.255C322.255 104.18 323.432 98.8654 325.786 94.3097C328.216 89.7541 331.594 86.1854 335.922 83.6039C340.326 81.0223 345.413 79.7315 351.184 79.7315C356.803 79.7315 361.7 80.9843 365.876 83.49C370.052 85.9956 373.279 89.3744 375.557 93.6264C377.835 97.8024 378.974 102.51 378.974 107.749C378.974 108.508 378.974 109.344 378.974 110.255C378.974 111.166 378.898 112.115 378.746 113.102H332.05V104.332H365.193C365.041 100.156 363.636 96.8913 360.979 94.5375C358.321 92.1078 355.018 90.893 351.07 90.893C348.261 90.893 345.679 91.5384 343.325 92.8291C340.972 94.1199 339.111 96.0561 337.745 98.6377C336.378 101.143 335.695 104.332 335.695 108.205V111.507C335.695 115.076 336.34 118.151 337.631 120.733C338.997 123.314 340.82 125.288 343.098 126.655C345.451 127.946 348.071 128.591 350.956 128.591C354.145 128.591 356.765 127.908 358.815 126.541C360.941 125.175 362.497 123.352 363.484 121.074H377.379C376.316 124.643 374.57 127.87 372.14 130.755C369.71 133.565 366.711 135.805 363.143 137.475C359.574 139.069 355.55 139.867 351.07 139.867ZM388.176 138.5V81.0982H400.248L401.273 90.6652C403.02 87.3243 405.525 84.6669 408.79 82.6927C412.055 80.7186 415.928 79.7315 420.407 79.7315C425.039 79.7315 428.987 80.7186 432.252 82.6927C435.517 84.5909 438.023 87.4003 439.769 91.1208C441.591 94.8412 442.503 99.4729 442.503 105.016V138.5H428.835V106.268C428.835 101.485 427.772 97.8024 425.646 95.2209C423.52 92.6393 420.369 91.3485 416.193 91.3485C413.46 91.3485 410.992 91.9939 408.79 93.2847C406.664 94.5755 404.956 96.4737 403.665 98.9793C402.45 101.409 401.843 104.37 401.843 107.863V138.5H388.176ZM454.064 138.5V81.0982H467.731V138.5H454.064ZM460.898 71.9868C458.392 71.9868 456.304 71.2275 454.634 69.709C453.039 68.1145 452.242 66.1783 452.242 63.9005C452.242 61.5467 453.039 59.6485 454.634 58.2059C456.304 56.6873 458.392 55.928 460.898 55.928C463.404 55.928 465.454 56.6873 467.048 58.2059C468.719 59.6485 469.554 61.5467 469.554 63.9005C469.554 66.1783 468.719 68.1145 467.048 69.709C465.454 71.2275 463.404 71.9868 460.898 71.9868ZM506.987 139.867C501.292 139.867 496.281 138.652 491.953 136.222C487.625 133.717 484.246 130.224 481.817 125.744C479.387 121.264 478.172 116.101 478.172 110.255C478.172 104.18 479.349 98.8654 481.703 94.3097C484.133 89.7541 487.511 86.1854 491.839 83.6039C496.243 81.0223 501.33 79.7315 507.101 79.7315C512.72 79.7315 517.617 80.9843 521.793 83.49C525.969 85.9956 529.196 89.3744 531.474 93.6264C533.752 97.8024 534.891 102.51 534.891 107.749C534.891 108.508 534.891 109.344 534.891 110.255C534.891 111.166 534.815 112.115 534.663 113.102H487.967V104.332H521.11C520.958 100.156 519.553 96.8913 516.896 94.5375C514.238 92.1078 510.935 90.893 506.987 90.893C504.178 90.893 501.596 91.5384 499.242 92.8291C496.889 94.1199 495.028 96.0561 493.662 98.6377C492.295 101.143 491.612 104.332 491.612 108.205V111.507C491.612 115.076 492.257 118.151 493.548 120.733C494.914 123.314 496.737 125.288 499.015 126.655C501.368 127.946 503.988 128.591 506.873 128.591C510.062 128.591 512.682 127.908 514.732 126.541C516.858 125.175 518.414 123.352 519.401 121.074H533.296C532.233 124.643 530.487 127.87 528.057 130.755C525.627 133.565 522.628 135.805 519.06 137.475C515.491 139.069 511.467 139.867 506.987 139.867Z" fill="#0B2026"/>
# MAGIC <rect x="563.157" y="58.4648" width="242.843" height="84.9055" rx="42.4527" fill="#0B2026"/>
# MAGIC <path d="M664.499 114.947C661.815 114.947 659.493 114.366 657.532 113.205C655.596 112.018 654.1 110.38 653.042 108.289C652.01 106.173 651.493 103.735 651.493 100.974C651.493 98.187 652.01 95.7485 653.042 93.6584C654.1 91.5424 655.596 89.8909 657.532 88.7039C659.493 87.517 661.815 86.9235 664.499 86.9235C667.724 86.9235 670.356 87.7234 672.395 89.3232C674.433 90.8973 675.724 93.1294 676.266 96.0195H671.156C670.795 94.497 670.06 93.2971 668.95 92.4198C667.84 91.5424 666.344 91.1037 664.46 91.1037C662.757 91.1037 661.286 91.5037 660.048 92.3036C658.835 93.0778 657.893 94.2132 657.222 95.7098C656.577 97.1807 656.254 98.9353 656.254 100.974C656.254 103.012 656.577 104.767 657.222 106.238C657.893 107.683 658.835 108.805 660.048 109.605C661.286 110.38 662.757 110.767 664.46 110.767C666.344 110.767 667.84 110.367 668.95 109.567C670.06 108.741 670.795 107.606 671.156 106.161H676.266C675.749 108.896 674.459 111.05 672.395 112.625C670.356 114.173 667.724 114.947 664.499 114.947ZM697.803 114.947C695.196 114.947 692.887 114.366 690.874 113.205C688.887 112.018 687.326 110.38 686.191 108.289C685.055 106.173 684.488 103.722 684.488 100.935C684.488 98.1741 685.055 95.7485 686.191 93.6584C687.326 91.5424 688.887 89.8909 690.874 88.7039C692.887 87.517 695.196 86.9235 697.803 86.9235C700.46 86.9235 702.796 87.517 704.808 88.7039C706.821 89.8909 708.382 91.5424 709.492 93.6584C710.602 95.7485 711.156 98.1741 711.156 100.935C711.156 103.722 710.602 106.173 709.492 108.289C708.382 110.38 706.821 112.018 704.808 113.205C702.796 114.366 700.46 114.947 697.803 114.947ZM697.841 110.767C699.57 110.767 701.08 110.38 702.37 109.605C703.66 108.805 704.654 107.67 705.35 106.199C706.047 104.728 706.395 102.974 706.395 100.935C706.395 98.8966 706.047 97.1419 705.35 95.6711C704.654 94.2003 703.66 93.0778 702.37 92.3036C701.08 91.5037 699.57 91.1037 697.841 91.1037C696.112 91.1037 694.603 91.5037 693.313 92.3036C692.022 93.0778 691.016 94.2003 690.294 95.6711C689.597 97.1419 689.249 98.8966 689.249 100.935C689.249 102.974 689.597 104.728 690.294 106.199C691.016 107.67 692.022 108.805 693.313 109.605C694.603 110.38 696.112 110.767 697.841 110.767ZM720.339 114.482V87.3879H729.087C732.235 87.3879 734.829 87.9427 736.867 89.0523C738.931 90.1619 740.454 91.7359 741.434 93.7745C742.441 95.7872 742.944 98.187 742.944 100.974C742.944 103.709 742.441 106.096 741.434 108.135C740.454 110.147 738.931 111.708 736.867 112.818C734.829 113.928 732.235 114.482 729.087 114.482H720.339ZM724.984 110.534H728.855C731.203 110.534 733.048 110.16 734.39 109.412C735.757 108.638 736.725 107.541 737.293 106.122C737.886 104.677 738.183 102.961 738.183 100.974C738.183 98.9611 737.886 97.2452 737.293 95.8259C736.725 94.3809 735.757 93.2713 734.39 92.4972C733.048 91.6972 731.203 91.2973 728.855 91.2973H724.984V110.534ZM752.124 114.482V87.3879H769.619V91.1425H756.769V98.9224H768.458V102.6H756.769V110.728H769.619V114.482H752.124Z" fill="white"/>
# MAGIC <path d="M636.472 101.239L620.99 116.72L613.554 109.283L621.598 101.239L613.554 93.1943L620.99 85.7578L636.472 101.239Z" fill="#FF3621"/>
# MAGIC <path d="M584.879 101.241L600.36 85.7598L607.797 93.1968L599.753 101.241L607.797 109.286L600.36 116.723L584.879 101.241Z" fill="#FF3621"/></svg></div>
# MAGIC   <div style="font-size: 15pt; color: #0b2026; line-height: 1.7; margin-bottom: 16px;">Investigate your application. Click <strong>Copy</strong> below and paste the prompt into Genie Code.</div>
# MAGIC   <div style="display: flex; align-items: center; gap: 10px; background: #fff; border: 1px solid #EEEDE9; border-radius: 6px; padding: 10px 14px; font-size: 14pt; font-family: monospace; color: #0b2026;">
# MAGIC     <span id="genie-query" style="flex: 1;">I'm onboarding to this agent application located in the folder agent-app. Help me understand the purpose of the application.</span>
# MAGIC     <button onclick="var text=document.getElementById('genie-query').innerText; var ta=document.createElement('textarea'); ta.value=text; ta.style.position='fixed'; ta.style.opacity='0'; document.body.appendChild(ta); ta.select(); document.execCommand('copy'); document.body.removeChild(ta); var b=this; b.innerText='Copied!'; setTimeout(function(){ b.innerText='Copy'; }, 2000);" style="background: #FF5F46; color: white; border: none; border-radius: 4px; padding: 6px 12px; font-size: 14pt; cursor: pointer; white-space: nowrap;">Copy</button>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Add a Test Model Service
# MAGIC Now that we understand our use case and the various files, we need to add a model service from UAIG.
# MAGIC
# MAGIC 1. Navigate to **AI/ML → AI Gateway → + Model**
# MAGIC 1. Choose your **Catalog** and **Schema** (see the output from cell 3)
# MAGIC 1. Type **claude-opus-5** for the model service name
# MAGIC 1. Leave the **Provider** as **Databricks hosted**
# MAGIC 1. For **Destination**, select **Claude Opus 5**
# MAGIC 1. Click **Create**

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Update permissions
# MAGIC Now that we have our model service created, let's update permissions. 
# MAGIC 1. Navigate to **Catalog** using the menu to the left and navigate to **Permissions** under your schema **ts_ai_gateway** in your lab catalog.
# MAGIC 1. Click on **claude-opus-5** under **Services**
# MAGIC 1. Under **Permissions** add **EXECUTE** permissions to your application. Recall the name of your application is printed in the workspace setup script above. 
# MAGIC 1. Reload the app in your browser and see that **claude-opus-5** is now shown under **Models**. 
# MAGIC
# MAGIC Alternatively, you can click the **Permissions** tab in **AI Gateway** and setup configuration as described above.

# COMMAND ----------

# MAGIC %md
# MAGIC ### E1. View a Summary of the Agent
# MAGIC Navigate to the application (see output from cell 3 for a direct link). In the application you will see a button at the top left that says **My Agent**. This contains a summary of your agent for this specific training (e.g. where the model service is located, details, etc).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## F. Test the Agent
# MAGIC
# MAGIC <div style="display: flex; align-items: flex-start; gap: 24px; flex-wrap: wrap;">
# MAGIC   <div style="flex: 1; min-width: 280px;">
# MAGIC     <p>Try the following questions and follow up with additional cleaning prompts (e.g. "some of the containers are overlapping, please fix", etc.):</p>
# MAGIC <div class="code-card">
# MAGIC       <div class="code-card-header">
# MAGIC         <span class="code-card-lang">Prompt</span>
# MAGIC         <button class="code-card-copy" onclick="(function(b){var c=b.closest('.code-card').querySelector('.code-card-body').textContent,t=document.createElement('textarea');t.value=c;t.style.position='fixed';t.style.opacity='0';document.body.appendChild(t);t.select();var ok=false;try{ok=document.execCommand('copy')}catch(e){}document.body.removeChild(t);if(ok){var l=b.querySelector('.copy-label'),ci=b.querySelector('.copy-icon'),chi=b.querySelector('.check-icon');ci.style.display='none';chi.style.display='';l.textContent='Copied!';b.classList.add('copied');setTimeout(function(){ci.style.display='';chi.style.display='none';l.textContent='Copy';b.classList.remove('copied')},2000)}})(this)">
# MAGIC           <svg class="copy-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg>
# MAGIC           <svg class="check-icon" style="display:none" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg>
# MAGIC           <span class="copy-label">Copy</span>
# MAGIC         </button>
# MAGIC       </div>
# MAGIC       <div class="code-card-body">Build a diagram explaining the ReLu activation function.</div>
# MAGIC     </div>
# MAGIC     <div class="code-card">
# MAGIC       <div class="code-card-header">
# MAGIC         <span class="code-card-lang">Prompt</span>
# MAGIC         <button class="code-card-copy" onclick="(function(b){var c=b.closest('.code-card').querySelector('.code-card-body').textContent,t=document.createElement('textarea');t.value=c;t.style.position='fixed';t.style.opacity='0';document.body.appendChild(t);t.select();var ok=false;try{ok=document.execCommand('copy')}catch(e){}document.body.removeChild(t);if(ok){var l=b.querySelector('.copy-label'),ci=b.querySelector('.copy-icon'),chi=b.querySelector('.check-icon');ci.style.display='none';chi.style.display='';l.textContent='Copied!';b.classList.add('copied');setTimeout(function(){ci.style.display='';chi.style.display='none';l.textContent='Copy';b.classList.remove('copied')},2000)}})(this)">
# MAGIC           <svg class="copy-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg>
# MAGIC           <svg class="check-icon" style="display:none" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg>
# MAGIC           <span class="copy-label">Copy</span>
# MAGIC         </button>
# MAGIC       </div>
# MAGIC       <div class="code-card-body">Make a diagram showing the difference between perceptron vs. multi-layer perceptron.</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div style="flex: 1; min-width: 280px; text-align: center;">
# MAGIC     <img src="https://files.training.databricks.com/binder/prod_main/unity-ai-gateway-en_us-1.0.0/images/20260821T112203Z/Includes/images/diagram-as-code-relu.png" alt="diagram-as-code-relu.png" title="diagram-as-code-relu.png" style="max-width: 100%; border-radius: 8px;" />
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <style>
# MAGIC   .code-card { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; border: 1px solid #DCE0E2; border-radius: 0.5rem; overflow: hidden; background: #F9F7F4; max-width: 100%; margin: 0.5rem 0; }
# MAGIC   .code-card-header { display: flex; align-items: center; justify-content: space-between; padding: 0.4rem 0.75rem; background: #EEEDE9; border-bottom: 1px solid #DCE0E2; }
# MAGIC   .code-card-lang { font-size: 0.7rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.1em; color: #5A6F77; }
# MAGIC   .code-card-copy { display: inline-flex; align-items: center; gap: 0.3rem; background: none; border: none; color: #5A6F77; cursor: pointer; padding: 0.2rem 0.4rem; border-radius: 0.25rem; font-size: 0.7rem; font-weight: 500; transition: color 0.15s, background 0.15s; }
# MAGIC   .code-card-copy:hover { color: #1B3139; background: rgba(0,0,0,0.05); }
# MAGIC   .code-card-copy.copied { color: #2272B4; }
# MAGIC   .code-card-copy svg { width: 14px; height: 14px; }
# MAGIC   .code-card-body { margin: 0; padding: 1rem; overflow-x: auto; font-family: 'SF Mono', 'Cascadia Code', 'Fira Code', Consolas, monospace; font-size: 0.82rem; line-height: 1.55; color: #1B3139; white-space: pre-wrap; word-break: break-word; }
# MAGIC </style>

# COMMAND ----------

# MAGIC %md
# MAGIC ### F1. Agent in the App
# MAGIC This application contains an agent visibility feature for demonstration purposes. Clicking on **My Agent** in the UI will display information such as where your model service is located, routing information, system prompt, etc. along with various links to take you to those locations within the UI.

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. Update the Agent's Governance Strategy
# MAGIC Now that we have a better understanding of the agent's output quality, let's be more rigorous with our model service setup for the application. First, we need to delete our test model service **claude-opus-5**. 
# MAGIC 1. Navigate to **AI Gateway** and locate your model service called **claude-opus-5** registered under the schema **ts_ai_gateway**. 
# MAGIC 1. Click on the name and click on the three vertical dots at the top right and click **Delete**.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### G1. Add a New Model Service
# MAGIC Next, we will add a model service that has more structure like traffic splitting and fallback routing. 
# MAGIC 1. Navigate to **AI Gateway** and add a new model to your schema **ts_ai_gateway** called **ts-demo-ms**. You can copy the name using the copy button below. 
# MAGIC
# MAGIC
# MAGIC <div class="code-card">
# MAGIC       <div class="code-card-header">
# MAGIC         <span class="code-card-lang">Text</span>
# MAGIC         <button class="code-card-copy" onclick="(function(b){var c=b.closest('.code-card').querySelector('.code-card-body').textContent,t=document.createElement('textarea');t.value=c;t.style.position='fixed';t.style.opacity='0';document.body.appendChild(t);t.select();var ok=false;try{ok=document.execCommand('copy')}catch(e){}document.body.removeChild(t);if(ok){var l=b.querySelector('.copy-label'),ci=b.querySelector('.copy-icon'),chi=b.querySelector('.check-icon');ci.style.display='none';chi.style.display='';l.textContent='Copied!';b.classList.add('copied');setTimeout(function(){ci.style.display='';chi.style.display='none';l.textContent='Copy';b.classList.remove('copied')},2000)}})(this)">
# MAGIC           <svg class="copy-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg>
# MAGIC           <svg class="check-icon" style="display:none" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg>
# MAGIC           <span class="copy-label">Copy</span>
# MAGIC         </button>
# MAGIC       </div>
# MAGIC       <div class="code-card-body">ts-demo-ms</div>
# MAGIC     </div>
# MAGIC
# MAGIC 2. Let's have our primary routing be for Claude models and our fallback be GPT models. Select **Opus 5** and click **Create**. 
# MAGIC 3. Add permissions for the app like before. 
# MAGIC
# MAGIC
# MAGIC <style>
# MAGIC   .code-card { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; border: 1px solid #DCE0E2; border-radius: 0.5rem; overflow: hidden; background: #F9F7F4; max-width: 100%; margin: 0.5rem 0; }
# MAGIC   .code-card-header { display: flex; align-items: center; justify-content: space-between; padding: 0.4rem 0.75rem; background: #EEEDE9; border-bottom: 1px solid #DCE0E2; }
# MAGIC   .code-card-lang { font-size: 0.7rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.1em; color: #5A6F77; }
# MAGIC   .code-card-copy { display: inline-flex; align-items: center; gap: 0.3rem; background: none; border: none; color: #5A6F77; cursor: pointer; padding: 0.2rem 0.4rem; border-radius: 0.25rem; font-size: 0.7rem; font-weight: 500; transition: color 0.15s, background 0.15s; }
# MAGIC   .code-card-copy:hover { color: #1B3139; background: rgba(0,0,0,0.05); }
# MAGIC   .code-card-copy.copied { color: #2272B4; }
# MAGIC   .code-card-copy svg { width: 14px; height: 14px; }
# MAGIC   .code-card-body { margin: 0; padding: 1rem; overflow-x: auto; font-family: 'SF Mono', 'Cascadia Code', 'Fira Code', Consolas, monospace; font-size: 0.82rem; line-height: 1.55; color: #1B3139; white-space: pre-wrap; word-break: break-word; }
# MAGIC </style>
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### G2. Add an Inference Table
# MAGIC With agentic applications, we want to make sure we have proper logging with our model service.
# MAGIC 1. While still in the **Overview** tab in **AI Gateway** click on **Set up** next to **Inference table**. 
# MAGIC     - This inference table will be empty until we send a query. 
# MAGIC 1. You will point the inference table location to your schema **ts_ai_gateway** and click **Save**. 
# MAGIC     - You can leave the table name prefix the **ts-demo-ms**. 
# MAGIC
# MAGIC We will query this inference table later after hitting the endpoint a few times in the Playground.

# COMMAND ----------

# MAGIC %md
# MAGIC ### G3. Add a Rate Limit
# MAGIC Rate limits will help us control spending across all our models.
# MAGIC 1. In the same **Overview** landing page for the new model service **ts-demo-ms**, click **Set up**
# MAGIC 1. Add 2 request limits per minute and set the total tokens per minute to 15,000.
# MAGIC 1. Click **Save**
# MAGIC
# MAGIC > **Rate limits vs. budgets:** rate limits cap the *velocity* of traffic (requests and tokens per minute), while **budgets** cap monthly *spend* per user and can block further requests once a threshold is hit. The two are complementary controls. See [Manage budgets for Unity AI Gateway](https://docs.databricks.com/aws/en/ai-gateway/budgets).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### G4. Add a New Policy
# MAGIC Setting a policy will add governance to our new model service. We can inspect requests and responses with automatic action taken on content that violates them. For our example use case, we don't want our agent to hallucinate and build incorrect diagrams.
# MAGIC - In practice, we would have connect our agent to multiple knowledge sources, but this demonstration is scoped for only showing fundamentals of UAIG. Instead, we will rely on the model's training data for source of truth. 
# MAGIC 1. Enter the name given in the copy box below. 
# MAGIC <div class="code-card">
# MAGIC       <div class="code-card-header">
# MAGIC         <span class="code-card-lang">Text</span>
# MAGIC         <button class="code-card-copy" onclick="(function(b){var c=b.closest('.code-card').querySelector('.code-card-body').textContent,t=document.createElement('textarea');t.value=c;t.style.position='fixed';t.style.opacity='0';document.body.appendChild(t);t.select();var ok=false;try{ok=document.execCommand('copy')}catch(e){}document.body.removeChild(t);if(ok){var l=b.querySelector('.copy-label'),ci=b.querySelector('.copy-icon'),chi=b.querySelector('.check-icon');ci.style.display='none';chi.style.display='';l.textContent='Copied!';b.classList.add('copied');setTimeout(function(){ci.style.display='';chi.style.display='none';l.textContent='Copy';b.classList.remove('copied')},2000)}})(this)">
# MAGIC           <svg class="copy-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg>
# MAGIC           <svg class="check-icon" style="display:none" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg>
# MAGIC           <span class="copy-label">Copy</span>
# MAGIC         </button>
# MAGIC       </div>
# MAGIC       <div class="code-card-body">Diagram-As-Code</div>
# MAGIC     </div>
# MAGIC 1. Skip **Principals and scope** (these are not yet configurable with UAIG)
# MAGIC 1. Under **Guardrail**, select the type as **Hallucination**.
# MAGIC 1. The default rank will be set to 1 since we'll only configure this one guardrail. 
# MAGIC 1. The default for **Phase** is **Output guardrails** for this **Guardrail type**. 
# MAGIC 1. You can view the **Prompt** being used and change the **Evaluator model service** under **Advanced options** to a different model if you would like (this is optional)
# MAGIC     - We will leave the **Mode** set to either **Enforce** or **Log**. **Enforce** will block or allow the response based on the policy while **Log** will observe what would have happened without blocking the response.
# MAGIC 1. Click **Create policy**. 
# MAGIC
# MAGIC <style>
# MAGIC   .code-card { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; border: 1px solid #DCE0E2; border-radius: 0.5rem; overflow: hidden; background: #F9F7F4; max-width: 100%; margin: 0.5rem 0; }
# MAGIC   .code-card-header { display: flex; align-items: center; justify-content: space-between; padding: 0.4rem 0.75rem; background: #EEEDE9; border-bottom: 1px solid #DCE0E2; }
# MAGIC   .code-card-lang { font-size: 0.7rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.1em; color: #5A6F77; }
# MAGIC   .code-card-copy { display: inline-flex; align-items: center; gap: 0.3rem; background: none; border: none; color: #5A6F77; cursor: pointer; padding: 0.2rem 0.4rem; border-radius: 0.25rem; font-size: 0.7rem; font-weight: 500; transition: color 0.15s, background 0.15s; }
# MAGIC   .code-card-copy:hover { color: #1B3139; background: rgba(0,0,0,0.05); }
# MAGIC   .code-card-copy.copied { color: #2272B4; }
# MAGIC   .code-card-copy svg { width: 14px; height: 14px; }
# MAGIC   .code-card-body { margin: 0; padding: 1rem; overflow-x: auto; font-family: 'SF Mono', 'Cascadia Code', 'Fira Code', Consolas, monospace; font-size: 0.82rem; line-height: 1.55; color: #1B3139; white-space: pre-wrap; word-break: break-word; }
# MAGIC </style>
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### G5. Configure and Test Traffic Split
# MAGIC We'll update our primary route to include a 50-50 split in routing between **Opus 5** and **Opus 4.8**.
# MAGIC 1. Navigate to the **Routing** tab and under the **Primary** group, select **+ Add another model**. 
# MAGIC 1. Choose **Opus 4.8** and click **Add**. 
# MAGIC 1. The traffic will show a 50-50 split as 50% next to **system.ai.databricks-claude-opus-5** and 50% next to **system.ai.databricks-claude-opus-4-8**.
# MAGIC
# MAGIC <div style="border-left: 4px solid #f44336; background: #ffebee; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC <div>
# MAGIC <strong style="color: #c62828; font-size: 1.1em;">Warning</strong>
# MAGIC <p style="margin: 8px 0 0 0; color: #333;">Currently, token usage visibility is surfaced for single model services in <strong>AI Gateway</strong>. However, you can view this information in the inference table using the query below.</p>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### G6. Test Traffic Split in Playground
# MAGIC To make sure routing is being properly handled by UAIG, we can test in the Playground.
# MAGIC 1. While still in the **Overview** tab of your model service, click on **Chat in playground**
# MAGIC 1. Click the **⊕** symbol and attach the playground to the model service you created
# MAGIC     - This will create 2 instances of our model service so we can populate our inference table with multiple payloads. 
# MAGIC 1. Copy and paste the following prompts and run next cell. 
# MAGIC
# MAGIC <div class="code-card">
# MAGIC       <div class="code-card-header"><span class="code-card-lang">Prompt</span><button class="code-card-copy" onclick="(function(b){var c=b.closest('.code-card').querySelector('.code-card-body').textContent,t=document.createElement('textarea');t.value=c;t.style.position='fixed';t.style.opacity='0';document.body.appendChild(t);t.select();var ok=false;try{ok=document.execCommand('copy')}catch(e){}document.body.removeChild(t);if(ok){var l=b.querySelector('.copy-label'),ci=b.querySelector('.copy-icon'),chi=b.querySelector('.check-icon');ci.style.display='none';chi.style.display='';l.textContent='Copied!';b.classList.add('copied');setTimeout(function(){ci.style.display='';chi.style.display='none';l.textContent='Copy';b.classList.remove('copied')},2000)}})(this)"><svg class="copy-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg><svg class="check-icon" style="display:none" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg><span class="copy-label">Copy</span></button></div>
# MAGIC       <div class="code-card-body">What is Apache Spark?</div>
# MAGIC     </div>
# MAGIC <div class="code-card">
# MAGIC       <div class="code-card-header"><span class="code-card-lang">Prompt</span><button class="code-card-copy" onclick="(function(b){var c=b.closest('.code-card').querySelector('.code-card-body').textContent,t=document.createElement('textarea');t.value=c;t.style.position='fixed';t.style.opacity='0';document.body.appendChild(t);t.select();var ok=false;try{ok=document.execCommand('copy')}catch(e){}document.body.removeChild(t);if(ok){var l=b.querySelector('.copy-label'),ci=b.querySelector('.copy-icon'),chi=b.querySelector('.check-icon');ci.style.display='none';chi.style.display='';l.textContent='Copied!';b.classList.add('copied');setTimeout(function(){ci.style.display='';chi.style.display='none';l.textContent='Copy';b.classList.remove('copied')},2000)}})(this)"><svg class="copy-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg><svg class="check-icon" style="display:none" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg><span class="copy-label">Copy</span></button></div>
# MAGIC       <div class="code-card-body">What's the difference between Delta and Iceberg?</div>
# MAGIC     </div>
# MAGIC <div class="code-card">
# MAGIC       <div class="code-card-header"><span class="code-card-lang">Prompt</span><button class="code-card-copy" onclick="(function(b){var c=b.closest('.code-card').querySelector('.code-card-body').textContent,t=document.createElement('textarea');t.value=c;t.style.position='fixed';t.style.opacity='0';document.body.appendChild(t);t.select();var ok=false;try{ok=document.execCommand('copy')}catch(e){}document.body.removeChild(t);if(ok){var l=b.querySelector('.copy-label'),ci=b.querySelector('.copy-icon'),chi=b.querySelector('.check-icon');ci.style.display='none';chi.style.display='';l.textContent='Copied!';b.classList.add('copied');setTimeout(function(){ci.style.display='';chi.style.display='none';l.textContent='Copy';b.classList.remove('copied')},2000)}})(this)"><svg class="copy-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg><svg class="check-icon" style="display:none" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg><span class="copy-label">Copy</span></button></div>
# MAGIC       <div class="code-card-body">What is the difference between OLTP and OLAP?</div>
# MAGIC     </div>
# MAGIC
# MAGIC 4. Next, navigate to the catalog explorer on the left by clicking **Catalog**
# MAGIC 5. Search for the schema **ts-demo-ms_payload**
# MAGIC 6. Click on the schema and click on the **Sample Data** tab. Here, you can use natural language with the Sample Data Explorer to get quick insights. Copy and paste the prompts below to query the inference table. 
# MAGIC
# MAGIC <div class="code-card">
# MAGIC       <div class="code-card-header"><span class="code-card-lang">Prompt</span><button class="code-card-copy" onclick="(function(b){var c=b.closest('.code-card').querySelector('.code-card-body').textContent,t=document.createElement('textarea');t.value=c;t.style.position='fixed';t.style.opacity='0';document.body.appendChild(t);t.select();var ok=false;try{ok=document.execCommand('copy')}catch(e){}document.body.removeChild(t);if(ok){var l=b.querySelector('.copy-label'),ci=b.querySelector('.copy-icon'),chi=b.querySelector('.check-icon');ci.style.display='none';chi.style.display='';l.textContent='Copied!';b.classList.add('copied');setTimeout(function(){ci.style.display='';chi.style.display='none';l.textContent='Copy';b.classList.remove('copied')},2000)}})(this)"><svg class="copy-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg><svg class="check-icon" style="display:none" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg><span class="copy-label">Copy</span></button></div>
# MAGIC       <div class="code-card-body">What is the percentage split for each model used?</div>
# MAGIC     </div>
# MAGIC <div class="code-card">
# MAGIC       <div class="code-card-header"><span class="code-card-lang">Prompt</span><button class="code-card-copy" onclick="(function(b){var c=b.closest('.code-card').querySelector('.code-card-body').textContent,t=document.createElement('textarea');t.value=c;t.style.position='fixed';t.style.opacity='0';document.body.appendChild(t);t.select();var ok=false;try{ok=document.execCommand('copy')}catch(e){}document.body.removeChild(t);if(ok){var l=b.querySelector('.copy-label'),ci=b.querySelector('.copy-icon'),chi=b.querySelector('.check-icon');ci.style.display='none';chi.style.display='';l.textContent='Copied!';b.classList.add('copied');setTimeout(function(){ci.style.display='';chi.style.display='none';l.textContent='Copy';b.classList.remove('copied')},2000)}})(this)"><svg class="copy-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg><svg class="check-icon" style="display:none" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg><span class="copy-label">Copy</span></button></div>
# MAGIC       <div class="code-card-body">What is the average latency per destination model?</div>
# MAGIC     </div>
# MAGIC <div class="code-card">
# MAGIC       <div class="code-card-header"><span class="code-card-lang">Prompt</span><button class="code-card-copy" onclick="(function(b){var c=b.closest('.code-card').querySelector('.code-card-body').textContent,t=document.createElement('textarea');t.value=c;t.style.position='fixed';t.style.opacity='0';document.body.appendChild(t);t.select();var ok=false;try{ok=document.execCommand('copy')}catch(e){}document.body.removeChild(t);if(ok){var l=b.querySelector('.copy-label'),ci=b.querySelector('.copy-icon'),chi=b.querySelector('.check-icon');ci.style.display='none';chi.style.display='';l.textContent='Copied!';b.classList.add('copied');setTimeout(function(){ci.style.display='';chi.style.display='none';l.textContent='Copy';b.classList.remove('copied')},2000)}})(this)"><svg class="copy-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg><svg class="check-icon" style="display:none" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg><span class="copy-label">Copy</span></button></div>
# MAGIC       <div class="code-card-body">Are there time periods with elevated 429 errors?</div>
# MAGIC     </div>
# MAGIC
# MAGIC <style>
# MAGIC   .code-card { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; border: 1px solid #DCE0E2; border-radius: 0.5rem; overflow: hidden; background: #F9F7F4; max-width: 100%; margin: 0.5rem 0; }
# MAGIC   .code-card-header { display: flex; align-items: center; justify-content: space-between; padding: 0.4rem 0.75rem; background: #EEEDE9; border-bottom: 1px solid #DCE0E2; }
# MAGIC   .code-card-lang { font-size: 0.7rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.1em; color: #5A6F77; }
# MAGIC   .code-card-copy { display: inline-flex; align-items: center; gap: 0.3rem; background: none; border: none; color: #5A6F77; cursor: pointer; padding: 0.2rem 0.4rem; border-radius: 0.25rem; font-size: 0.7rem; font-weight: 500; transition: color 0.15s, background 0.15s; }
# MAGIC   .code-card-copy:hover { color: #1B3139; background: rgba(0,0,0,0.05); }
# MAGIC   .code-card-copy.copied { color: #2272B4; }
# MAGIC   .code-card-copy svg { width: 14px; height: 14px; }
# MAGIC   .code-card-body { margin: 0; padding: 1rem; overflow-x: auto; font-family: 'SF Mono', 'Cascadia Code', 'Fira Code', Consolas, monospace; font-size: 0.82rem; line-height: 1.55; color: #1B3139; white-space: pre-wrap; word-break: break-word; }
# MAGIC </style>

# COMMAND ----------

# MAGIC %md
# MAGIC ### G7. Add a Fallback Model and Policy
# MAGIC Finally, we'll configure a fallback model. 
# MAGIC 1. In the **Overview** tab click on **Routing**
# MAGIC 1. Click **Add fallback**
# MAGIC 1. Choose a model to fallback to as a **Databricks hosted** provider
# MAGIC 1. Click **Add**
# MAGIC
# MAGIC With this final update, your agentic application is now ready for you to begin building and iterating technical diagrams for future Databricks notebooks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## H. Exploring the `system` Catalog
# MAGIC
# MAGIC In the `system` catalog, you can see two different catalogs that are worth exploring: `system.ai` and `system.ai_gateway`. 
# MAGIC
# MAGIC - **`system.ai`** is the Unity Catalog schema for AI securables like Databricks-hosted model services, MCP services, and built-in service policies. By default, users can `EXECUTE` the system-provided models there.  
# MAGIC
# MAGIC - **`system.ai_gateway`** is the system schema for telemetry/observability, not the model objects themselves. It stores tables like `system.ai_gateway.usage` and `system.ai_gateway.external_model_spend` for usage, latency, routing, and spend tracking.
# MAGIC
# MAGIC ##### How do the system catalogs/tables differ from the model service inference table?
# MAGIC
# MAGIC `system.ai` describes what the model service is and how it is governed. The inference table created previously records what was sent to and returned by that service. `system.ai_gateway.usage` records how the service was used and performed, without being the primary store for full request and response payloads.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### H1. Querying the System Table
# MAGIC Just like before, we can also query the tables in the system catalog as long as you have permissions to do so.  Below is a sample prompt you can send to the **Sample Data Explorer** using natural language. 
# MAGIC <div class="code-card">
# MAGIC       <div class="code-card-header"><span class="code-card-lang">Prompt</span><button class="code-card-copy" onclick="(function(b){var c=b.closest('.code-card').querySelector('.code-card-body').textContent,t=document.createElement('textarea');t.value=c;t.style.position='fixed';t.style.opacity='0';document.body.appendChild(t);t.select();var ok=false;try{ok=document.execCommand('copy')}catch(e){}document.body.removeChild(t);if(ok){var l=b.querySelector('.copy-label'),ci=b.querySelector('.copy-icon'),chi=b.querySelector('.check-icon');ci.style.display='none';chi.style.display='';l.textContent='Copied!';b.classList.add('copied');setTimeout(function(){ci.style.display='';chi.style.display='none';l.textContent='Copy';b.classList.remove('copied')},2000)}})(this)"><svg class="copy-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 01-2-2V4a2 2 0 012-2h9a2 2 0 012 2v1"/></svg><svg class="check-icon" style="display:none" fill="none" stroke="currentColor" viewBox="0 0 24 24" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg><span class="copy-label">Copy</span></button></div>
# MAGIC       <div class="code-card-body">Are there time periods with elevated 429 errors?</div>
# MAGIC     </div>
# MAGIC
# MAGIC <style>
# MAGIC   .code-card { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; border: 1px solid #DCE0E2; border-radius: 0.5rem; overflow: hidden; background: #F9F7F4; max-width: 100%; margin: 0.5rem 0; }
# MAGIC   .code-card-header { display: flex; align-items: center; justify-content: space-between; padding: 0.4rem 0.75rem; background: #EEEDE9; border-bottom: 1px solid #DCE0E2; }
# MAGIC   .code-card-lang { font-size: 0.7rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.1em; color: #5A6F77; }
# MAGIC   .code-card-copy { display: inline-flex; align-items: center; gap: 0.3rem; background: none; border: none; color: #5A6F77; cursor: pointer; padding: 0.2rem 0.4rem; border-radius: 0.25rem; font-size: 0.7rem; font-weight: 500; transition: color 0.15s, background 0.15s; }
# MAGIC   .code-card-copy:hover { color: #1B3139; background: rgba(0,0,0,0.05); }
# MAGIC   .code-card-copy.copied { color: #2272B4; }
# MAGIC   .code-card-copy svg { width: 14px; height: 14px; }
# MAGIC   .code-card-body { margin: 0; padding: 1rem; overflow-x: auto; font-family: 'SF Mono', 'Cascadia Code', 'Fira Code', Consolas, monospace; font-size: 0.82rem; line-height: 1.55; color: #1B3139; white-space: pre-wrap; word-break: break-word; }
# MAGIC </style>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC In this demo, you configured Unity AI Gateway end-to-end for an agentic application. You created a model service, granted permissions, and then iteratively hardened governance by adding an inference table, rate limits, a hallucination guardrail policy, traffic splitting between Claude models, and fallback routing. Finally, you explored the `system.ai` and `system.ai_gateway` schemas to understand how telemetry and observability complement payload-level inference logging. These patterns form the foundation for deploying production-grade generative AI applications with centralized governance on Databricks.
# MAGIC
# MAGIC **Further reading:** Unity AI Gateway documentation (<a href="https://docs.databricks.com/aws/en/ai-gateway/" style="color: #1976d2; text-decoration: underline;">AWS</a> | <a href="https://learn.microsoft.com/en-us/azure/databricks/ai-gateway/" style="color: #1976d2; text-decoration: underline;">Azure</a> | <a href="https://docs.databricks.com/gcp/en/ai-gateway/" style="color: #1976d2; text-decoration: underline;">GCP</a>)

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; <span id="dbx-year"></span> Databricks, Inc. All rights reserved.
# MAGIC Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
# MAGIC <script>
# MAGIC   document.getElementById("dbx-year").textContent = new Date().getFullYear();
# MAGIC </script>
# MAGIC