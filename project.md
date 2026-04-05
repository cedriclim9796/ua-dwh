### 1.1 Data Exploration

### 1.1 Data Ingestion & Modeling
- Load the CSV into bigquery 
- build the core tables needed for analytics
- pipeline should work as a daily batch process
- Include basic tests or data quality assertions using Great Expectations
#### Required Metrics (Daily / Weekly / Monthly)
- Active Entity Growth Accounting
- New Users — first-time actives in the period
- Retained Users — active in both current and prior period
- Resurrected Users — active now, inactive in prior period, but seen before
- Churned Users — active in prior period but not current
- cohort retention chart
- Engagement depth (e.g. events per active user, session frequency)
#### Bonus Metrics
- Any additional metric you think tells an important story in this dataset
#### 
### 1.2 Growth Accounting Dashboard
Build a dashboard to track the following growth accounting metrics. 





1.3 Infrastructure & Code Quality
● Share everything via a public GitHub repo with a clear, self-contained README
● Your 
● Code should be readable, well-structured, and production-minded
● Include at least basic tests or data quality assertions
● Bonus: CI/CD pipeline, logging, dbt docs, or containerisation

Part 2 — AI Agentic Systems Design
This part is about how you think, not just what you build. You will design an AI agent to help
scale the kind of data work you just completed in Part 1. We are looking for clear reasoning,
honest tradeoff analysis, and practical scoping.
2.1 Design: Data Documentation Agent
Design an AI agent that automatically generates and maintains column-level documentation for
dbt models as they evolve. Your design document should cover:
A. Agent Architecture
● What triggers the agent?
● What inputs does the agent need?
● What tools or APIs does it call, and in what order?
● How does it handle models it has never seen before vs. models it has documented
before?
B. Human-in-the-Loop Design
● At what points should a human review or approve the agent's output?
● How do you prevent the agent from silently writing bad documentation at scale?

● How would you surface proposed changes to the team?
C. Failure Modes & Observability
● What are the top 3 ways this agent could fail or degrade in production?
● How would you detect each failure mode?
● What logging, evals, or alerting would you put in place?
D. Scope & Build Plan
● If you had one week to ship a working v1, what would be in scope and what would not?
● How would you measure whether the agent is actually useful?
Deliverable: A written design doc (Markdown in your repo is fine) of ~300–600 words.
Diagrams or pseudocode are welcome but not required.
