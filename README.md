# ReNew_AI_Week - Databricks Asset Bundle

## Overview
At ReNew Capital Partners, the Chief Investment Officer owns portfolio cash yield and distribution coverage across a diversified mix of wind and solar assets. Day to day, the CIO relies on a single portfolio view of availability, net energy production, and revenue variance to ensure the fund stays on track against merchant price exposure and operating cost plans.

On 2025-11-21 that rhythm breaks: the portfolio 30-day average net generation drops from about 18,500 MWh/day to about 15,900 MWh/day and stays depressed into mid-December. The analyst team quickly proves the drop is not portfolio-wide. Wind remains stable, while three Texas solar plants show a step-change in inverter availability and a repeating weekday curtailment pattern that amplifies lost production during stacked constraint windows.

Drilling into vendor changes reveals a single dated cause: a firmware rollout for the XG-440 inverter fleet executed between 2025-11-20 and 2025-11-21. The new firmware increases nuisance protective trips and derate behavior, driving avoidable downtime and higher reactive dispatch.

Through 2025-12-15, the issue quantifiably costs $1.42M in lost merchant revenue plus penalties and imbalance charges, and drives an additional $260K in incremental O&M.

With data and metrics governed on Databricks, the team can build AI Agents with Agent Bricks to monitor generation, availability, and revenue variance, reason over firmware and work-order context, and automatically trigger vendor escalation, dispatch workflows, and stakeholder alerts the moment conditions repeat.

## Tested Region

This demo has been tested on **Databricks on AWS us-west-2 (Oregon)**. Deployment to other regions may work for the core DAB components, but several AI features have limited regional availability and may not function outside of supported regions. See the [Regional Availability Notes](#regional-availability-notes) section for details.

## Deployment

### Step 0 — Extract the Bundle

Unzip the archive and navigate into the directory:

```bash
unzip ReNew_AI_Week_dab_bundle.zip
cd ReNew_AI_Week_dab_bundle
```

### Prerequisites

1. **Databricks CLI v0.200+**: Install the latest version
   ```bash
   # macOS
   brew tap databricks/tap && brew install databricks
   # or via pip
   pip install databricks-cli
   ```

2. **Authentication**: Configure your workspace credentials
   ```bash
   databricks configure --host https://<your-workspace-url> --token
   ```
   Or using OAuth:
   ```bash
   databricks auth login --host https://<your-workspace-url>
   ```

3. **Workspace Access**: Ensure you have permissions for:
   - `main` catalog must already exist in Unity Catalog (DAB cannot create catalogs)
   - CREATE SCHEMA and CREATE VOLUME on the `main` catalog
   - SQL Warehouse access (a warehouse named `Shared Unity Catalog Serverless` must exist, or update `warehouse_name` in `databricks.yml`)
   - Workspace file storage
   - Model Serving (for Knowledge Assistant endpoint creation)
   - Genie Space and AI/BI features enabled on the workspace

### Step 1 — Update Configuration for Your Workspace

**Edit `databricks.yml`** — change the workspace path and schema name to your own:

```yaml
variables:
  workspace_path:
    default: /Users/<your-email@company.com>/ReNew_AI_Week   # your Databricks user path
  warehouse_name:
    default: Shared Unity Catalog Serverless                  # match an existing warehouse name

resources:
  schemas:
    demo_schema:
      name: <your_schema_name>        # change from madan_gopal to your preferred schema name
```

**Edit `bricks_conf.json`** — update the warehouse ID on line 8 to match your workspace:

```bash
# Find your warehouse ID
databricks warehouses list
```

Then replace `"warehouse_id": "862f1d757f0424f7"` with your warehouse's ID.

> **Note**: All other IDs in `bricks_conf.json` (genie space ID, tile IDs, etc.) are from the original workspace and will be replaced automatically when `deploy_resources.py` runs — you do not need to change them.

### Step 2 — Validate the Bundle

```bash
databricks bundle validate
```

Fix any warnings about missing warehouses or catalog permissions before proceeding.

### Step 3 — Deploy the Bundle

```bash
databricks bundle deploy --force-lock
```

This will:

1. Create Unity Catalog schema and volume under `main`
2. Sync all Python scripts, SQL, PDFs, and dashboard JSON to the workspace
3. Deploy the Lakeview dashboard
4. Register the workflow job

### Step 4 — Run the Workflow

```bash
databricks bundle run demo_workflow
```

Or trigger from the Databricks UI: **Workflows > [DEMOGEN] - ReNew_AI_Week - Data Generation and Transformation > Run Now**.

The workflow runs three sequential tasks:

1. **`generate_data`** — Generates synthetic renewable energy data and writes Delta tables to Unity Catalog
2. **`sql_transformations`** — Runs bronze → silver → gold medallion transformations via SQL warehouse
3. **`deploy_agent_bricks`** — Creates the Genie Space, Knowledge Assistant, and Multi-Agent Supervisor from `bricks_conf.json`

### Step 5 — Verify the Assets

After the workflow completes, verify in the Databricks UI:

| Asset | Location in UI |
| --- | --- |
| Gold tables | **Catalog > main > `<your_schema_name>`** |
| PDF files | **Catalog > main > `<your_schema_name>` > raw_data** (volume) |
| Lakeview Dashboard | **Dashboards > ReNew Portfolio Performance and Firmware Impact** |
| Genie Space | **Dashboards > ReNew AI Week - Portfolio Performance & Firmware Impact (Genie AI-BI)** |
| Knowledge Assistant | **Dashboards** (tile: `DBGEN_ReNew_Portfolio_Firmware_Incident_KA`) |
| Multi-Agent Supervisor | **Dashboards** (tile: `DBGEN_ReNew_Portfolio_Firmware_Supervisor`) |

### Step 6 — (Optional) Run the Databricks App

The `app/` folder contains a React + FastAPI Databricks App. If you want to deploy it:

```bash
cd app/frontend
npm install        # restore node_modules (excluded from zip to reduce size)
npm run build      # rebuild the dist/ folder
cd ../..
databricks apps deploy --source-code-path ./app
```

---

## Regional Availability Notes

This demo was tested on **AWS us-west-2 (Oregon)**. The table below summarizes which components are region-agnostic vs. which have limited availability outside of us-west-2.

### Component Availability

| Component | Availability | Notes |
| --- | --- | --- |
| DAB (jobs, schema, volume, dashboard sync) | All regions | Region-agnostic |
| Unity Catalog | All regions | GA globally |
| Lakeview Dashboards | All regions | GA globally |
| SQL Warehouses (serverless) | Most regions | Confirm serverless is enabled in target workspace |
| Foundation Model APIs (`databricks-gte-large-en`) | Limited regions | Required for Knowledge Assistant; may not be available outside us-west-2/us-east-1 |
| Genie Space (AI/BI Genie) | Limited regions | Requires AI/BI feature flag enabled on workspace |
| Knowledge Assistant | Limited regions | Requires Foundation Model API + Model Serving |
| Multi-Agent Supervisor | Limited regions | Requires Genie + Knowledge Assistant; newer feature |

### Features That May Not Work in Other Regions

#### 1. Foundation Model API — embedding model availability

The Knowledge Assistant uses `databricks-gte-large-en` for embeddings. This model is only available in regions where Databricks Foundation Model APIs (pay-per-token) are enabled. Confirm availability in your target workspace before deploying:

```bash
databricks serving-endpoints list | grep gte
```

If the model is unavailable, the `deploy_agent_bricks` job task will fail. Contact your Databricks account team to enable FMAPIs in your region.

#### 2. Agent Bricks feature flag (Genie, Knowledge Assistant, Multi-Agent Supervisor)

These features require explicit enablement on the workspace and may not be available in all regions. Verify by navigating to **Databricks UI > Dashboards** and checking for the Genie/AI options. If not visible, request enablement from your account team.

#### 3. Serverless compute

The job environments use `client: '3'` (serverless compute). If serverless is not available in your region, add a `job_cluster_key` and cluster spec to each task in `databricks.yml`.

### Required Config Changes for Any New Workspace

1. Update `databricks.yml`: set `workspace_path` to your user path and `warehouse_name` to match an existing warehouse.
2. Update `warehouse_id` in `bricks_conf.json` to the target workspace's warehouse ID (`databricks warehouses list` to find it).
3. The existing IDs in `bricks_conf.json` (`genie_space.config.id`, `knowledge_assistant.tile.tile_id`, etc.) are from the original workspace — `deploy_resources.py` recreates all resources idempotently; these fields are informational only.

## Bundle Contents

### Core Files
- `databricks.yml` - Asset bundle configuration defining jobs, dashboards, and deployment settings
- `bricks_conf.json` - Agent brick configurations (Genie/KA/MAS) if applicable
- `agent_bricks_service.py` - Service for managing agent brick resources (includes type definitions)
- `deploy_resources.py` - Script to recreate agent bricks in the target workspace

### Data Generation
- Python scripts using Faker library for realistic synthetic data
- Configurable row counts, schemas, and business logic
- Automatic Delta table creation in Unity Catalog

### SQL Transformations
- `transformations.sql` - SQL transformations for data processing
- Bronze (raw) → Silver (cleaned) → Gold (aggregated) medallion architecture
- Views and tables for business analytics

### Agent Bricks
This bundle includes AI agent resources:

- **Genie Space** (ID: `01f11d1809eb1a109ecec68f8b72904b`)
  - Natural language interface for data exploration
  - Configured with table identifiers from your catalog/schema
  - Sample questions and instructions included

- **Knowledge Assistant** (ID: `b76cd4b8-40c1-4e7f-9ac8-59353fa35c73`)
  - AI assistant with knowledge sources from Unity Catalog volumes
  - Vector search-powered retrieval augmented generation (RAG)
  - Example questions and guidelines included

### Dashboards
This bundle includes Lakeview dashboards:
- **ReNew Portfolio Performance and Firmware Impact** - Business intelligence dashboard with visualizations

### PDF Documents
This bundle includes PDF documents that will be uploaded to the workspace:
- PDF files are automatically uploaded during bundle deployment via DAB artifacts
- These PDFs are included in the bundle workspace path
- You can manually copy them to Unity Catalog Volumes if needed for RAG scenarios
- Example: Use Databricks Files API or `dbutils.fs.cp` to move files to a volume

## Configuration

### Unity Catalog

- **Catalog**: `main` (must pre-exist; DAB cannot create catalogs)
- **Schema**: configurable — defaults to `madan_gopal`, change in `databricks.yml`
- **Volume**: `raw_data` (created under the schema above)
- **Workspace Path**: configurable via `workspace_path` variable in `databricks.yml`

### Customization

Edit `databricks.yml` to:

- Change the target schema name (`resources.schemas.demo_schema.name`)
- Change the workspace path (`variables.workspace_path.default`)
- Change the SQL warehouse name (`variables.warehouse_name.default`)
- Add a classic cluster spec to job tasks if serverless compute is unavailable

## Key Questions This Demo Answers
1. When did net generation first break from the 30-day baseline, and what was the magnitude of the drop (18,500 MWh/day to 15,900 MWh/day) starting 2025-11-21?
2. Which plants and asset types contributed most to the variance after 2025-11-21, and did wind remain stable while three Texas solar plants drove the drop?
3. How did availability change by equipment type and inverter model, and how much of the downtime is attributable to XG-440 inverters post-firmware rollout?
4. What firmware versions were running before and after the 2025-11-20 to 2025-11-21 rollout, and how do vendor change tickets align with the onset of increased trips and derates?
5. How much cumulative merchant revenue was lost and how much was driven by penalties and imbalance charges through 2025-12-15 (total $1.42M)?
6. How much incremental O&M spend was incurred through 2025-12-15 ($260K), and which corrective work order types and vendors drove it?
7. What weekday curtailment pattern explains the recurring lost production, and how do stacked curtailment windows correlate with net MWh variance in the affected plants?

## Deployment to New Workspaces

This bundle is **portable** and can be deployed to any Databricks workspace:

1. The bundle will recreate all resources in the target workspace
2. Agent bricks (Genie/KA/MAS) are recreated from saved configurations in `bricks_conf.json`
3. SQL transformations and data generation scripts are environment-agnostic
4. Dashboards are deployed as Lakeview dashboard definitions

Simply run `databricks bundle deploy` in any workspace where you have the required permissions.

## Troubleshooting

### Common Issues

**Bundle validation fails:**
- Ensure `databricks.yml` has valid YAML syntax
- Check that catalog and schema names are valid
- Verify warehouse lookup matches an existing warehouse

**Agent brick deployment fails:**
- Check that `bricks_conf.json` exists and contains valid configurations
- Ensure you have permissions to create Genie spaces, KA tiles, and MAS tiles
- Verify vector search endpoint exists for Knowledge Assistants

**SQL transformations fail:**
- Ensure the catalog and schema exist in the target workspace
- Check warehouse permissions and availability
- Review SQL syntax for Unity Catalog compatibility (3-level namespace: `catalog.schema.table`)

### Getting Help
- Review Databricks Asset Bundles documentation: https://docs.databricks.com/dev-tools/bundles/
- Check the generated code in this bundle for implementation details
- Contact your Databricks workspace administrator for permissions issues

## Generated with AI Demo Generator
🤖 This bundle was automatically created using the Databricks AI Demo Generator.

**Created**: 2026-03-11 06:44:18
**User**: madan.gopal@databricks.com
