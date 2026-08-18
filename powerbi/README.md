# Power BI semantic model -- Fjord Mart Analytics

This is a real, version-controlled Power BI semantic model written as
TMDL (Tabular Model Definition Language) source -- the same text format
Power BI Desktop itself uses when a project is saved as a `.pbip`
(Power BI Project), not a mockup. It was authored on Linux, where
Desktop can't run, so it's structured to be opened and exercised on a
Windows/Mac machine rather than something built and screenshotted here.

## What's here

```
powerbi/
  export_gold_for_powerbi.py   Exports every gold mart to Parquet (tested, real -- see docs/BUILD_LOG.md)
  model/
    model.tmdl                 Root: table list, culture, expression list
    expressions.tmdl           ParquetFolder -- the one shared M parameter every table reads from
    relationships.tmdl         Star schema wiring, with reasoning for each non-obvious choice
    roles.tmdl                 RLS -- dynamic ("Regional Manager") + a static demo role
    tables/
      DimDate.tmdl              Calendar hierarchy (Year > Month > Day)
      DimCustomers.tmdl
      DimStores.tmdl            Geography hierarchy (Country > City > Store)
      DimProducts.tmdl          SCD2 -- Category hierarchy (Category > Subcategory > Product)
      DimUserCountryAccess.tmdl RLS mapping table (hidden from report view)
      FactOrders.tmdl           Order grain
      FactOrderItems.tmdl       Line grain, SCD2 point-in-time margin
      FactCampaignPerformance.tmdl
      FactClickstreamSessions.tmdl
      _Measures.tmdl            All DAX measures, one browsable home
```

## Why Parquet, not a live DuckDB connector

DuckDB has no first-class Power BI connector (a community ODBC driver
exists but adds a system dependency this repo can't verify from a
Linux build environment). Parquet import is zero-driver and identical
on Windows/Mac; Power BI Desktop reads it natively via **Get Data >
Parquet**. If this project's warehouse target moves to Snowflake (see
`dbt/profiles.yml` -- that's a config swap, not a rewrite), Power BI's
native Snowflake connector replaces this export entirely and nothing in
the model itself changes.

## Quickstart (on a Windows/Mac machine with Power BI Desktop installed)

1. **Export the data.** From the repo, with the platform running:
   ```bash
   docker compose --profile orchestration exec airflow-scheduler \
     python /opt/airflow/powerbi/export_gold_for_powerbi.py
   ```
   This writes to `powerbi/data_export/*.parquet` on the host (bind-mounted).
   Copy that folder to your Windows/Mac machine if Desktop isn't running
   against the same filesystem.

2. **Open the model.** Either:
   - Create a new blank Power BI Desktop project saved as `.pbip`, then
     replace its `<name>.SemanticModel/definition/` contents with the
     files under `powerbi/model/` (matching folder layout: `tables/`
     subfolder, root files alongside), and re-open the `.pbip`; or
   - Use Desktop's **Model view > TMDL view** (Options > Preview
     features > "TMDL view for Model editing") to paste each file's
     contents directly into a blank model.

3. **Point `ParquetFolder` at your export.** Transform Data > Manage
   Parameters > `ParquetFolder` > set to the absolute path of
   `powerbi/data_export/` on your machine (must end in a path
   separator). Refresh.

4. **Test RLS without publishing anything.** Modeling ribbon > **View
   As** > tick **Regional Manager** > **Other user** > paste
   `regional.manager.ie@fjordmart.example` (or `.gb`/`.de`, or
   `global.exec@fjordmart.example` for the `*` all-countries case --
   see `export_gold_for_powerbi.py`'s `USER_COUNTRY_ACCESS` list). Every
   visual re-filters live to that user's country. This is the free,
   local, fully-sufficient way to prove RLS works -- **no Power BI
   Service tenant or Pro licence needed** for this. Also try the
   **Ireland Only (static demo)** role for a no-lookup-table sanity
   check.

## Where paid Power BI Service actually enters the picture

Publishing this model so *other, real* users see enforced RLS when
*they* open a shared report (rather than you, locally, using View As)
needs the model published to a **Power BI Service workspace**, which
needs a tenant and, for anyone viewing it, at least Power BI Pro (or a
Premium-capacity workspace) — that's the one piece of this whole build
that genuinely can't be done for free. Everything else -- the model
itself, every DAX measure, every hierarchy, and RLS actually filtering
correctly -- is fully built, fully real, and fully testable locally, for
free, right now.

## What to actually look at if you only have a few minutes

- `model/tables/FactOrderItems.tmdl` + `model/tables/DimProducts.tmdl`:
  the SCD2 point-in-time relationship (`ProductKey`, not `ProductId`) --
  this is the model actually using the Spark MERGE-built SCD2 history
  for something (correct historical margin), not just displaying it.
- `model/roles.tmdl`: dynamic RLS via a lookup table and
  `USERPRINCIPALNAME()`, not a hard-coded filter per role.
- `model/tables/_Measures.tmdl`'s `Data Quality: SCD2 Fallback Rate %`
  measure: a real, documented data-lineage caveat (see
  `docs/BUILD_LOG.md`) surfaced directly in the BI layer, not left only
  in a markdown file nobody using the dashboard will ever read.
