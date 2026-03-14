from dagster import define_asset_job, AssetSelection

pipeline_job = define_asset_job(
    name="pipeline_job",
    selection=AssetSelection.all(),
    description="Full Bronze -> Silver -> Gold pipeline run",
)

agg_refresh_job = define_asset_job(
    name="agg_refresh_job",
    selection=AssetSelection.keys("gold_aggregates_asset", "gold_exports_asset"),
    description="Fast re-aggregation without re-running bronze/silver/gold facts",
)
