# orchestration/flows/elt_pipeline.py
from prefect import flow, task, get_run_logger
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

from orchestration.logging_setup import setup_file_logging

from extractor.api_extractor import TranslationAPIExtractor, GeolocationAPIExtractor
from extractor.postgres_extractor import PostgresSnapshotExtractor, PostgresIncrementalExtractor
from extractor.s3_extractor import ExternalS3Extractor
from loader.main import build_pipeline as build_loader, SOURCES

import os
# --------------------------------------------------------------------------- #
# Extractor tasks — one per source, independently retried
# --------------------------------------------------------------------------- #

@task(name="extract-translations", retries=3, retry_delay_seconds=30, log_prints=True)
def extract_translations(force_refresh: bool):
    TranslationAPIExtractor().run(force_refresh=force_refresh)

@task(name="extract-geolocation", retries=3, retry_delay_seconds=30, log_prints=True)
def extract_geolocation(force_refresh: bool):
    GeolocationAPIExtractor().run(force_refresh=force_refresh)

@task(name="extract-pg-static", retries=2, retry_delay_seconds=30, log_prints=True)
def extract_pg_static():
    PostgresSnapshotExtractor().run()

@task(name="extract-pg-live", retries=2, retry_delay_seconds=30, log_prints=True)
def extract_pg_live():
    PostgresIncrementalExtractor().run()

@task(name="extract-s3", retries=2, retry_delay_seconds=30, log_prints=True)
def extract_s3():
    ExternalS3Extractor().run()


# --------------------------------------------------------------------------- #
# Loader tasks — one per bronze table
# --------------------------------------------------------------------------- #

@task(name="load-bronze-table", retries=2, retry_delay_seconds=15, log_prints=True)
def load_bronze_table(table_name: str):
    pipeline = build_loader()
    pipeline.load_table(table_name)  # raises on failure — no silent swallow


# --------------------------------------------------------------------------- #
# dbt tasks
# --------------------------------------------------------------------------- #

@task(name="dbt-build", log_prints=True)
def dbt_build(select: str, project_dir: str, profiles_dir: str):
    settings = PrefectDbtSettings(project_dir=project_dir, profiles_dir=profiles_dir)
    runner = PrefectDbtRunner(settings=settings)
    return runner.invoke(["build", "--select", select])


# --------------------------------------------------------------------------- #
# Daily flow
# --------------------------------------------------------------------------- #

@flow(name="elt-pipeline", log_prints=True)
def elt_pipeline(
    api_force_refresh: bool = False,
    dbt_project_dir: str = "transform",
    dbt_profiles_dir: str = "~/.dbt",
):
    logger = get_run_logger()
    logger.info("Starting ELT pipeline (api_force_refresh=%s)", api_force_refresh)

    profiles_dir = os.path.expanduser(dbt_profiles_dir) 

    extract_futs = [
        extract_translations.submit(api_force_refresh),
        extract_geolocation.submit(api_force_refresh),
        extract_pg_static.submit(),
        extract_pg_live.submit(),
        extract_s3.submit(),
    ]
    [f.result() for f in extract_futs]  # raises if any extractor failed

    load_futs = [load_bronze_table.submit(t) for t in SOURCES]
    [f.result() for f in load_futs]

    dbt_build(select="silver", project_dir=dbt_project_dir, profiles_dir=profiles_dir)
    dbt_build(select="gold", project_dir=dbt_project_dir, profiles_dir=profiles_dir)

    logger.info("ELT pipeline complete")

log_file = setup_file_logging(run_name="elt_pipeline")


if __name__ == "__main__":
    print(f"Logging to: {log_file}")
    # For manual Run Just call the function
    # elt_pipeline()

    # For server mode
    elt_pipeline.serve(
        name="elt-pipeline",
        cron="*/5 * * * *",
        )

    # For deploy mode
    # flow.from_source(
    #     source=".",  # current project directory
    #     entrypoint="orchestration/flows/elt_pipeline.py:elt_pipeline",
    # ).deploy(
    #     name="elt-pipeline-dep",
    #     work_pool_name="elt-pool",
    #     cron="*/5 * * * *",
    # )
