# orchestration/flows/reset_pipeline.py
from prefect import flow, task, get_run_logger

from orchestration.logging_setup import setup_file_logging
from extractor.reset import empty_s3_bucket, DEFAULT_BUCKET
from loader.main import build_pipeline as build_loader
from config_loader import CONFIG


@task(name="reset-s3-bronze-bucket", log_prints=True)
def reset_extractor_bucket(bucket_name: str):
    logger = get_run_logger()
    logger.warning("Emptying bucket s3://%s — force=True, confirmation was handled upstream", bucket_name)
    # force=True: skip the interactive input() entirely — confirmation already
    # happened once, in reset_pipeline's __main__ block. Prevents a second,
    # stdin-dependent prompt from hanging inside a Prefect task/worker.
    empty_s3_bucket(bucket_name=bucket_name, force=True)

@task(name="reset-postgres-bronze-schema", log_prints=True)
def reset_loader_schema():
    """Truncates every table registered in SOURCES. Re-creates schema via DDL after."""
    pipeline = build_loader()  # this already re-runs create_bronze_schema() on build
    for table_name in pipeline.sources:
        pipeline.loader.run_sql(f"TRUNCATE TABLE {table_name}")
    pipeline.loader.run_sql("TRUNCATE TABLE bronze._processed_files")


@flow(name="reset-pipeline", log_prints=True)
def reset_pipeline(reset_s3: bool = True, reset_postgres: bool = True):
    logger = get_run_logger()
    logger.warning("DESTRUCTIVE RESET requested (s3=%s, postgres=%s)", reset_s3, reset_postgres)

    if reset_s3:
        reset_extractor_bucket(CONFIG["storage"]["bronze_bucket"])
    if reset_postgres:
        reset_loader_schema()

    logger.warning("Reset complete")


if __name__ == "__main__":
    log_file = setup_file_logging(run_name="reset_pipeline")
    print(f"Logging to: {log_file}")

    # Require explicit confirmation so this can't be run by muscle memory
    confirm = input("Type 'RESET' to confirm wiping bronze bucket + schema: ")
    if confirm == "RESET":
        reset_pipeline()
    else:
        print("Aborted.")  