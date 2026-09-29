"""
Data Pipeline Ingestion Orchestrator

Usage Examples:
---------------
1. Standard daily pipeline run (uses cached/existing API data, skips re-fetching
   slow static/reference sources like translations & geolocation):
   python main.py

2. Force-refresh API sources (archives old translation & geolocation files in S3,
   then re-fetches fresh data from the API):
   python main.py --api-force-refresh True

3. Run ONLY API extractors:
   python main.py --only api

4. Run ONLY the Postgres static snapshot (customers, sellers, products):
   python main.py --only postgres-static

5. Run ONLY the Postgres incremental extractor (orders, order items):
   python main.py --only postgres-live

6. Run ONLY the external S3 extractor (reviews & payments):
   python main.py --only s3

7. Combine flags — force-refresh API sources while running only the API layer:
   python main.py --only api --api-force-refresh True

8. Emptying the whole Bronze bucket (use with caution!):
   python main.py --reset True

Arguments:
----------
--api-force-refresh   [True|False, default: False]
                       When True, archives the existing translation & geolocation
                       extract files already in the Bronze bucket (e.g. moves them
                       to an archive/ prefix with a timestamp) and re-fetches fresh
                       data from the API sources. When False (default), the
                       extractors reuse/skip existing data as per their normal
                       incremental behavior.

--only                [postgres-static|postgres-live|api|s3]
                       Restricts the run to a single source layer instead of
                       running the full pipeline. Useful for backfills, debugging,
                       or re-running just one failed layer.

--reset                [True|False, default: False]
                       Empties the entire Bronze bucket before exiting. Does NOT
                       run the rest of the pipeline afterward. Use with caution —
                       this is destructive and cannot be undone.
"""

import argparse
import sys
import time
from datetime import datetime

# Import all extractor modules
from extractor.postgres_extractor import (
    PostgresSnapshotExtractor,
    PostgresIncrementalExtractor,
)
from extractor.api_extractor import (
    TranslationAPIExtractor,
    GeolocationAPIExtractor,
)
from extractor.s3_extractor import ExternalS3Extractor
from extractor.reset import empty_s3_bucket, DEFAULT_BUCKET


from config_loader import CONFIG


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Run Bronze Ingestion Pipeline for Postgres, API, and S3 sources."
    )

    parser.add_argument(
        "--api-force-refresh",
        choices=["True", "False"],
        default="False",
        help="Archive old API extract files and force re-fetching fresh data from the API sources.",
    )

    parser.add_argument(
        "--only",
        choices=["postgres-static", "postgres-live", "api", "s3"],
        help="Run only a specific source layer.",
    )

    parser.add_argument(
        "--reset",
        choices=["True", "False"],
        help="Emptying the entire Bronze bucket. Use with caution!",
    )

    return parser.parse_args()


def run_pipeline(api_force_refresh: bool = False, only: str = None, reset: bool = False):
    start_time = time.time()
    print("=" * 70)
    print(f"🚀 [BRONZE INGESTION PIPELINE START] - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"   Config: api_force_refresh={api_force_refresh} | source_filter={only or 'ALL'} | reset={reset}")
    print("=" * 70)

    if reset:
        empty_s3_bucket(bucket_name=CONFIG["storage"]["bronze_bucket"], force=False)
        return  # Exit after reset to avoid running the pipeline
    # -------------------------------------------------------------------------
    # 1. API EXTRACTORS (Translations & Geolocation)
    # -------------------------------------------------------------------------
    if only is None or only == "api":
        print("\n🌐 === RUNNING API SOURCES ===")

        # Category Translations
        try:
            print("\n[API] Running Category Translation Extractor...")
            translation_extractor = TranslationAPIExtractor()
            translation_extractor.run(force_refresh=api_force_refresh)
        except Exception as e:
            print(f"❌ Error extracting Category Translations: {e}")

        # Large Geolocation Dataset
        try:
            print("\n[API] Running Geolocation Extractor (Batching to S3)...")
            geo_extractor = GeolocationAPIExtractor()
            geo_extractor.run(force_refresh=api_force_refresh)
        except Exception as e:
            print(f"❌ Error extracting Geolocation API: {e}")

    # -------------------------------------------------------------------------
    # 2. POSTGRES EXTRACTORS (Transactional Data)
    # -------------------------------------------------------------------------
    if only is None or only == "postgres-static":
        print("\n🐘 === RUNNING POSTGRES STATIC SOURCES ===")

        # Customers (Static/Reference Table)
        try:
            print("\n[Postgres] Snapshotting Customers + Sellers + Products Table...")
            customer_extractor = PostgresSnapshotExtractor()
            customer_extractor.run()
        except Exception as e:
            print(f"❌ Error in Postgres Static snapshot: {e}")

    if only is None or only == "postgres-live":
        print("\n🐘 === RUNNING POSTGRES LIVE SOURCES ===")

        # Orders (Incremental Table)
        try:
            print("\n[Postgres] Incrementally Extracting Orders + Order Items Table...")
            orders_extractor = PostgresIncrementalExtractor()
            orders_extractor.run()
        except Exception as e:
            print(f"❌ Error in Postgres Incremental extraction: {e}")

    # -------------------------------------------------------------------------
    # 3. EXTERNAL S3 EXTRACTORS (Reviews & Payments)
    # -------------------------------------------------------------------------
    if only is None or only == "s3":
        print("\n🪣 === RUNNING EXTERNAL S3 SOURCES ===")
        try:
            print("\n[S3] Running External Reviews & Payments Extractor...")
            s3_extractor = ExternalS3Extractor()
            s3_extractor.run()
        except Exception as e:
            print(f"❌ Error in External S3 Sync: {e}")

    # -------------------------------------------------------------------------
    # SUMMARY
    # -------------------------------------------------------------------------
    elapsed_time = time.time() - start_time
    print("\n" + "=" * 70)
    print(f"✅ [BRONZE INGESTION PIPELINE COMPLETE] - Execution Time: {elapsed_time:.2f} seconds")
    print("=" * 70)


if __name__ == "__main__":
    args = parse_arguments()

    run_pipeline(
        api_force_refresh=(args.api_force_refresh == "True"),
        only=args.only,
        reset=(args.reset == "True") if args.reset is not None else False,
    )