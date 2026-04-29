"""Static ETL: ściąga rozkład GTFS z GZM i ładuje do bazy."""

from loguru import logger

from gtfs_olap.static import run

if __name__ == "__main__":
    run()
    logger.success("Static ETL OK")