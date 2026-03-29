# Scripts Module Index

## Structure
- `scripts/data_ingestion`: data pulling and source ingestion scripts.
- `scripts/data_processing`: feature engineering and table transformation scripts.
- `scripts/data_quality`: data checks, integrity validation, and repair scripts.

## Current scripts
- `scripts/data_ingestion/daily_bars/build_a_share_daily_db.py`: build/update local A-share daily SQLite DB.
- `scripts/data_ingestion/daily_bars/init_tushare_secret.ps1`: initialize and protect local Tushare secret file.
- scripts/data_ingestion/daily_bars/README_tushare_daily_db.md: detailed usage doc for ingestion DB builder.
- scripts/data_ingestion/info_driven_bars/build_info_driven_bars.py: pull minute data and build tick/volume/dollar info-driven bars.

## Compatibility entrypoints
- `scripts/build_a_share_daily_db.py`: compatibility wrapper, forwards to ingestion module.
- `scripts/init_tushare_secret.ps1`: compatibility wrapper, forwards to daily_bars module.

## Conventions
- Add new raw source pullers under `data_ingestion`.
- Add derived feature jobs under `data_processing`.
- Add completeness checks / repair jobs under `data_quality`.



