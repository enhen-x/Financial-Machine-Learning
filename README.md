# Financial Machine Learning

A-share data engineering toolkit for trend-following ML research. This repository focuses on reliable data ingestion, local database construction, and reproducible preprocessing workflows.

## What This Project Provides

- Daily A-share local SQLite database builder (market data + key reference tables)
- Info-driven bar builder from minute data (`tick`, `volume`, `dollar` bars)
- Retry, integrity checks, progress display, and logging for long-running jobs
- Structured script modules for ingestion, processing, and quality checks
- Chapter notes under `summery/` for *Advances in Financial Machine Learning*

## Repository Structure

```text
.
|- data/
|  |- daily_bars/
|  `- info_driven_bars/
|- logs/
|- scripts/
|  |- data_ingestion/
|  |  |- daily_bars/
|  |  |  |- build_a_share_daily_db.py
|  |  |  `- init_tushare_secret.ps1
|  |  `- info_driven_bars/
|  |     `- build_info_driven_bars.py
|  |- data_processing/
|  `- data_quality/
|- secrets/
|  `- tushare.env.example
`- summery/
```

## Environment Setup

1. Create and activate virtual environment

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

2. Install dependencies

```powershell
pip install tushare pandas
```

## Tushare Configuration

Copy template and fill your private values:

```powershell
Copy-Item secrets\tushare.env.example secrets\tushare.env
```

Then edit `secrets/tushare.env`:

```env
TUSHARE_TOKEN=your_token
TUSHARE_HTTP_URL=http://your_gateway_or_proxy
```

Optional helper to initialize and protect secret file:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\data_ingestion\daily_bars\init_tushare_secret.ps1
```

## Build Daily A-share Database

Full init:

```powershell
python scripts\data_ingestion\daily_bars\build_a_share_daily_db.py --init --start-date 20100101 --end-date 20260328
```

Incremental update:

```powershell
python scripts\data_ingestion\daily_bars\build_a_share_daily_db.py --update --lookback-days 10
```

Default output:

- `data/daily_bars/a_share_daily.db`
- `logs/build_a_share_daily_db.log`

## Build Info-Driven Bars

Small sample run (recommended first):

```powershell
python scripts\data_ingestion\info_driven_bars\build_info_driven_bars.py --init --start-date 20200102 --end-date 20200103 --max-symbols-per-day 5 --api-calls-per-minute 2
```

Default output:

- `data/info_driven_bars/a_share_info_driven.db`
- `logs/build_info_driven_bars.log`

## Important Notes

- Minute-data endpoints are often strongly rate-limited by account permissions.
- For low limits (for example `2 calls/min`), always run small batches first.
- If you see permission/rate-limit errors, check your Tushare plan and interface rights.
- Logs are written to `logs/`; monitor there for retry and failure details.

## Data Processing Standards

Data-processing conventions are documented in:

- `scripts/data_processing/README.md`

Please follow that document for feature engineering and labeling workflows.

## License

This project is licensed under the terms in `LICENSE`.
