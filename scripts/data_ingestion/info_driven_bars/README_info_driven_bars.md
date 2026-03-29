# 信息驱动采样数据拉取脚本

脚本：`scripts/data_ingestion/info_driven_bars/build_info_driven_bars.py`

## 功能

1. 从 Tushare 分钟数据拉取（默认 `1min`）。
2. 生成三类信息驱动采样 bars：
- `tick`（按分钟条数累计）
- `volume`（按成交量累计）
- `dollar`（按成交额累计）
3. 写入目标库：`data/info_driven_bars/a_share_info_driven.db`

## 依赖

```powershell
pip install tushare pandas
```

## 运行示例

```powershell
python scripts/data_ingestion/info_driven_bars/build_info_driven_bars.py --init
python scripts/data_ingestion/info_driven_bars/build_info_driven_bars.py --update
python scripts/data_ingestion/info_driven_bars/build_info_driven_bars.py --update --lookback-days 10
```

只拉部分股票（逗号分隔）：

```powershell
python scripts/data_ingestion/info_driven_bars/build_info_driven_bars.py --init --start-date 20250101 --end-date 20250131 --ts-codes 000001.SZ,600000.SH
```

## 关键参数

- `--bar-types`: `tick,volume,dollar`
- `--tick-threshold`: tick bar 阈值
- `--volume-threshold`: volume bar 阈值
- `--dollar-threshold`: dollar bar 阈值
- `--max-symbols-per-day`: 自动选股时每日最大股票数
- `--workers` / `--max-inflight`: 并行控制
- `--timeout-sec` / `--retries`: 网络波动控制

## 数据来源与目标

- 源库（默认）：`data/daily_bars/a_share_daily.db`
- 目标库（默认）：`data/info_driven_bars/a_share_info_driven.db`

## 说明

1. 当前脚本使用分钟数据近似构造信息驱动采样，后续可接入逐笔成交进一步提升精度。
2. 单任务失败不会中断全局，会在日志记录并继续执行。
