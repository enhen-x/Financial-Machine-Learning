# 日频 A 股本地数据库（Tushare）

## 1. 功能
脚本：`scripts/data_ingestion/daily_bars/build_a_share_daily_db.py`

可构建并增量更新以下表（SQLite）：
1. `dim_security`
2. `fact_trade_cal`
3. `fact_daily_bar`
4. `fact_adj_factor`
5. `fact_daily_basic`
6. `fact_stk_limit`
7. `fact_suspend_d`
8. `fact_namechange`
9. `fact_universe_pti`

默认库文件：`data/daily_bars/a_share_daily.db`

## 2. 依赖
```bash
pip install tushare pandas
```

## 3. 鉴权与自定义地址
建议用环境变量，避免 token 写进代码：

```powershell
$env:TUSHARE_TOKEN = "你的token"
$env:TUSHARE_HTTP_URL = "http://8.136.22.187:8010/"
```

## 4. 全量初始化
```powershell
python scripts/data_ingestion/daily_bars/build_a_share_daily_db.py --init --start-date 20150101 --end-date 20260328
```

## 5. 日常增量更新
```powershell
python scripts/data_ingestion/daily_bars/build_a_share_daily_db.py --update --lookback-days 10
```

## 6. 常用参数
1. `--min-listed-days`：最少上市天数（默认 120）
2. `--min-amount`：成交额阈值（默认 20000000）
3. `--min-price`：最低收盘价（默认 3.0）
4. `--sleep-sec`：API 调用间隔（默认 0.2）
5. `--retries`：失败重试次数（默认 3）

## 7. 快速校验
可用 sqlite 工具检查：
```sql
SELECT COUNT(*) FROM dim_security;
SELECT MIN(trade_date), MAX(trade_date), COUNT(*) FROM fact_daily_bar;
SELECT trade_date, COUNT(*) FROM fact_universe_pti WHERE in_universe=1 GROUP BY trade_date ORDER BY trade_date DESC LIMIT 5;
```

## 8. 说明
1. `fact_universe_pti` 是点时股票池（当日可交易筛选），用于训练样本过滤。
2. `is_st` 基于 `fact_namechange` 的 ST 名称区间判断。
3. 若接口权限不足，脚本会在对应 API 调用报错并停止。

## 9. 本地保存 Token（推荐）
1. 运行初始化脚本（会写入 secrets/tushare.env 并收紧 ACL）
```powershell
powershell -ExecutionPolicy Bypass -File scripts/data_ingestion/daily_bars/init_tushare_secret.ps1
```

2. 脚本读取优先级
- `--token / --http-url` 参数
- 环境变量 `TUSHARE_TOKEN / TUSHARE_HTTP_URL`
- 本地密钥文件 `secrets/tushare.env`

3. 使用本地密钥文件运行
```powershell
python scripts/data_ingestion/daily_bars/build_a_share_daily_db.py --init --secret-file secrets/tushare.env --start-date 20150101 --end-date 20260328
```

