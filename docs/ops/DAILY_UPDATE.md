# 每日数据更新（Daily Update）

本文档用于指导你每天更新数据库中的 **日K线/复权因子/指数/估值数据**，并说明如何查看日志与排查问题。

## 前置条件

- 已启动数据库容器（ClickHouse + PostgreSQL + Redis）
- 已创建 Python 虚拟环境并安装依赖（`.venv/` 存在）
- 已在 `.env` 中配置好数据库密码与 Tushare token（如已完成初始化可忽略本条）

项目根目录（本文档约定）：

- `/home/lcw/quantitative_trading_projects`

## Web 运维界面

通过浏览器操作：起停容器、每日更新、历史回填、查看日志。

### 启动

```bash
cd /home/lcw/quantitative_trading_projects
./ops.sh web
```

或指定端口：

```bash
./ops.sh web 8502
```

首次使用需安装依赖：`pip install streamlit`

浏览器访问：`http://127.0.0.1:8501`

---

## 一键执行（推荐）

项目已提供一键脚本：`run_daily.sh`

### 执行“更新今天”

在任意目录执行：

```bash
/home/lcw/quantitative_trading_projects/run_daily.sh
```

### 通过总控脚本执行（推荐）

如果你希望把“起容器/每日更新/回填/看日志”都统一到一个入口，使用 `ops.sh`：

```bash
cd /home/lcw/quantitative_trading_projects
./ops.sh daily
```

### 执行“更新指定日期”（可选）

日期格式：`YYYYMMDD`

```bash
/home/lcw/quantitative_trading_projects/run_daily.sh 20260304
```

## 手动执行（可选）

如果你想直接运行 Python 脚本（不经过 `.sh`）：

```bash
cd /home/lcw/quantitative_trading_projects
source .venv/bin/activate
python scripts/daily_update.py
```

## 定时执行（crontab）

目标：在 **交易日收盘后** 自动执行（建议 15:35）。

### 查看当前定时任务

```bash
crontab -l
```

### 编辑定时任务

```bash
crontab -e
```

推荐配置（工作日 15:35 运行）：

```cron
35 15 * * 1-5 cd /home/lcw/quantitative_trading_projects && ./ops.sh daily
```

说明：
- `1-5` 表示周一到周五（节假日不是交易日，脚本会自动跳过或写入“非交易日”提示）
- 服务器时区建议为 `Asia/Shanghai`（你当前已是该时区）

## 日志获取与查看

### 日志文件位置

每日更新日志统一写入：

- `/home/lcw/quantitative_trading_projects/scripts/daily_update.log`

### 查看最近 50 行

```bash
tail -50 /home/lcw/quantitative_trading_projects/scripts/daily_update.log
```

### 持续跟踪（实时滚动）

```bash
tail -f /home/lcw/quantitative_trading_projects/scripts/daily_update.log
```

也可以通过总控脚本跟踪日志：

```bash
cd /home/lcw/quantitative_trading_projects
./ops.sh logs daily
```

## 本次每日更新会写入哪些表

- **ClickHouse**
  - `stock_daily`：全市场日K线 + 复权因子 + 清洗后字段（如停牌/涨跌停标记等）
  - `index_daily`：指数日线（示例：上证、沪深300、中证500）
- **PostgreSQL**
  - `daily_valuation`：每日估值（`pe_ttm/pb/ps_ttm/total_mv/circ_mv`）

## 常见问题

### 1) 找不到日志文件

第一次成功执行后才会生成日志文件。先运行一次：

```bash
/home/lcw/quantitative_trading_projects/run_daily.sh
```

然后再查看：

```bash
tail -50 /home/lcw/quantitative_trading_projects/scripts/daily_update.log
```

### 2) 15:35 没有自动跑

检查三件事：

1. cron 服务是否运行：

```bash
systemctl status cron
```

2. 是否配置了 crontab：

```bash
crontab -l
```

3. 看日志是否有执行记录（若没有，通常是 cron 没触发或权限问题）：

```bash
tail -200 /home/lcw/quantitative_trading_projects/scripts/daily_update.log
```

### 3) Tushare 报权限/频率限制

这属于数据源侧问题：
- token 配置是否正确
- 账户权限/积分是否满足接口要求
- 是否触发限频（可稍后重试）

建议先手动执行一次确认错误信息完整写入日志，再针对性处理。

