# tick_futures

国内期货只读行情采集、持久化、质量审计与本地归档。当前交付范围为**主力真实月份合约，每个源事件秒的最后一条快照**，保留量额、持仓和全部可用盘口字段。仅用 TqSim，不连接交易账户或下单。

**本机已停止行情接收与补采，两个项目 launchd 已卸载。后续本机只做代码开发和离线验证；GitHub 仅推送代码、schema、模板及交接文档，不上传行情或凭据。** Ubuntu/Debian x86_64 服务器自行采集，上线步骤见 [服务器交接文档](docs/SERVER_HANDOFF.md#L1)。服务器尚未实际部署，完整日夜盘验收为 PENDING。

每秒末快照是按用户选择的投影，秒内其他 Tick 会减少，不能称为原始 Tick 全保留；被选快照的字段、类型、浮点位模式严格可逆。无报价的秒不填前值、不伪造 Tick。SDK 快照、序号连续和归档恢复都不能证明上游零漏发。

## 开发与服务器接管

```bash
# 本机离线验证，无行情认证、无 SDK 连接
~/.venvs/hf/bin/python scripts/verify_offline.py
~/.venvs/hf/bin/python -m unittest discover -s tests -v
~/.venvs/hf/bin/python -m tick_futures.verify_samples  # 仅有原始样本的开发机
~/.venvs/hf/bin/python -m tick_futures --help
```

服务器使用独立 Python 3.13 环境和 editable 安装。必须先将**主机时区**设为 Asia/Shanghai，并确认 NTP 同步；仅设置进程 TZ 不够。`server-preflight` 在 Linux 上拒绝错误时区或未同步时钟，所有 systemd 日历触发也显式带 Asia/Shanghai；原始 epoch 不加减8小时。

接管命令包括 `prepare-seconds`、`server-preflight`、`server-units`、`service collector`、`status`、`overview`、`audit --scan-values`、`backfill`、`seal-seconds`、`restore-dataset`、`verify-db`。默认根目录为 `data/derived/main-seconds`，自定义目录在子命令前加 `--root PATH`。认证仅环境变量或显式 AST 配置，不能执行旧交易脚本。完整安装、状态、停用及卸载命令见交接文档。

采集进程持续运行，约200ms截止轮询；成功 FULL 提交后才推进源游标。保留每秒末快照不等于每秒只请求一次行情。心跳约5秒，主力映射复核约5分钟，维护每分钟，封存约每小时，07:30/19:30为启动兜底。午休及跨午夜维持连接。换月先加入新主力，新清单就绪后保留60秒旧月重叠；旧SDK对象到重连才释放，状态单独列出。

近期 Tick 窗口上限10,000行，实时缓冲选1,000行；断点以内容锚点核对，必要时单独请求10,000行。窗口按行计，不能由1Hz输出反推可安全每1/2/3小时离线下载；当前采用连续接收。故障超过源窗口会明确留下未知/不可补范围，不保证无条件“任何一秒不丢”。

100GB是数据总预算，包含热库、冷归档和工作副本：80GB告警、90GB暂停新增写入，留10GB工作余量；空闲磁盘不足5GiB也拒写。无自动历史删除，因此必须在达到水位前由服务器运维处理留存，不能无限期积累。短样本推算主力秒级净压缩负载约45MB/交易日，三倍情景135MB，**不含SQLite、索引和工作副本，也不是完整交易日实测**；旧估算见 [容量研究](docs/SECOND_SNAPSHOT_PLAN.md#L1)。

冷路径使用独立 Zstd9 无字典分片，逐片压缩/原始 SHA-256 和 TFREC 类型/位模式校验，并在本地空目录恢复。覆盖账本与归档完整性分别报告。历史远端上传/恢复模块保留，自动数据上传默认关闭；当前不执行行情 GitHub 上传或下载。

运行记录见 [PROJECT_CONTEXT.md](PROJECT_CONTEXT.md#L1)，证明范围见 [验收说明](docs/ACCEPTANCE.md#L1)，格式与恢复见 [恢复说明](docs/RECOVERY.md#L1)。此前870合约原型、六市场盘中测试、压缩基准与纯合成 GitHub 恢复都是历史验证，不能描述为当前运行状态或服务器验收。

## 原样本及历史验证

以下保留2026-11-02的样本和既有功能说明。原有71份样本/证据及既有特征代码不改写；历史全量热库和测试工作副本按用户要求清理，已校验的冷归档仍在本地保留，不向服务器或GitHub传输。在线 probe 示例仅说明旧原型用法，本机不再执行。

## 历史快速使用

在本项目根目录运行，复用本机已验证环境：Python 3.13.12、TqSdk 3.10.2、pandas 3.0.5、NumPy 2.5.2。依赖版本记录于 [pyproject.toml](pyproject.toml)。

```bash
cd ~/tick_futures

# 离线核验压缩样本与原始证据，完全不需要账号
~/.venvs/hf/bin/python -m tick_futures.verify_samples

# 用真实甲醇样本计算基础特征和异常标记
~/.venvs/hf/bin/python -m tick_futures.features \
  --input data/samples/20261102/CZCE.MA701/ticks.csv.gz \
  --quote data/samples/20261102/CZCE.MA701/quote.json \
  --output data/derived/MA701_features.csv.gz

# 在线验证当前主力：显式借用本机已有配置，只做 AST 读取，不执行旧代码
TQ_AUTH_CONFIG=../futures/scripts/test.py \
  ~/.venvs/hf/bin/python -m tick_futures.batch_probe \
  --products SHFE.au DCE.m CZCE.MA INE.sc GFEX.si CFFEX.IF \
  --observe-seconds 60

# 离线单元测试
~/.venvs/hf/bin/python -m unittest discover -s tests -v
```

在线认证优先读取 `TQ_USER`、`TQ_PASSWORD`，两者必须同时设置；也可显式指定 `TQ_AUTH_CONFIG`。项目不自动读取旧项目配置、不加载 `.env`，不包含认证值。已有配置仅作为可选认证来源，样本核验和特征计算完全独立。

采集代码：[单合约入口](tick_futures/probe.py#L1)、[多品种入口](tick_futures/batch_probe.py#L1)。使用 `TqSim`，禁用下单和撤单请求。多品种入口会查询 `KQ.m@交易所.品种`，再订阅当时有效的交割合约；单合约入口可通过 `--symbol` 指定已核实有效的月份合约。

## 已确认的数据能力

以下为 **2026-11-02** 的真实接口结果。新增八品种每个均取到 10,000 条 Tick，以及 600 根 1 秒线、200 根 5 秒线、200 根分钟线、100 根日线，共 80,000 条 Tick、8,800 根 K 线；螺纹钢来自另一个批次，额外保留 10,000 条 Tick 和 600 根秒线。

| 交易所 | 品种 | 当时交割合约 | 本批有效盘口 |
| --- | --- | --- | --- |
| SHFE | 黄金、铜 | `au2612`、`cu2612` | 五档 |
| DCE | 豆粕、铁矿石 | `m2701`、`i2701` | 一档 |
| CZCE | 甲醇 | `MA701` | 一档；成交额有异常 |
| INE | 原油 | `sc2612` | 五档 |
| GFEX | 工业硅 | `si2612` | 一档 |
| CFFEX | 沪深300股指期货 | `IF2612` | 一档 |
| SHFE | 螺纹钢，独立批次 | `rb2701` | 一档关键字段通过；此前未做同口径五档验收 |

| 数据维度 | 主要字段 | 口径 |
| --- | --- | --- |
| Tick 时间、价格 | `datetime/id/last_price` | 纳秒编码、SDK 序号、最新价；不代表逐笔或纳秒采样 |
| 当日统计 | `average/highest/lowest` | 当日统计，不是秒内统计 |
| 量额与持仓 | `volume/amount/open_interest` | 量额为当日累计，持仓为当前总量 |
| 盘口 | `bid/ask_price1..5`、`bid/ask_volume1..5` | 按有效值判断档位，不能只看列名 |
| K 线 | `open/high/low/close/volume/open_oi/close_oi` | 成交量为周期增量；本批无 K 线 `amount` 字段 |
| 合约信息 | `price_tick/volume_multiple/trading_time/expired` 等 | 合约乘数和交易时段用于量价处理 |

本轮日线最近 100 根覆盖 2026-06-05 至 2026-11-02，最新一根仍未收盘。**近期序列包含历史记录；当前不能使用指定任意起止日期的专业版下载接口。** [官方 API](https://doc.shinnytech.com/tqsdk/latest/reference/tqsdk.api.html)、[字段定义](https://doc.shinnytech.com/tqsdk/latest/reference/tqsdk.objs.html)

## 午休基线的频次与权限边界

| 项目 | 当前结论 |
| --- | --- |
| 序列长度 | 当前 API 与本机 SDK 每个 Tick/K 线序列最多 10,000 条；不是固定天数 |
| 接收方式 | 初始化订阅后复用引用，持续 `wait_update()`；不要在循环里反复登录或重建序列 |
| 行情频率 | 多数样本有约 500ms 间隔，也有抖动、休市长间隔和微小时间戳偏移；不能统一假定每秒两条 |
| 统一 QPS、带宽、连接上限 | 未确认公开具体数值；不能表述为无限频，未做限额压测 |
| 同时订阅规模 | 一个会话成功维护八个实际合约的 Tick 和 32 个 K 线序列；这不是账号并发上限 |
| 按日期历史下载 | 本账户 `DataDownloader` 实测被权限拒绝；其他专业版历史接口未逐一调用 |
| 实时能力 | 两批都在午休，观察窗口无新增 Tick；盘中新增、延迟、重连和全天稳定性尚未验收 |

`wait_update()` 返回次数不等于行情更新次数或请求 QPS。官方历史答复说明服务端存在请求限制，但没有给出可用于当前账号的配额。[更新机制](https://doc.shinnytech.com/tqsdk/latest/usage/framework.html)、[2026 年更新频率答复](https://forum.shinnytech.com/question/26530/answer/26531/)、[限流历史答复](https://forum.shinnytech.com/question/8204/answer/8205/)、[历史下载权限](https://doc.shinnytech.com/tqsdk/latest/reference/tqsdk.tools.download.html)

## 质量规则与基础特征

- 原始快照不改写。甲醇有 **4 次累计成交额回退**，对应 10:34:30、10:56:37、11:05:49 附近和 11:29:34；原因未确认，不能直接用于量额差分或区间 VWAP。
- 序号连续、字段非空只表示本批检查通过，不证明上游零漏数。普通 Tick 不能还原完整逐笔成交、主动方向、开平或撤单队列。[数据语义](https://www.shinnytech.com/articles/use-case/q73/0514-article/kuaiqi3-tick-vs-transaction-data-guide)
- Tick 量额需要差分，K 线成交量直接使用；首条、换合约、序号缺口、逆序/重复时间、长间隔及累计值回退都要标记。午休等时段的长间隔不能直接判为网络故障。
- K 线最新一根可能未完成；供应商有边界与补齐规则，自行重采样可能不一致。[K 线合成规则](https://www.shinnytech.com/articles/reference/why-our-klines-are-different)

[特征入口](tick_futures/features.py#L1) 提供中间价、价差、一档盘口不平衡、量额差分、持仓变化及快照区间 VWAP。它保留原始负差分，派生无效值使用 NaN，不排序、去重或补零；默认不跨超过 2 秒的间隔计算增量。该阈值可配置，属于工程规则，不是交易所规定。只剔除明显无效区间，尚不构成完整的数据清洗或策略模型。

## 文件组织与证据

```text
tick_futures/                 采集、特征与样本校验模块
tests/                        认证边界和数值语义测试
data/samples/20261102/         九个合约的 CSV.gz、行情截面及原始结果
data/raw/                     以后在线采集的输出，运行时生成
data/derived/                 本地特征输出
evidence/20261102/            两个原始批次的完整结果与复核
evidence/manifest.json        原路径到新路径、压缩前后 SHA-256
```

原有样本与证据约 **21.4 MB**，整理后约 **2.4 MB**。CSV 使用无损 gzip；原始 JSON 按字节保留。旧结果里的路径仍描述原项目，新的对应位置见 [迁移清单](evidence/manifest.json)。[样本校验入口](tick_futures/verify_samples.py#L1) 同时核验压缩文件和解压后原始内容散列。

完整证据：[八品种结果](evidence/20261102/batch_source_summary.json)、[质量复核及甲醇异常](evidence/20261102/batch_source_validation.json)、[螺纹钢结果](evidence/20261102/rb_source_summary.json)。未将原项目的交易脚本、账号密码或私有 SDK 日志带入本项目。

迁移验收：71 个样本/证据文件的压缩前后散列全部匹配，合计 90,000 条 Tick；12 项单元测试通过。甲醇离线特征计算复现 4 次成交额回退。新项目单合约入口另以螺纹钢复测，成功返回 10 条近期 Tick 和 600 根秒线；该次未观察实时新增。
