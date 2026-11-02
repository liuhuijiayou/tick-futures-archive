# 服务器交接：主力合约秒级快照

## 当前交付与边界

目标环境：Ubuntu/Debian、x86_64、systemd、CPython 3.13。GitHub 仅交付代码、schema、配置模板和文档；**不交付行情、数据库、归档分片、凭据或本机配置**。不要把 git clone 当作行情数据恢复。

按用户最新指令，本机真实行情接收、补采已经停止，两个项目 launchd 调度已经卸载。后续本机只运行离线测试，不承担明日采集。服务器未由开发机登录或实际启用；下面的服务器验收必须由接管者执行。

采集范围是供应商定义的每品种当前主力**真实月份合约**。2026-11-02 曾只读核对87个有效主力；数量来自当时接口，不保证永远87。`CZCE.LR601` 曾被主力接口额外返回，但元数据 expired=True，已排除并留审计。代码显式使用 TqSim，并禁止发送下单/撤单包。

输出为每合约、每个**源事件秒**的最后一条快照，全部返回字段保持原类型/位模式，包括量额、持仓和可用盘口；原始 datetime/id 不改写。秒内其余 Tick 是按用户要求有意减少的信息，因此不能称作完整原始 Tick。没有新报价的秒不伪造一条 Tick；活跃时段静默不能证明正常时留未知并告警。

本机曾验证主力订阅和秒级落盘/本地恢复；没有完成整个交易日及服务器端真实行情验收。SDK普通Tick是行情快照，不能证明交易所上游逐笔事件零遗漏。

## 1. 系统时区和时钟必须先对齐

系统、服务环境、交易日解释和盘前定时均显式使用 **Asia/Shanghai（UTC+08:00）**。设置时区只影响显示和日历解释，原始行情epoch不加减8小时。

```bash
sudo timedatectl set-timezone Asia/Shanghai
sudo timedatectl set-ntp true
export TZ=Asia/Shanghai
timedatectl status
timedatectl show --property=Timezone --property=NTPSynchronized
date --iso-8601=seconds
```

必须看到 `Timezone=Asia/Shanghai`、`NTPSynchronized=yes` 和本地时间 `+08:00`。如果服务器使用 chrony，应先按运维流程确认其正常同步。不要通过修改系统时钟数值来补时区差。

`server-preflight` 同时检查主机时区和进程时差，不能仅用 `TZ` 环境变量掩盖仍为UTC的主机。Linux未确认NTP同步时拒绝启动；维护任务也会复查。systemd所有日历触发器另带 `Asia/Shanghai`，不依赖默认时区。[官方 timedatectl](https://www.freedesktop.org/software/systemd/man/262/timedatectl.html)、[systemd时间规范](https://www.freedesktop.org/software/systemd/man/262/systemd.time.html)。

## 2. 下载代码并建立独立环境

使用有权访问私有仓库的正常GitHub认证，不复制开发机认证目录或token。

```bash
git clone git@github.com:liuhuijiayou/tick-futures-archive.git
cd tick-futures-archive
python3.13 -m venv .venv
.venv/bin/python -m pip install -e '.[benchmark]'
.venv/bin/python scripts/verify_offline.py
```

必须在checkout中以 editable 方式安装，保留 `schema/`、`decoder/` 和文档目录，服务工作目录也是该checkout。不要只复制一个Python文件。若系统还没有Python3.13，先按服务器运维流程安装；可使用现有 uv 的 `uv venv --python 3.13 .venv` 和 `uv pip install --python .venv/bin/python -e '.[benchmark]'`。全套测试含Parquet往返，故安装benchmark可选依赖。采集依赖版本见 pyproject.toml。不要把Mac的venv或macOS wheels复制到Linux。

离线单元测试不需要真实行情认证；代码中测试行情均为合成构造。入口禁止主/子进程IP连接，保留本地IPC；结果和源码散列写入本地reports目录，不上传。也可运行 `python -m unittest discover -s tests -v`。原有私有样本不在Git中，新的服务器不应把缺少这些原样本当成运行故障。

## 3. 配置本人行情认证

认证只来自 `TQ_USER`/`TQ_PASSWORD` 同时设置，或显式 `TQ_AUTH_CONFIG`。推荐独立AST配置文件，避免把完整旧交易脚本带到服务器。

```bash
mkdir -p ~/.config/tick-futures
chmod 700 ~/.config/tick-futures
```

用编辑器创建 `~/.config/tick-futures/auth.py`，仅填写一个字面量调用：

```python
# 仅供AST读取，不直接运行，不放入Git。
auth = TqAuth(user_name="YOUR_ACCOUNT", password="YOUR_PASSWORD")
```

```bash
chmod 600 ~/.config/tick-futures/auth.py
export TQ_AUTH_CONFIG="$HOME/.config/tick-futures/auth.py"
```

程序不会import/执行此文件，不接入真实交易账户。不购买订阅或使用多账号规避限制。按日期下载已知无权限，代码不依赖该功能。

## 4. 盘前准备

以下交易日是本次计划示例，后续须改成实际目标交易日。期货交易日不是自然日加一；例如2026-11-03的夜盘从11-02晚间开始。准备命令查询当前主力、校验真实未到期月份合约、保存元数据和有版本日历，并计算竞价前缓冲及跨午夜时段。

```bash
.venv/bin/python -m tick_futures prepare-seconds --trading-day 2026-11-03 --auth-config "$TQ_AUTH_CONFIG"
.venv/bin/python -m tick_futures server-preflight
```

数据根默认 `data/derived/main-seconds`。若指定其他目录，全程在子命令前加 `--root /绝对路径/专属目录`。使用本机磁盘，不把SQLite WAL库放到NFS共享盘。配置文件只记录认证文件路径，不含密码。

若准备发生在目标覆盖起点之后，初始回补只限实际可回溯窗口，不会自动补回早已消失的夜盘。当前SDK日历已验证边界为2026；跨2027前必须更新可审计日历/SDK并复核。节假日前夜盘证据不足时，不能靠自然日推算或宣称覆盖完整。

## 5. 安装systemd服务

以下命令以拥有checkout和认证文件的普通账号运行生成器，再用sudo安装系统服务。没有要求用户持续登录；系统服务会随服务器启动。

```bash
.venv/bin/python -m tick_futures server-units --output deploy/generated --user "$(id -un)"
sudo install -m 644 deploy/generated/tick-futures-*.service /etc/systemd/system/
sudo install -m 644 deploy/generated/tick-futures-*.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemd-analyze verify /etc/systemd/system/tick-futures-collector.service /etc/systemd/system/tick-futures-maintenance.service /etc/systemd/system/tick-futures-maintenance.timer /etc/systemd/system/tick-futures-preopen.timer
sudo systemctl enable --now tick-futures-collector.service tick-futures-maintenance.timer tick-futures-preopen.timer
sudo systemctl status --no-pager tick-futures-collector.service
sudo systemctl list-timers --all 'tick-futures-*'
```

采集服务先执行时区/NTP/磁盘预检，异常退出后30秒退避再拉起，项目锁阻止同一数据根重复实例。不要以每天/每小时重启充当健康检查。

## 6. 触发频率与运行周期

| 工作 | 周期/触发方式 |
| --- | --- |
| 行情接收 | 一个持续运行进程；行情更新驱动，wait_update截止轮询约200ms |
| 保存粒度 | 每源事件秒的最后一条完整快照；候选和源checkpoint同事务FULL提交 |
| 心跳 | 约5秒，包含队列、待确认、订阅和静默状态 |
| 主力/元数据复核 | 约5分钟；新增主力加入，换月保留明确衔接 |
| 维护/空间检查 | 独立进程，每分钟 |
| 封存/校验 | 约每小时，独立Zstd9分片；字段位模式检查及本地空目录恢复 |
| 盘前启动兜底 | 上海时间07:30、19:30；已运行服务继续运行 |

午休、午夜和夜盘尾段都不退出。各品种实际时段、竞价缓冲和尾段从元数据/日历解释，不能统一按15:00结束。

不建议改成1/2/3小时离线下载。Tick窗口最多最近10,000条，不是固定小时。本地降为1Hz不会延长源窗口。以2条/秒仅作算例，1,000条约8分20秒，10,000条约83分20秒；2/3小时会越窗，1小时还需初始化、重试和重叠余量。真实峰值未完成整日认证，所以当前实现用连续接收。

正常实时缓冲1,000行，重启重新取得窗口并核对内容锚点；窗口不重叠会登记未知/待补，并由单一受锁子进程尝试公开10,000行窗口。失败不购买权限或伪造数据。窗口外不能恢复的范围明确登记，不能以SDK序号连续替代上游完整性证明。

## 7. 运维、预算与恢复

```bash
.venv/bin/python -m tick_futures status
.venv/bin/python -m tick_futures overview
.venv/bin/python -m tick_futures audit --scan-values --output reports/local/server-coverage.json
.venv/bin/python -m tick_futures backfill --symbols SHFE.au2612
.venv/bin/python -m tick_futures seal-seconds
.venv/bin/python -m tick_futures verify-db
sudo journalctl -u tick-futures-collector.service --since today --no-pager
sudo journalctl -u tick-futures-maintenance.service --since today --no-pager
```

补采示例合约须换成当前主力；未指定合约时只排查尚未处理的窗口事件。补采文件先校验散列，经同一个持久化写线程导入，成功取窗/导入与“覆盖完整”分别记录。历史导入不移动实时checkpoint。订阅就绪数量与实际有新报价的合约数要分别检查；夜盘不会有全部日盘品种的新报价。

100GB为十进制**数据总预算**，包含热库、归档、工作副本和本项目证据。80GB告警，90GB暂停新增写入并留下10GB工作余量；磁盘不足5GiB也拒写，20GiB以下停止新增冷任务/补采。属于应用层保护，不是操作系统分区quota；外部复制文件仍需运维管控。

当前不自动删除唯一历史数据，也不自动清理已采秒快照。达到暂停水位会中断新增采集，故不能把100GB理解为无限期保留且永不维护。主力秒级净记录流曾按短样本推算约45MB/交易日，三倍情景约135MB；热SQLite、索引、修订、元数据和临时文件须另外计入，完整日夜盘尚未实测。上线后以实际增长重新确定留存/迁出策略，在80GB告警前处理。

本地恢复必须使用空目录，读取全部父归档，不能只取最后一个分片：

```bash
.venv/bin/python -m tick_futures restore-dataset data/derived/main-seconds/archives /var/tmp/tick-futures-empty-restore
.venv/bin/python -m tick_futures verify-seconds-file /恢复目录/archives/归档ID/seconds/snapshots.tfrec
```

恢复逐片验证压缩/原始SHA-256，schema和编码版本；缺片、损坏、顺序/字典或版本不兼容均非零退出。TFREC1保留NaN/空值/浮点特殊位模式；源SDK实际返回的id/datetime曾为float64，系统不能恢复SDK之前已丢失的纳秒精度。量额是源累计值，异常只标记。

**不上传数据到GitHub。** 历史上传/恢复模块保留但默认不启用。仓库此前的软件依赖和纯合成实验资产不是生产行情备份。服务器自己采新行情；仅拿代码无法恢复开发机过去的行情。若另需迁移存量，由数据所有者另行选择合法渠道。

## 8. 接管验收与停用

上线后检查：系统与进程时区正确、NTP=yes；单实例服务启动；当前主力清单无未解释缺失；实际订阅/失败列表；队列和消费延迟；断线/重启重叠是否核验；一份真实本地封存能在空目录恢复。最后在服务器完成包含相应夜盘和日盘的完整交易日账本审查，才可以标记该范围端到端通过。

当前开发机不再承担采集，因此不能依赖开发机补服务器启动前的数据。网络、供电、磁盘或行情源中断超过可补窗口会留下不可补/未知区间；代码不可能无条件保证交易所上游“任何一秒都不丢”。状态必须据实报告，不填前值伪造Tick。

停用且禁止开机再启动：

```bash
sudo systemctl disable --now tick-futures-collector.service tick-futures-maintenance.timer tick-futures-preopen.timer
sudo systemctl stop tick-futures-maintenance.service
```

卸载时仅删除以上四个项目unit并 `sudo systemctl daemon-reload`；数据和认证文件不自动删除。升级时先停项目服务、git pull、重新安装editable依赖、运行离线测试和预检，再启动；确认checkpoint和重叠核验，不能以“进程启动成功”认定行情完整。
