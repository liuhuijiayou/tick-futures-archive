# 归档与恢复格式

## 当前：主力秒级快照、本地归档

用户已要求GitHub仅代码/文档，不再上传行情，也不在开发机下载行情。服务器新采数据保存在本地。历史远端模块保留，但自动数据上传默认关闭；下方旧远端示例不是当前部署步骤。

`seconds.sqlite3`保存每(symbol,源事件秒)所选的最后快照。当前候选可被同秒更新替换，原始TFREC字段、类型与位模式不变；已封存的迟到修订另记增量版本和旧原值。源checkpoint与候选同一FULL事务；收到ACK才推进接收端基线。历史补采导入不改变实时checkpoint。秒内其他Tick按用户选择减少，不能把该格式叫全原始Tick无损。

`seal-seconds`导出闭合秒（留2秒尾部）、观察来源/接收时间、增量质量/健康证据、版本化主力映射和schema。采用64MiB原始分片、独立Zstd9帧、无字典；每片压缩/原始SHA逐一核验，在本地空目录恢复后才推进exported_generation。审计变化也可产生零数据修订。已验证的临时staging/恢复目录清理，热候选和冷归档保留；没有自动删除历史数据。

```bash
.venv/bin/python -m tick_futures seal-seconds
.venv/bin/python -m tick_futures verify-db
.venv/bin/python -m tick_futures restore-dataset data/derived/main-seconds/archives /var/tmp/tick-empty-restore
.venv/bin/python -m tick_futures verify-seconds-file /恢复目录/archives/归档ID/seconds/snapshots.tfrec
```

目标必须为空，父归档必须齐全；恢复得到各版本的原始文件，保持版本顺序，**不会自动合并或覆盖先前修订**。seconds/snapshots.tfrec是所选源记录，observations.tfrec记录接收语义/来源/generation。缺片、坏片、错序、字典或编码版本不兼容均非零失败。所选快照的质量与采集覆盖分别审计；完整恢复不证明上游全量。

开发机已按用户要求清理热/测试工作副本；原样本以及曾空目录核验过的紧凑冷归档保留在data/derived/retained-evidence-20261102。没有将其上传或迁移到服务器。

## 历史：全Tick追加日志及GitHub合成恢复验证

下述为旧原型格式/能力说明；本机不再运行或上传数据。文中“全部副本保留”和“staging保留”描述清理前历史，现以用户授权清理记录为准。普通Git代码下载不等于行情恢复。Linux公开依赖锁已补入decoder/runtime-linux-x86_64-py313.json，但Linux执行验收仍PENDING，不能用Mac验证代替。


热库 `hot.sqlite3` 是追加日志。`records.raw` 按 [TFREC1 schema](../schema/tfrec-1.json#L1) 编码原始字段，`fingerprint` 是编码后、压缩前 SHA-256。Python/NumPy 类型、整数、NaN 载荷、Inf、负零、None/pd.NA/pd.NaT 和字段顺序可逆。`seq` 是本机持久化顺序；单次 SDK 窗口内保留返回顺序，跨合约未提供的原始网络交错顺序无法恢复。`received_ns` 是应用观察窗口时间，不是每条历史行情的网络到达时间。`event_ns` 与 `sdk_id` 仅作索引，原值仍在 BLOB。`observations` 保存每次投递的有序指纹，`events` 保留修订和质量标记。SQLite 完整性和 BLOB 逐条往返校验独立于压缩文件校验。

`seal` 以一致只读事务导出上次封存之后的 seq 范围到独立 SQLite journal，并保留元数据、schema 和审计。observations/events 增量保存，contracts/checkpoints/runs/gaps 保存完整状态快照；缺口状态变化有事件链，即使没有新 Tick 也生成审计修订归档。空数据分片的 `archive_context` 明确声明 checkpoint 所在父范围。热库不清理。`seal-index.json` 记录不可变归档及父归档；后续增量不能替代此前归档。每个归档都保留一个原始 staging 副本，计入本地磁盘成本。

冷分片采用标准带 checksum 的独立 Zstandard 帧，无私有字典，默认级别 9；每片最多 256 MiB 原始字节，压缩结果安全上限 1 GiB。`manifest.json` 是最后原子落盘的完成文件，含文件路径、顺序、原始/压缩 SHA-256 和大小、源/合约/行数/时间、版本、编码参数、schema 和依赖。各片无需其他片即可解码；缺片不能算批次恢复完成。

```bash
# 在项目根目录；以下使用统一入口
PY=~/.venvs/hf/bin/python
$PY -m tick_futures seal data/derived/manual-archive-001
$PY -m tick_futures verify-archive data/derived/manual-archive-001
$PY -m tick_futures restore data/derived/manual-archive-001 /private/tmp/recovered-001
$PY -m tick_futures verify-db /private/tmp/recovered-001/journal/journal.sqlite3
# 恢复目录下所有父归档链；存在多条无关链时显式 --leaf ARCHIVE_ID
$PY -m tick_futures restore-dataset data/derived/market/archives /private/tmp/recovered-market
```

恢复目标须为空。恢复只读 manifest 指定的资产，不回退到生产库、旧 checkpoint、原始 CSV 或缓存。少片、坏片、错误顺序、字典或版本返回非零。新归档没有查询副本；本地热库和 staging 均仍保留，因此磁盘成本需额外计入，不能仅展示冷文件大小。

GitHub 使用本人私有仓库、普通 Git 交付代码/schema/轻量审计，Release 交付数据。每个 Release 安全限制 900 资产，低于官方 1,000；每片也低于官方 2 GiB 限制。相同名字不同字节报冲突，不覆盖；上传每片后实际下载、解码、复核，manifest 与 `complete-<archive_id>.json` 最后发布。中断重试只上传缺片，但会复核已存在片。

```bash
# 真实行情仅在有 private off-host backup 授权证据后执行
$PY -m tick_futures upload ARCHIVE_DIR liuhuijiayou/tick-futures-archive TAG \
  --authorization /安全位置/backup-authorization.json

# 真正从 GitHub 下载资产并恢复，git clone 本身不恢复行情
gh repo clone liuhuijiayou/tick-futures-archive /private/tmp/recovery-code
cd /private/tmp/recovery-code
# 基础条件：macOS arm64、Python 3.13、gh 正常私有仓库认证
# 不使用生产 venv；依赖 wheel 从 GitHub 下载，在新 venv 离线安装
/opt/homebrew/bin/python3.13 scripts/recover_github.py \
  liuhuijiayou/tick-futures-archive synthetic-chain-audit-20261102 \
  /private/tmp/recovered-remote
```

授权文件需明确 `source: TqSdk`、`status: approved`、`private_backup: true`、`evidence_url`、`checked_at`；程序不会自动生成授权，也不会把仓库私有等同于数据权利。当前授权状态 PENDING。合成通路验收不构成真实行情远端恢复成功。

上述 tag 是纯合成验收集；真实行情在获授权上传后改成对应的 tag。基础解码依赖六个 wheel 共 16,505,168 字节，随 `decoder-runtime-v1` 交付；含可选 Parquet 的七个 wheel 共 47,659,474 字节。依赖锁、软件版本和 SHA-256 均在 GitHub，bootstrap 不调用 SDK、不读行情认证脚本，不使用 pip 网络或缓存。当前 bundle 针对本机平台，其他平台须另外构建和验证依赖包。

多个增量归档由 `restore-github` 根据完成清单的 `parent_tag` 自动递归下载，校验父 ID、循环、seq 连续关系与每片散列，再逐库恢复，保持原始 seq。bootstrap 继续对全部 journal 校验类型和位模式，包括零 Tick 的审计修订。当前没有自动合并/重写历史库的操作；数据分布在 `archives/<archive_id>/` 下。`restore-report.json` 是远端恢复报告，`dataset-restore-report.json` 是其本地解码阶段报告。归档可完整恢复而行情仍有缺口。

并行导入仍由单 writer 顺序提交，CPU 编码/校验可以并行完成。每个补采作业的原始 `hot.sqlite3` 与完成清单另作为 `backfill-jobs/<job>/journal.sqlite3` 封存一次；原始文件 SHA、源进程内跨合约 seq、接收时间和源审计均可恢复，不能只保留按合约导入后的顺序。bootstrap 也验证这些源库；报告的 canonical rows 与 preserved source rows 分开，后者属于原始投递证据副本，不能相加宣称采得更多唯一 Tick。额外原始源库和归档字节全部计入成本。

保留策略：原有样本/证据永不覆盖；当前热库、补采源日志、staging、冷分片和恢复验收副本全部保留，没有启用自动清理。只有完整冷归档与真实行情远端恢复都核验后，才具备讨论清理热文件的条件。磁盘低于 20 GiB 告警并暂停新冷任务，低于 5 GiB 拒绝持久化，checkpoint 不超前；上线前需监控本机容量。GitHub 的使用政策仍可能限制长期吞吐，不承诺无限存储或带宽。

平台与授权核查（2026-11-02）：[GitHub Release 配额](https://docs.github.com/en/repositories/releasing-projects-on-github/about-releases#storage-and-bandwidth-quotas)、[GitHub 使用政策](https://docs.github.com/en/site-policy/acceptable-use-policies/github-acceptable-use-policies)、[TqSdk 公开声明](https://www.shinnytech.com/pages/tq-disclaimer)。公开声明未提供本项目可据以确认私有异地行情备份范围的明确条款；未据此自行扩大授权。
