# 归档与恢复格式

热库 `hot.sqlite3` 是追加日志。`records.raw` 按 `schema/tfrec-1.json` 编码原始字段；`fingerprint` 是编码前 SHA-256。`seq` 是本机到达/持久化顺序，`event_ns` 与 `sdk_id` 仅作索引，原值仍在 BLOB。`observations` 保存重复窗口的有序指纹，`events` 保留修订和质量标记，`gaps` 与源覆盖有关。SQLite 完整性和 BLOB 逐条往返校验独立于压缩文件校验。

`seal` 以一致只读事务导出上次封存之后的 seq 范围到独立 SQLite journal，并保留元数据、schema 和审计。热库不清理。`seal-index.json` 记录不可变归档及父归档；后续增量不能替代此前归档。每个归档都保留一个原始 staging 副本，计入本地磁盘成本。

冷分片采用标准带 checksum 的独立 Zstandard 帧，无私有字典，默认级别 9；每片最多 256 MiB 原始字节，压缩结果安全上限 1 GiB。`manifest.json` 是最后原子落盘的完成文件，含文件路径、顺序、原始/压缩 SHA-256 和大小、源/合约/行数/时间、版本、编码参数、schema 和依赖。各片无需其他片即可解码；缺片不能算批次恢复完成。

```bash
# 在项目根目录；以下使用统一入口
PY=~/.venvs/hf/bin/python
$PY -m tick_futures seal data/derived/manual-archive-001
$PY -m tick_futures verify-archive data/derived/manual-archive-001
$PY -m tick_futures restore data/derived/manual-archive-001 /private/tmp/recovered-001
$PY -m tick_futures verify-db /private/tmp/recovered-001/journal/journal.sqlite3
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
$PY -m tick_futures restore-github liuhuijiayou/tick-futures-archive TAG \
  /private/tmp/recovered-remote --archive-id ARCHIVE_ID
```

授权文件需明确 `source: TqSdk`、`status: approved`、`private_backup: true`、`evidence_url`、`checked_at`；程序不会自动生成授权，也不会把仓库私有等同于数据权利。当前授权状态 PENDING。合成通路验收不构成真实行情远端恢复成功。

多个增量归档按 `parent` 和 seq 范围恢复；应逐归档下载并逐库校验，保持原始 seq。当前没有自动合并/重写历史库的操作。恢复报告保留源覆盖状态，归档可完整恢复而行情仍有缺口。

平台与授权核查（2026-11-02）：[GitHub Release 配额](https://docs.github.com/en/repositories/releasing-projects-on-github/about-releases#storage-and-bandwidth-quotas)、[GitHub 使用政策](https://docs.github.com/en/site-policy/acceptable-use-policies/github-acceptable-use-policies)、[TqSdk 公开声明](https://www.shinnytech.com/pages/tq-disclaimer)。公开声明未提供本项目可据以确认私有异地行情备份范围的明确条款；未据此自行扩大授权。
