# tick_futures 工作约定

- 项目范围：国内期货 Tick 行情采集、持久化、无损归档、质量审计和恢复。保留已有特征功能，不继续开发因子、策略或回测。采集入口只用 TqSim，不接入交易账户或提交委托。
- 凭据仅来自环境变量或显式 TQ_AUTH_CONFIG；后者只允许 AST 读取 TqAuth 字面量，不能 import/执行旧交易脚本。不要复制凭据或原项目私有 SDK 日志。
- 数据时间使用 Asia/Shanghai，保留原始 datetime/id。普通 Tick 是快照；纳秒编码不代表纳秒采样，SDK 序号连续不代表上游零漏数。
- 2026-11-02 已验证 9 个品种共 90,000 条 Tick。新增 8 品种批次含 8,800 根 K 线；螺纹钢另有 600 根秒线。两次采集都在午休，尚无盘中实时新增、延迟或全天稳定性验收。
- 已知边界：每序列最多 10,000 条；统一 QPS、带宽及并发上限未确认；当前账户无指定日期历史下载权限。
- 黄金、铜、原油样本有五档，其余新测品种只有一档；甲醇有 4 次累计成交额回退。深度字段存在不等于有有效数据。
- data/samples 中的 gzip 数据与 evidence 原始证据不可覆盖清洗。派生输出放 data/derived，保留负差分和异常标记。
- 不自动排序、去重、补零或跨合约/长缺口差分；对资金流向、逐笔方向等只能作明确标记的推断。
- 代码位置使用仓库相对 Markdown 链接和 #L行号；当前解释器为 ~/.venvs/hf/bin/python。
- 常用离线检查：python -m tick_futures.verify_samples；python -m unittest discover -s tests -v。
- 新入口：python -m tick_futures --help；本机持久化根目录 data/derived/market；新原始记录采用 schema/tfrec-1.json，SQLite WAL/FULL（Mac fullfsync），records 不修改/删除，checkpoint 与原始 BLOB 同事务。
- 2026-11-02 下午已实际枚举 870 个 FUTURE/expired=False 合约（SHFE 238、INE 63、DCE 250、CZCE 243、CFFEX 28、GFEX 48）；不是交易所独立清单全量核对结论。实测 6 市场盘中 45 秒记录 60,323 条（初始化 60,000，后续更新 323）。全合约订阅扩展及完整日夜盘验收以 PROJECT_CONTEXT.md 和 reports 为准，不把历史缓冲算作实时覆盖。
- 普通 Git 不含样本、原始行情 JSON、热库、凭据与本机认证配置；本地历史证据仍全部保留。身份明确的私有 GitHub 目标为 liuhuijiayou/tick-futures-archive，真实行情异地备份授权尚 PENDING。不可用 synthetic 通路验收冒充真实行情已远端恢复。
- launchd 仅管理 com.liuhui.tick-futures.collector / maintenance；不改全局电源策略。离线超过窗口可能不可补，不能承诺零遗漏。当前全市场部署缓冲 1,000 行，允许窗口内补采扩大到 10,000；记录实际请求长度与起点。
- 冷归档独立 Zstd 9 无字典分片；恢复逐片验证压缩/原始 SHA-256，并校验记录类型/位模式。任何自动清理数据副本均禁用；低于 20 GiB 告警，低于 5 GiB 停写并保留 checkpoint。
