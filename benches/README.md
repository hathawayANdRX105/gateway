# benches/

Gateway 压测工具。测 `gatewayd` 数据面本身（gate / dispatch / forward 各层延迟、吞吐、错误率）；上游模型只负责返回一次 200。

## 前置条件

1. **启动 gatewayd**（默认监听 `127.0.0.1:8080`）：

   ```bash
   cargo run --release -- --config config/gateway.toml
   ```

2. **替换 catalog 里的占位上游**：`config/catalog.toml` 的 `[[channels]]` 默认指向
   `https://api.example.com`、`secret = "sk-upstream"`，不替换的话每个请求都会在
   forward 阶段失败，压测只会得到错误率。把 `base_url` / `secret` / `[[channels.models]]`
   换成真实上游；gateway 自己不产生回复。

3. **对齐 token**：默认 catalog 的 `[[tokens]].key = "sk-replace-me"`，与
   `benches/data/scenarios.yaml` 的 `global.api_key` 一致。改 token 时两处一起改，
   或让 `global.api_key` 指回 catalog 的值。

4. **模型别名必须存在**：场景里的 `model` 必须是 catalog 中某个 group 允许、且
   某个 channel 暴露的别名。默认 catalog 只有 `gpt-4o`；加模型时同时改
   `[[groups]].allowed_models`、`[[channels.models]]` 和 `[[prices]]`。

## 语料

提问语料在 `config/prompt-templates.jsonl`，每行：

```json
{"id": "...", "source": "...", "name": "...", "stream?": true, "messages": [{"role": "user", "content": "..."}]}
```

`benches/bench_gateway.sh` 的 `random_prompt` 随机抽一条，并用场景里的
`model` / `stream` 覆盖语料里的同名字段——语料只提供问题，不绑定部署的模型别名，
也不决定是否走 SSE。语料不进编译，改完直接重跑即可。

## 目录

```text
benches/
├── README.md           # 本文件
├── bench_gateway.sh    # 压测脚本（路径相对脚本自身，可从任意目录调用）
├── data/
│   └── scenarios.yaml  # 场景定义 + 速率控制 + global
└── results/            # gitignore，原始结果 + 分析

config/
├── gateway.toml             # gatewayd 进程配置
├── catalog.toml             # 渠道 / 分组 / 用户 / token / 价格（压测上游在这里）
└── prompt-templates.jsonl   # 提问语料
```

## 使用

```bash
# 列出场景
./benches/bench_gateway.sh

# 运行单个场景
./benches/bench_gateway.sh baseline

# 运行所有启用的场景
./benches/bench_gateway.sh all
```

`global.gateway_url` 默认 `http://127.0.0.1:8080`；gatewayd 换了端口时同步改这里。
`global.gateway_log` 可选：gatewayd 默认把日志写到 stdout，重定向到文件后填路径，
分析结果才会附带日志条目统计；留空则跳过日志分析。

## 场景

| 场景 | 描述 | RPS | 并发 | 模型 |
|---|---|---|---|---|
| baseline | 低并发基线 | 2 | 5 | `gpt-4o` |
| medium_load | 中等并发 | 5 | 20 | `gpt-4o` |
| multi_model | 多模型轮换（当前脚本只取 `models[0]`） | 3 | 15 | `gpt-4o` |
| streaming | 流式请求（SSE） | 2 | 10 | `gpt-4o` |
| stress | 压力测试（默认关闭） | 10 | 50 | `gpt-4o` |

`multi_model` 的轮换需要按请求切换 body 里的 model；当前 `hey` 单 body 的实现只取
第一个别名，保留该场景是给后续按模型分组压测留的入口。

## 结果

结果存 `benches/results/<scenario>_<timestamp>/`：

- `hey_output.csv`：hey 原始 CSV（序号, 延迟秒, 状态码, 响应大小）
- `request_body.json`：本次实际使用的请求体
- `analysis.json`：p50/p95/p99、吞吐、状态码分布、错误率
- `gateway_log_before/after.log`：配置了 `global.gateway_log` 时的日志快照

## 防封号

- 控制 RPS（不是跑满）
- 遇到 429 时脚本会按 `abort_on_429` 停止场景
- 多模型轮换需自行把 `models` 配成真实存在的别名

## 依赖

- `hey`：`go install github.com/rakyll/hey@latest`
- `jq`
- `python3` + `PyYAML`（`python3 -c 'import yaml'` 能通过）

## Rust 微基准与性能报告

端到端压测回答“整条数据面能扛多少”，微基准回答“哪个具体函数的单次调用最贵”。
两类互补，都不进 CI；`just check` / `just clippy` 带 `--all-targets`，会编译 bench target。
bench 文件集中在本目录，各 crate 用 `[[bench]] path = "../../benches/<name>.rs"` 认领自己的 target，
所以归档只在一处，编译归属和 crate 名仍保留。
根包设了 `autobenches = false`，否则 Cargo 会把本目录的 `.rs` 当成 gateway-daemon 的 bench。

| crate                   | bench 文件                                                            | 覆盖点                                                          |
| ----------------------- | --------------------------------------------------------------------- | --------------------------------------------------------------- |
| gateway-cache           | `benches/cache_ops.rs`                           | `response_key`、TtlCache get/insert、ResponseCache store/lookup |
| gateway-security        | `benches/word_filter.rs`                      | `WordFilter::filter` / `has_match`、`StreamFilter::push`        |
| gateway-metering        | `benches/metering_ops.rs`                     | token 估算、prompt 估算、流式扫描、价格算术、usage 提取         |
| gateway-dispatch        | `benches/selector_latency.rs`、`state_ops.rs` | P2C 选择、限流、sticky、t-digest、延迟分位、token 预算          |
| gateway-protocol-bridge | `benches/codec_ops.rs`                 | codec resolve/translate、SSE 解码、错误映射                     |
| gateway-forward         | `benches/pipeline_ops.rs`                      | 请求体/模型名/请求头纯函数、状态码分类                          |
| gateway-proxy           | `benches/proxy_ops.rs`                           | SSRF 校验、IP 检查、节点元数据与 adapter、pool/manager 选择     |

命令：

```bash
just bench                                        # 跑全部微基准
cargo bench -p gateway-cache --bench cache_ops     # 跑单个 bench 文件
just bench-report                                 # 从 Criterion JSON 重建 benches/REPORT.md
just bench-report --perf-folded benches/perf-folded.txt   # 附上 CPU 采样函数表
```

### 哈希候选对比

`benches/hash_candidates.rs` 对比响应缓存键可用的摘要算法，输入与 `response_key` 的
字节布局完全一致（每个字段 8 字节大端长度前缀 + body）。算法、语料与正确性断言
集中在 `benches/support/hash_candidates.rs`，bench 与测试共用同一份，口径不会漂移。

覆盖 SHA-256、BLAKE3（256/128）、XXH3-128、XXH3-64、SipHash-2-4-128、ahash、foldhash。
`just bench` 后报告会多出 `## Hash candidate throughput` 一节（MiB/s，本机相对值），
`vs base` 与 CPU 采样同样适用。

正确性与一致性由 `crates/gateway-cache/tests/hash_consistency.rs` 保证，样本量 2048：

| 断言 | 覆盖 |
| --- | --- |
| 摘要宽度与确定性 | 每个候选按声明的字节数输出，重复调用一致 |
| 2048 个不同请求零碰撞 | 每个候选 2048 个互异摘要 |
| 隔离边界不碰撞 | 单字节差异、`user+model` 拼接陷阱、换 user/group/协议 |
| SHA-256 匹配 NIST 向量 | 证明 harness 喂的就是预期字节 |
| 跨进程稳定性策略 | ahash/foldhash 标记为不可作共享缓存键 |

`just test-fast` 会一并运行。要加新候选：在 `Algorithm::ALL` 注册、实现 `digest`
并声明 `is_cross_process_stable`，bench 与测试自动覆盖。

### 响应缓存命中路径成本

`benches/cache_ops.rs` 在 store / lookup 之外补了三个口径，服务一个待决策问题：
要不要给缓存键加一道廉价预筛（先用更便宜的摘要排除不可能命中的请求，再走完整的
`response_key`）。三个 bench 都在 `cache_ops` group 里，沿用既有 group 的
sample_size / measurement_time / warm_up_time：

| bench id            | 测什么                                                |
| ------------------- | ----------------------------------------------------- |
| `cache_lookup_miss` | 用一个从未入库的 key 查找，纯 miss 路径的成本         |
| `cache_lookup_hit`  | 用一个已在库的 key 查找，纯 hit 查找的成本            |
| `cache_hit_path`    | `response_key()` + 命中查找，一次缓存命中的端到端成本 |

`cache_hit_path` 是唯一省不掉完整键派生的场景（命中了就必须拿到 key），所以它是
预筛新增开销的上限参照；`response_key_<size>` 三个 bench 给出不同 body 大小下的键派生
成本，和这三个口径合起来就能判断预筛是否划算。

命中率由 `ResponseCache::stats()` 的 hits / misses 计数器维护（`gateway-cache` 每次
get 自增），进程侧通过 `Gateway::response_cache()` 窄端口把同一个 `Arc` 接出来，
在 `/readyz` 的 `responseCache` 字段里暴露 `enabled` / `hits` / `misses`——
预筛是否值得落地，就由这个累计命中率决定。接线前，先用
`cargo bench -p gateway-cache --bench cache_ops` 的成本数据推进预筛决策。

### 预筛探针

`benches/prefilter_probe.rs` 测量「给缓存键加廉价预筛是否值得」。它实现了**修正后的**
设计：二级索引 `quick_fp -> full_key`，而不是原方案里「未命中就跳过 SHA-256」——
后者是错的，write-through 缓存在未命中后仍要算全量 key 才能存入新响应。

CI 干净 runner（AMD EPYC 7763，有 SHA-NI）实测：

| body | 现状 sha256_only | 命中省下 | 未命中多付 | 盈亏平衡命中率 |
| --- | ---: | ---: | ---: | ---: |
| 512 B | 444.5 ns | 401.7 ns | 35.4 ns | 8.1% |
| 4 KiB | 2432.4 ns | 2273.0 ns | 151.2 ns | 6.2% |
| 16 KiB | 9173.1 ns | 9011.8 ns | 154.0 ns | 1.7% |
| 32 KiB | 18176.9 ns | 18018.2 ns | 163.7 ns | 0.9% |

盈亏平衡命中率 = 未命中多付 /（未命中多付 + 命中省下）。低于该命中率时预筛净亏。
本机（i5-9300H，无 SHA-NI，负载 6.9）测出的符号会翻转，只能看 CI 数据。

**未计入的成本**：每次 store 都要写索引、索引与缓存双份内存、淘汰需要两处协调。
所以「盈亏平衡命中率只有 1-8%」不等于「值得做」——还要命中率真的那么高，且能接受
多维护一份结构的复杂度。

### CI 手动入口

`.github/workflows/benchmark.yml` 只在 GitHub Actions 页面手动触发（Actions → Benchmark (manual) → Run workflow）：

- `destination = none`（默认）：跑完上传 artifact，并把表格写进 workflow 的 job summary。
- `destination = issue` / `pr` 并填 `number`：额外把同一张表贴到指定 issue 或 PR。
- 本地和 CI 共用 `scripts/bench.sh`：本地 `just bench`，CI 直接调脚本，表格格式一致，区别只是机器环境。
- CI 表格不会自动提交进仓库；要把 CI 数字固化到文档，下载 artifact 覆盖 `benches/REPORT.md` 再提交。

CI 还会做两件事：

- **基线对比**：`target/criterion` 通过 `actions/cache` 跨 run 保存。每当次 run 起，报告的 `vs base` 列就是 Criterion
  自己算出的回归百分比；首轮显示 `-`。本地也可复现：先跑一次 `just bench`（产出 `base/`），之后每次跑都会带上对比列。
- **CPU 采样**：`scripts/bench-cpu.sh` 用 callgrind 对 bench 进程做用户态插桩，产出函数级热点表
  （cost = 指令数 Ir，与延迟列互补）。callgrind 不需要内核 perf 权限，适合 GitHub hosted runner；
  `Cargo.toml` 的 `[profile.bench]` 保留符号供 `callgrind_annotate` 反查函数名。
  这是尽力而为步骤：valgrind 不可用或采样失败时，CPU 表写明原因，不影响主 benchmark。
  本机若有 perf 权限，也可用 perf 采样，报告对两种来源的成本行都兼容。

`benches/REPORT.md` 是提交进仓库的分析表：每行数值来自 Criterion 写出的
`target/criterion/<group>/<function>/new/estimates.json`，脚本不估算、不造数。
把新机器的结果贴进 PR / issue 时，重新跑 `just bench` 即可。

### 找 CPU 热点函数

Criterion 给出“哪个函数慢”，CPU 采样给出“这个函数里哪行最贵”。
采样工具在本机不可用：`perf`、`flamegraph`、`valgrind` 均未安装，
且 `/proc/sys/kernel/perf_event_paranoid = 2` 限制非特权 perf 事件。
在有权限的机器上：

```bash
cargo bench -p gateway-cache --bench cache_ops -- --profile-time 20
# bench 运行期间另开终端：
perf record -F 99 -g -p <bench-pid> -- sleep 20
perf script | stackcollapse-perf.pl > benches/perf-folded.txt
just bench-report --perf-folded benches/perf-folded.txt
```

报告会新增按采样数排序的函数表（函数、样本数、占比），数值来自折叠栈计数。
Rust 原生替代是 `pprof` crate，同样依赖 perf_event 权限；LTO 下函数可能被内联，
读采样表时结合 Criterion 的 bench ID 一起看。

## 业界参考

- [hyperium/hyper](https://github.com/hyperium/hyper) — `cargo bench` + `github-action-benchmark` + gh-pages 存结果
- [mlpack/benchmarks](https://github.com/mlpack/benchmarks) — 独立 repo，YAML 配置驱动，SQLite 存历史
- [tokio-rs/tokio](https://github.com/tokio-rs/tokio) — `benches/` + criterion.rs，CI 回归检测
