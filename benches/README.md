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

### CI 手动入口

`.github/workflows/benchmark.yml` 只在 GitHub Actions 页面手动触发（Actions → Benchmark (manual) → Run workflow）：

- `destination = none`（默认）：跑完上传 artifact，并把表格写进 workflow 的 job summary。
- `destination = issue` / `pr` 并填 `number`：额外把同一张表贴到指定 issue 或 PR。
- 本地和 CI 共用 `scripts/bench.sh`：本地 `just bench`，CI 直接调脚本，表格格式一致，区别只是机器环境。
- CI 表格不会自动提交进仓库；要把 CI 数字固化到文档，下载 artifact 覆盖 `benches/REPORT.md` 再提交。

CI 还会做两件事：

- **基线对比**：`target/criterion` 通过 `actions/cache` 跨 run 保存。每当次 run 起，报告的 `vs base` 列就是 Criterion
  自己算出的回归百分比；首轮显示 `-`。本地也可复现：先跑一次 `just bench`（产出 `base/`），之后每次跑都会带上对比列。
- **CPU 采样**：`scripts/bench-cpu.sh` 用 perf 采样一个 bench 进程，折叠栈交给报告脚本生成函数级热点表。
  这是尽力而为步骤：perf 权限或折叠工具不可用时，CPU 表留空并在日志里写原因，不影响主 benchmark。

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
