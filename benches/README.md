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

## 业界参考

- [hyperium/hyper](https://github.com/hyperium/hyper) — `cargo bench` + `github-action-benchmark` + gh-pages 存结果
- [mlpack/benchmarks](https://github.com/mlpack/benchmarks) — 独立 repo，YAML 配置驱动，SQLite 存历史
- [tokio-rs/tokio](https://github.com/tokio-rs/tokio) — `benches/` + criterion.rs，CI 回归检测
