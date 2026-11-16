# arch/skill — 出图工具与出图纪律

这个目录放两样东西：**能重跑这些图的工具**，和**防止图画错的检查清单**。

## archify/ —— 出图引擎（vendored 3.2.0）

从 `~/.agents/skills/archify` 复制进来，砍掉了 `examples/*.html` 渲染产物（13M → 360K）。
`node bin/archify.mjs doctor` 通过，可直接用：

```bash
node arch/skill/archify/bin/archify.mjs finalize architecture <candidate.json> <out.html> \
  --quality showcase --repo-root /home/hathaway/projects/gateway --json
```

依赖：Node ≥18、一个 Chromium（`finalize` 的 browser-check 门需要）。
本机已把 Playwright 的 Chromium 链到 `~/.local/bin/google-chrome`，自动检测即可。

为什么 vendored 而不是依赖全局安装：**图要能在这个提交上重跑**。
全局 skill 会升级，升级后同一份 candidate 可能被新规则拒绝，历史图就再也生成不出来。

## 出图纪律（这次踩过的坑）

`archify/references/reading-a-codebase.md` 是通用版，`../impl/CODEBASE-READING.md` 是同一份。
核心是六条：

1. **先普查端口再画实现** —— 画「谁做决定」的节点前，必须列出该 trait 的**全部生产实现**、
   选择点（`match` / `with_selector`）、扩展点。
2. **选择策略的枚举本身是架构的一部分** —— `SelectorKind` 要画进流程，不能只画实现。
3. **找端口之上的第二层决策** —— 优先级分层、限流重抽都在策略之外运行。
4. **一个实现是断言，两个实现才是架构** —— 只画一个就会把局部当整体。
5. **门禁通过 ≠ 图是对的** —— `finalize` 只证明格式合法、几何不重叠、引用可解析。
6. **普查表要边做边写** —— 见 `../impl/PORT-CENSUS.md`。

## 这次的实例

`Selector` 有 `P2CSelector` 与 `WeightedRandom` 两个实现，由 `SelectorKind` 在装配期选择。
第一版图只画了 P2C，标题写「P2C 循环」——**门禁全过，图是错的**。
修订后 07 图从 `SelectorKind → wire_signals → SelectorSlot → {P2C, WeightedRandom, 自定义}` 画起。

同一次普查还捞出三处遗漏：`PriceTable` 的 `CatalogPriceTable`（daemon 侧）、
`Egress` 的 `AdapterEgress`（协议拨号）、两个仅测试使用的 `Stage` 实现。