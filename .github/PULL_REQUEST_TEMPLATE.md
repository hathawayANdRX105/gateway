<!--
填写规范：
- PR 标题可以使用自然的英文描述，不要求固定前缀；类型通过 GitHub label 区分。
- PR 正文结构使用英文 heading，正文内容使用中文。
- 一个 PR 对应一个主要 Issue；使用 `Fixes #N` 关闭主 Issue，使用 `Related #N` 或 `Part of #N` 关联更大的工作。
- 不要直接推送或合并到 `main`。
- diff 必须聚焦，所有变更路径都必须能由主 Issue 解释。
- 不要包含密钥、真实配置、生产环境信息、生成垃圾或无关格式化。
- 只有适用时才复制额外区块，不要发布空的额外标题。
-->

**开始前：** 从当前 `main` 创建分支，搜索已有 Issue 和 PR；每个 PR 只解决一个主要结果；验证步骤必须让评审者可以复现。

## What

<!-- 合并后会发生什么变化？正文请用中文填写。 -->

## Why

<!-- 为什么这样做：不这么做的代价是什么。正文请用中文填写。 -->

## Issue

<!-- 主 Issue：使用 `Fixes #N`，合并后自动关闭。正文请用中文填写。 -->

Fixes #

<!-- 可选：关联或父 Issue。 -->

Related #

## Construction plan

<!-- 实际完成的最小实现或文档步骤，正文请用中文填写。 -->

- [ ]

## Delivery record

<!-- 完成后填写，正文请用中文填写。 -->

- Delivered:
- Verification:
- Follow-up: none | #

## How to test

<!-- 评审者可以执行的命令或手动步骤，正文请用中文填写。 -->

1.

```text
# commands run, if any
```

## Checklist

- [ ] 已使用 `Fixes #N` 关联一个主 Issue。
- [ ] 已使用正确的英文类型 label：`bug`、`enhancement` 或 `chore`。
- [ ] 已添加适用的区域 label：`contract`、`cache`、`dispatch`、`forward`、`gate`、`metering`、`pipeline`、`protocol`、`proxy`、`security`、`facade`、`tests`、`ci` 或 `documentation`。
- [ ] 没有包含密钥、真实配置、生成垃圾或无关变更。
- [ ] diff 只服务于关联的主 Issue。
- [ ] 已执行上面列出的测试或手动验证；如果不适用，已说明原因。
- [ ] Rust 改动已执行 `just verify`（fmt / clippy / check / test-fast）。
- [ ] 依赖方向红线未被破坏（engines → contracts，facade 只被消费方依赖；`app-contract` 保持 wasm-safe）。

<!--
===========================================================================
推荐额外区块：只有适用时才复制完整区块。

## Screenshots
适用场景：仅靠 diff 无法验证的 UI/UX 变化（本仓是纯库，通常不适用）。

## Risk
适用场景：迁移、数据结构变化、难以回滚的行为或安全敏感路径。
- Risk: low / medium / high
- Migration or configuration:
- Revert by reverting this PR? yes / no

## Changelog
适用场景：`gateway-facade` 表面或 contract 类型的公开 API 变化（这是发布库，
破坏性变化必须写迁移说明给下游消费方）。
-

## Notes for reviewers
适用场景：非直观边界、刻意延后的工作或希望评审者重点检查的内容。
-
===========================================================================
-->

## Review record (closeout hard rule)

<!-- 收尾审查后填写。每条发现一行, 不许静默漏项; 驳回要写理由 -->

| 发现 | 来源              | 严重度          | 处置                   |
| ---- | ----------------- | --------------- | ---------------------- |
|      | gate/CRG/模型评审 | BLOCK/WARN/INFO | 修@commit / 驳回: 理由 |

- [ ] 全部 BLOCK/WARN 已处置（修复或书面驳回），零未跟踪项
- [ ] 工作树无残留：worktree、agent 分支、临时文件已清
- [ ] 无残留进程/端口占用
