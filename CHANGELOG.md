# Changelog

本文件记录项目的显著变更。

## [Unreleased]

### Changed

- `gateway_cache::CacheKey` 的内部表示由 hex 编码的摘要 `String` 改为两个 32 字节摘要（`[u8; 32]`）。
  旧实现计算完 SHA-256 后还要做一次 hex 编码，键的哈希与相等比较都落在两倍长度的字符串上；
  新实现直接保存原始摘要字节，消除了 hex 编码的分配与长字符串比较开销。

  这是一次纯粹的内部表示变更：公开类型名与使用方式完全不变——`Clone` / `Hash` / `Eq` / `Debug`
  派生、`response_key()` 的签名、`ResponseCache::store` / `lookup` 以及调用方持有的 `Option<CacheKey>`
  均未改变。字段原本就是 `pub(crate)`，外部代码无法直接构造或读取内部值，因此本次变更对只通过
  `gateway-facade` 依赖本库的消费者完全透明，无需任何迁移。
