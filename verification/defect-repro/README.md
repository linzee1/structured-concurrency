# 缺陷复现 harness

复现 `reports/defect-analysis-2026-09-10.md` 报告的五个缺陷。只用公开 API，不进入 `src/`，因此不影响
`mvn test`。

```bash
./verification/defect-repro/run.sh
```

- **退出码 0**：五条全部复现（即当前版本确有这些缺陷）。
- **退出码 1**：至少一条未复现——说明该条已被修复或报告需要修正；harness 会打印每条的状态。

## 复现项

| Check | 缺陷 | 稳定性 |
|---|---|---|
| 1 | `VariableLinkedBlockingQueue.drainTo` 遇抛异常的 target：count 漂移、元素丢失、后续 poll/take 抛 NPE | 必现 |
| 2 | `setCapacity` 缩容到 `count > capacity` 后 `clear()`/`drainTo()` 不唤醒阻塞的 `put` | 必现 |
| 3 | `SlidingWindowSubmitter` 的交接窗口：元素报 `SUBMISSION_FAILURE` 而其 callable 已执行 | 竞态（强制交接窗口下约 1-2/10 轮；其余轮次取消级联先取消引擎 future） |
| 4 | deadline 在提交前已过期仍进入用户 callable（且组 outcome 可能是 `SUCCESS`） | 用户代码进入必现（20/20），outcome 视竞态 |
| 5 | 失败成员的组 outcome 随完成顺序在 `MEMBER_CANCELED`/`USER_FAILURE` 间漂移 | 必现 |
| 控制项 | 健康组（含 terminal combine）仍报 `SUCCESS` | 必现（证明 harness 不会把正常路径误报为缺陷）|

第 3、4 条是竞态，harness 会打印多轮统计而不是单次结果；单次运行的比例波动属预期。
注意第 3 条的退出码是概率性的：10 轮中若竞态窗口一次都未命中（矛盾 0 轮），该条会报
NOT REPRODUCED、整体 exit 1，这不代表缺陷不存在——多跑几次或结合打印的轮次分布判断。
第 5 条的判定同时要求"失败者先完成 → USER_FAILURE"与"失败者最后完成 → MEMBER_CANCELED"
两个方向都观察到，单向结果不算复现。
