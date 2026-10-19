# elastic-workload-operator

基于 Go / controller-runtime 的指标驱动弹性编排实验。ElasticWorkload 管理两类 Deployment：先保存完整分配目标，再幂等收敛；工作负载未就绪或指标失效时暂停新决策。

## 学习与复现

[项目一完整教材：从零到工程实战](../投递档案/字节跳动_DataAML_机器学习系统调度编排工程师/项目一_从零到工程实战教材.md)

教材包含基础知识、当前 API 合约、源码逐步讲解、独立命名空间实验、故障注入、排障、练习答案和面试边界。阶段报告与旧设计说明已合并清理。

## 当前边界

- 本机真实联调伸缩 CPU 转发副本，共用外部单 GPU 引擎；训练为占位进程。
- status 保存目标是单对象提交，两个 Deployment 的更新与真实资源变化并不原子。
- 未实现真实 GPU 让渡、训练 Checkpoint、无损缩容、多租户 Quota 或多副本控制面高可用。
- prefixCaching=true 当前报不支持；没有 scale 子资源；初始副本与 GPU 单位不可变。
- CPU/内存 requests 需对称；默认 request 100m/64Mi、limit 500m/256Mi 为演示值。

## 入口与证据

- API：`api/v1alpha1/elasticworkload_types.go`
- 控制器：`internal/controller/`
- 当前实验部署：`config/validation/workloads.yaml`
- 有修改环境副作用的验收：`scripts/validate_cluster.py`
- 既有结果：`artifacts/cluster-validation.json`、测试与覆盖率日志。
- `config/demo/`、`config/samples/` 是历史/脚手架材料，不能替代教材当前完整示例。

此前 Go test/envtest/race 与 Kind 12 组检查通过；最新收尾记录 controller 语句覆盖率 86.7%，全仓 lint 仍有 44 条提示。历史记录不是此刻重新验证的状态。

## 运行原则

先读教材第 22—26 章；使用私有 `.state/validation.kubeconfig` 和显式 `kind-ew-recovery-lab` 上下文，不在默认业务 k3d 执行验收。测试涉及 envtest 资产，完整命令见教材。

修改 API 标记后通过 `make manifests` / `make generate` 重新生成，遵循本仓库 AGENTS.md。
