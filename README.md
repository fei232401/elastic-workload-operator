# elastic-workload-operator

> **Kubernetes Operator：在共享 GPU 集群上做训练 ↔ 推理的弹性资源让渡。**
> 一个 `ElasticWorkload` 自定义资源同时声明训练与推理两个工作负载，控制器读取推理侧 TTFT P95，
> 按规则引擎做渐进式 GPU 让渡与归还——高峰保护推理 SLO，回落后完整归还训练。

`Go 1.26` · `controller-runtime` · `Kubernetes ≥1.28` · `19 Go 单测 + envtest` · `k3d 端到端验证`

---

## 1. 它解决什么问题

训推混部的共享 GPU 集群里，训练任务长期占卡、推理流量按潮汐起伏。当推理高峰来临：

- **不动**：推理排队，SLO（TTFT P95）被击穿；
- **硬抢占**：训练被打断、checkpoint 反复回滚，振荡且代价更高；
- **本方案（渐进让渡）**：训练按档位逐步让出 GPU，推理 SLO 恢复，流量回落再逐档归还。

难点不在"让几张卡"，而在**让渡的决策如何落到 Kubernetes 的声明式模型上**——这正是本仓库做的事。

## 2. 架构

```
                    ┌──────────────────────────────────────┐
                    │   ElasticWorkload  (单个 CRD)          │
                    │   spec: training + inference + 弹性策略 │
                    └──────────────────┬───────────────────┘
                                       │ watch
                    ┌──────────────────▼───────────────────┐
                    │   Controller (Reconcile 主循环)        │
                    │                                       │
                    │   observe  ← 推理侧 TTFT P95 (/metrics)│
                    │      │                                │
                    │   decide   ← 规则引擎(纯函数, 可单测)   │
                    │      │        + min_hold 防抖闸门      │
                    │   commit   ← 原子写 status.desired*    │
                    │      │                                │
                    │   apply    ← 朝目标收敛(幂等)           │
                    └──────────────────┬───────────────────┘
                                       │ patch
                    ┌──────────────────▼───────────────────┐
                    │  Training Workload    Inference Workload│
                    │  (replicas 逐档收缩)   (replicas 逐档扩张)│
                    └──────────────────────────────────────┘
```

## 3. 三个关键设计

**① 单 CRD 管两个工作负载。** 训练与推理的副本数存在同一个对象的 `status` 里，因此"让渡"是一次**原子提交**，而不是两次互相看不见的更新。

**② 声明式目标收敛，而不是增量修补。**
决策输出的是**目标分配**（`DesiredTraining` / `DesiredInference`），`commit()` 单次 `Status().Update` 原子写入，`apply()` 再朝已提交目标收敛。
已到位即 no-op、未到位补上 —— 幂等，部分失败重试**不叠加漂移**。
> 这是从一版"两段式写"的 bug 修出来的：先写训练、再写推理，中间失败会丢卡（6→5）。
> 声明式收敛后，意图一旦提交就不会漂移，副本总数守恒。

**③ 防抖（min_hold）。** 指标抖动会导致让渡/归还反复横跳（振荡）。`min_hold` 闸门要求一次变更后保持 N 个周期才允许下一次决策。

**④ 与规则引擎解耦。** 决策规则以**纯函数**形式实现（`internal/controller/scaling.go`），零 K8s 依赖 —— 因此可以脱离集群做单元测试，也让规则本身可以被独立替换。

## 4. 验证分层

| 层 | 内容 | 覆盖 |
|---|---|---|
| L1 | `go test ./...` —— **19 个单元测试** | 决策翻译、步长钳制、min_hold 闸门、finalizer、GPU 资源限额、metrics 解析 |
| L1+ | **envtest**（真实 kube-apiserver + etcd） | Reconcile 全链路：收敛幂等、部分失败重试、事件记录 |
| L2 | **k3d 真集群 + mock 数据面** | 让渡/归还/防抖/钳制/事件/history/finalizer 级联删除 |

```bash
# 单元测试（不含 envtest，无需集群）
go test ./internal/... ./api/...

# 全量（envtest 需先安装 kubebuilder 资产）
make test

# L2：k3d 真集群 + mock 数据面闭环（步骤见 config/demo/README.md）
```

## 5. 目录结构

```
api/v1alpha1/elasticworkload_types.go   CRD Go types（status 含 desiredReplicas，CEL 校验）
internal/controller/
  ├── elasticworkload_controller.go     Reconcile 主循环：observe → decide → commit → apply
  ├── scaling.go                        决策规则（纯函数，零 K8s 依赖）
  ├── decide_controller.go              读 metrics + min_hold 闸门
  ├── apply_controller.go               commit（原子提交）+ apply（朝目标收敛）
  └── metrics_router.go                 HTTP 读取推理侧 /ttft_p95
cmd/main.go                             入口（注入 router URL）
config/crd/bases/                       生成的 CRD
config/demo/                            L2 演示清单 + 复现步骤
test/                                   mock 数据面 / e2e 骨架
```

## 6. 诚实边界

- **推理数据面在 L2 中是 mock**（脚本化的 TTFT 曲线），不是真实 vLLM 端点。真实端点需要另做 exporter/适配。
- **未实现预测式调度**：本仓库是反应式的。在"决策 → patch → kubelet 生效"的反馈回路时间常数未压缩之前，预测带来的提前量会被吃掉。
- **守恒口径**：保证的是**副本数**守恒。若要 GPU 总量守恒，需要 `gpuPerWorker == gpuPerInstance`；两者不等时是总量守恒而非 GPU 守恒。

## 7. 技术栈

Go · controller-runtime · Kubernetes CRD/CEL · envtest · k3d · Prometheus metrics
