# ElasticWorkload Operator 设计文档(训推弹性调度平台)

> **版本**:v0.1 | **日期**:2026-08-25 | **状态**:设计定稿,待建设
> **定位**:Kubernetes-native 训推共存弹性调度平台——算法层(算法)的 K8s 平台落地,数据面(KV 路由)的容器。

---

## 0. 一句话

> 一个 Kubernetes Operator:用户声明一个 `ElasticWorkload` CR(**训练 + 推理两个工作负载 + 弹性策略**),controller 读推理 TTFT P95 → **复用算法层 Rule1-5** 决策 → 把 `{scale_training, scale_inference}` 应用到两个工作负载。**KV 路由器(数据面)是推理侧数据面。**

---

## 1. 为什么是这个形态(三个决策)

### 1.1 为什么是训推共存,不是纯 serving operator

算法层的本体就是**训推共存**(Training↔Inference 弹性分配)。一个只管推理的 operator 只是"KV 穿 K8s 外衣",掉进 **KServe / KEDA / Knative** 的拥挤赛道——它们都做"扩缩容 serving"。

训推共存是**无人区**:TTFT-SLO 驱动的**跨训推弹性分配** + KV 路由数据面,没有现成项目做这个。对比 KEDA+vLLM metrics:KEDA 只扩缩 serving,不做训练/推理之间的分配决策。

### 1.2 为什么单 CRD(ElasticWorkload),不是两个 CRD

训练和推理的 GPU 配额**是一个调度域**——弹性分配必须在两者之间**原子决策**。拆成两个 CR 要跨 CR 协调(状态共享/竞态);一个 CR 让决策单点落地。

生命周期差异(训练是长时批任务、推理是常驻服务)由 controller **内部两条 reconcile 路径**处理,互不阻塞,不体现在 API 层。

> 补充说明:"训练任务跑完、CR 删了,推理会受影响吗?"→ 不会,reconcile 按工作负载类型分路;ownerReference 只级联它自己的 Pod。

### 1.3 为什么不用 Volcano/Kueue 原生方案

| | Volcano / Kueue | 本项目 |
|---|---|---|
| 配额管理 | ✅ 内置 | ✅ **集成 Kueue**(stretch) |
| Gang 调度 | ✅ 内置 | ✅ 复用 Kueue/KubeScheduler |
| **SLO 驱动的跨训推弹性分配** | ❌ 不做 | ✅ **核心能力** |
| 训练进度感知的缩容 | ❌ | ✅ 算法层 §12(slowdown 闸门) |

**结论**:Kueue 处理"配额",本项目处理"SLO 驱动的弹性分配决策",两者互补。本项目的全部新价值 = **决策层**,不是重复造配额轮子。

---

## 2. CRD 定义

```yaml
apiVersion: aiinfra.example.com/v1alpha1
kind: ElasticWorkload
metadata:
  name: llm-platform-001
  namespace: ml-team-a
spec:
  # ===== 训练侧 =====
  training:
    replicas: 4
    minReplicas: 2
    maxReplicas: 8
    gpuPerWorker: 1
    image: nvcr.io/nvidia/pytorch:24.07-py3
    command: ["torchrun", "train.py"]
  # ===== 推理侧(数据面 = KV 路由 + vLLM 实例)=====
  inference:
    replicas: 2
    minReplicas: 1
    maxReplicas: 4
    gpuPerInstance: 1
    image: vllm/vllm-openai:latest
    prefixCaching: true          # 前缀缓存 = KV 数据面的核心
  # ===== 弹性策略(算法层 Rule1-5 的配置)=====
  scalingPolicy:
    targetMetric: ttft_p95       # 目标指标(ms)
    slo: 300
    minHoldDuration: 24          # tick(@5s = 120s)
    maxScaleStep: 1              # 每决策周期最多 ±1(渐进)
status:
  phase: Running                 # Pending/Running/Deleting
  readyReplicas:
    training: 4
    inference: 2
  conditions:
    - type: Admitted
      status: "True"
  scalingHistory:
    - time: "2026-09-01T10:00:00Z"
      action: "inference+1/training-1"
      reason: "overload"
      metric: "ttft_p95=412ms"
```

### CEL 校验(CRD schema x-kubernetes-validations)

- `minReplicas <= replicas <= maxReplicas`(训练/推理各自)
- `minHoldDuration >= 0`、`maxScaleStep >= 1`
- `slo > 0`、`targetMetric` 枚举白名单

### subresources

- `status: {}`(controller 写 status)
- `scale`(specReplicasPath 指向训练或推理的 replicas → `kubectl scale` 可直接用)

---

## 3. Controller 架构:四步 reconcile

```
observe → decide → apply → report
  │          │        │        │
读取 CR      算法层     改 K8s    status/conditions/
实例 Pod     Rule1-5   replicas  事件
TTFT P95    →ResourceDecision
```

### 3.1 observe

- 读 `ElasticWorkload` CR
- 读两个工作负载的 Pod 列表,算 readyReplicas
- 读推理侧 TTFT P95(Router metrics 端点)

### 3.2 decide(复用算法层,零改动思想)

`ElasticWorkload` 的弹性决策**直接复用** `elastic-scheduler-v2` 的 `ElasticScheduler.step` / Rule1-5:

- **Rule1 过载**(TTFT P95 > SLO)→ 推理 +1,训练 −1(同一 ResourceDecision 输出两边)
- **Rule3 回归**(TTFT P95 < 0.6×SLO)→ 推理 −1,训练 +1
- **min_hold / max_scale_step 防抖**——算法层已实证:**渐进 ±1 + min_hold = 零振荡**;hard 全量×双向 = 极限环(Pareto 支配)
- **§12 训练进度闸门**——训练 slowdown 超限时禁止继续让渡

**不是重写算法,是把已验证的决策函数翻译到 K8s 控制器。** 决策函数的正确性由算法层 135 tests 覆盖;本项目的测试目标是"翻译是否正确"。

### 3.3 apply

- 训练侧:改 Deployment/Job 的 replicas(或经 Kueue Workload podset replicas)
- 推理侧:改 vLLM 实例的 StatefulSet/Deployment replicas + 通知 Router 成员变化

### 3.4 report

- 写 status(readyReplicas/phase/conditions)、发 Event、记 scalingHistory

### 3.5 生命周期

- **Finalizer**:删除 CR 时回收数据面(删 KV Router Deployment + 关停 vLLM 实例、清 Kueue Workload),避免 GPU 悬挂
- **OwnerReference**:实例 Pod 级联删除(删 CR → 自动回收)

---

## 4. 数据面(复用数据面,零移植)

- **KV Router**(`kv_aware` 路由,Python 原样跑)作 Deployment——请求按 KV 亲和 + 负载打分路由
- **vLLM 实例**作 StatefulSet/Deployment,开启 prefix caching
- Router 暴露 **TTFT P95 metrics** → controller 读取 → 决策输入
- **不需要把路由逻辑移植成 Go**——控制面只读 metrics,数据面保持 Python。Go 代码量 = 纯控制器机制。

数据面已验证的证据直接继承:4090 校准(util=0.35 多实例近免费、contention 1.03)、真机闭环(prefix caching 抹平路由分化)、链式闭环(elastic+kv_aware 78.7%→6.1%)。

---

## 5. 分层验证(诚实分层,复用已有证据)

| 层 | 验证什么 | 怎么做 |
|---|---|---|
| **L1** | 控制器翻译正确 | Go 单测:Reconcile 创建/删除、伸缩决策、min_hold、finalizer、RBAC。**决策算法本身不重测**(算法层 135 tests 已覆盖) |
| **L2** | 真机闭环 | **k3d(WSL2 + RTX 4070 8GB)**:真 vLLM 实例(Qwen2.5-1.5B,复用 util=0.35 校准结论),static vs elastic 实测 SLO 违约率 |
| **L3** | 万级平台翻译 | **复用算法层 L3**(KWOK 10k + Kueue + 决策引擎零改动)——"平台翻译正确"已证明,不重复 claim |

> 诚实分层:**L3 证明"决策引擎能翻译到 K8s 正确"(算法层做的);L2 证明"真实 serving 数据面 + 真机 SLO"(本 operator 做的)。两个数字分开讲,不混。**

---

## 6. 实操路线(每步 = 一个 K8s 知识点)

| 步 | 做什么 | 学的 K8s 知识点 |
|---|---|---|
| 1 | kubebuilder 脚手架 + CRD types + CEL 校验 | API 约定 / apiextensions.k8s.io |
| 2 | Reconcile:创建/删除实例 Pod | informer/watch、ownerReference、status 子资源 |
| 3 | 缩容/删除时资源回收 | **finalizer 语义**(做对才是真会) |
| 4 | RBAC + Router metrics 接线 | 权限模型、自定义指标 |
| 5 | k3d 真集群 static vs elastic | 真机闭环对比 |
| 6 | (stretch) Kueue 配额 + Gang | 多租户 / 批量调度 |

**实操定位**:以"机制做对"为验收,不追求生产级(5 阶段 Operator 那套交给成熟开源当范例)。每个机制 = 一个 K8s 知识点。

---

## 8. 诚实边界(stretch 一律标 future work)

1. **L2 单卡 4070 实测**:多卡缩放仍是算法层 EMULATED 假设,不声称真实多卡
2. **1.5B 模型**:7B/更大未测(先撞显存)
3. **预测未做**:算法层 Experiment E / L3 predictive 已证"执行延迟不压缩时预测反噬"——所以本 operator **先做反应式**,预测 = future work
4. **KWOK 万级复用算法层 L3**,本 operator 不重复 claim
5. **Kueue 配额 = stretch**(先直管 GPU,后接配额隔离)
6. **训练侧弹性语义**:真实弹性训练(elastic-job 缩容改世界大小)vs 仿真降速——本 operator 先用算法层已验证的"让渡 + 恢复"语义,L2 只证调度正确性

---

## 9. 与两项目的关系(一句话)

> "算法层证明算法有效(L3 验证平台翻译),数据面提供推理数据面(KV 路由),本 operator 把两者焊在 K8s 上——训推共存弹性调度:算法 → 平台 → 数据面,三层证据,一条脊柱。"
