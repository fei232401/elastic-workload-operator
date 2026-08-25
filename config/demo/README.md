# L2 真集群验证(mock 数据面)

本机 WSL/k3d 无 GPU,真 vLLM 起不来 → **mock 数据面**验证控制面闭环
(replicas 该涨该跌对得上;不验推理吞吐——诚实边界)。

## 复现步骤

```bash
# 1. 建集群(带 daocloud 镜像源,WSL 直连 docker.io 会超时)
k3d cluster create ew-cluster --servers 1 --no-lb \
  --registry-config config/demo/registry.yaml

# 2. 导入 mock 镜像 + 装 CRD + 交 demo
k3d image import mockrouter:latest -c ew-cluster
kubectl apply -f config/crd/bases/aiinfra.example.com_elasticworkloads.yaml
kubectl apply -f config/demo/l2-demo.yaml

# 3. 打通数据面 + 起算子(本地 out-of-cluster,免构建算子镜像)
kubectl port-forward svc/router 18080:8080 &
go run ./cmd/main.go --leader-elect=false --metrics-bind-address=0 \
  --health-probe-bind-address=0 -router-url=http://localhost:18080
```

mock 脚本曲线(相对 mockrouter 进程启动):
`<20s=150ms(死区) → <100s=600ms(过载让渡) → <140s=120ms(健康归还) → 回稳 150ms`。
manual 覆盖:`curl 'localhost:18080/control?ttft=600'` / `?auto=1` 回脚本。

## 本次验证结果(2026-08-25)

| 阶段 | ttft | 动作 |
|---|---|---|
| 死区 | 150ms | 不动 |
| 让渡 | 600ms>SLO | inference 2→3→4, training 4→3→2→1(触 min 停) |
| 归还 | 120ms<0.6×SLO | inference 4→3→2→1, training 1→2→3→4(触 min 停) |

min_hold=2 ticks 严格生效(每步间隔 10s,非 5s tick 乱动);scalingHistory 5 条 +
Event 5 条正确;死区 clamp 停表。

## 已知局限(要点)

**apply 两段式写非原子**:apply 先写 training 再写 inference,并发 reconcile 下
可能 training 成功 + inference 冲突 → 总卡数丢 1 单位且不恢复(L2 实测总数 6→5,
让渡/归还仍守恒但基数已偏)。修法候选:
- apply 内冲突重试(写失败读最新重试,re-derive target);
- 或声明式总和不变量(desired_total = spec.training.replicas + spec.inference.replicas),
  收敛到不变量而非增量守恒;
- 防同 CR 并发 reconcile。
此问题不草率落地,连同一起处理。
