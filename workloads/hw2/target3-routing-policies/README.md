# 作业二目标三：四副本在线路由负载

本目录提供固定 workload 和 HTTP/SSE 流量发生器，用于比较 Ray Serve 在四个 SGLang 后端上的路由效果。实验保持模型、后端、请求内容和到达时间不变，只调整 Ray Serve 配置或请求路由器。

建议先复现 Ray Serve 默认配置，再调整公开参数，最后尝试扩展或自定义请求路由器。重点观察服务质量、负载分布和前缀缓存复用之间的关系。

## 快速开始

流量发生器需要 Python 3.10 或更高版本。进入本目录后安装客户端依赖，并先校验固定负载：

```bash
python -m pip install -r requirements.txt
python validate_workload.py mooncake_prefix_workload_v2_seed2026.jsonl
```

准备一个可访问的 Ray Serve 入口后即可回放负载：

```bash
python run_workload.py \
  --policy serve \
  --run-name p2c_default \
  --base-url http://127.0.0.1:8000 \
  --workload mooncake_prefix_workload_v2_seed2026.jsonl \
  --max-in-flight 2048 \
  --output-dir results/p2c_default
```

服务端接口约定和完整对照流程见下文。

## 文件

| 文件 | 作用 |
| --- | --- |
| `mooncake_prefix_workload_v2_seed2026.jsonl` | 已生成到达时间的课程固定负载，可直接回放 |
| `mooncake_prefix_families_v2_seed2026.jsonl` | 同一批请求，不含到达时间，可用于重新设计流量阶段 |
| `run_workload.py` | 发送流式请求并记录逐请求指标 |
| `validate_workload.py` | 检查请求数、前缀关系、到达序列和文件摘要 |
| `compare_runs.py` | 汇总多个实验配置的差异 |
| `sample_prefix_families.py` | 从 Mooncake trace 复现前缀族采样 |
| `make_arrivals.py` | 生成固定的 Poisson 到达序列 |

## 负载规模

数据来源为 [Mooncake FAST'25 tool-agent trace](https://github.com/kvcache-ai/Mooncake/blob/main/FAST25-release/traces/toolagent_trace.jsonl)。固定负载包含：

| 项目 | 数值 |
| --- | ---: |
| 前缀族 | 64 |
| 预热请求 | 64 |
| 测量请求 | 2048 |
| 输入 token 总量 | 3,233,280 |
| 请求输出 token 总量 | 268,831 |
| 单请求输入长度 | 1,216-1,600 tokens |
| 单请求输出长度 | 16-190 tokens |

64 个前缀族分为 1 个超热、7 个热、24 个中等热度和 32 个冷前缀族。每个前缀族至少共享 1024 个 token。超热前缀族包含 768 条测量请求，每条生成 190 tokens，用来形成持续热点；其余请求分散在不同前缀族中。

测量请求使用三段固定到达序列：

| 阶段 | 请求数 | 平均到达速率 |
| --- | ---: | ---: |
| steady | 512 | 40 req/s |
| burst | 1024 | 120 req/s |
| recovery | 512 | 60 req/s |

计划发送时间约 29.54 秒。该规模足以在四个课程小模型后端上形成可观察的排队，同时仍适合在单机实验中反复运行。

文件摘要：

```text
Mooncake source SHA-256:
48a2db1a13d3bc05e6330140c64f604ba366df20d3c9e128b5c35a01c1fa5f71

families v2 SHA-256:
54fc4f69b065dbc8e832275ff9e51d58a9cc7df91376b201210ec6c92c822742

workload v2 SHA-256:
f154ba954e28ca9612257edf5deb0d5dfc6bf27fd0115a701e1f49e52df75b03
```

## 数据说明

Mooncake trace 提供每个 512-token 块的 `hash_ids`，不提供原始 Prompt token IDs。采样脚本保留 trace 中的前缀相等关系和输出长度，并生成适合课程模型的确定性 token IDs：

1. 每个 trace hash block 映射为 128 个 token，最多保留 12 个块。
2. 相同 `hash_id` 始终映射为相同 token 序列。
3. 同一前缀族至少共享前 8 个块，即 1024 个 token。
4. 每条请求追加 64 个独立后缀，避免整条输入完全相同。
5. 为形成不同热度，脚本会确定性地重复选取同族 trace 记录；每次回放使用独立后缀。
6. 超热族从固定采样候选中按 trace 平均输出长度选出，使单副本热点同时具有持续 Decode 压力。

因此它是 **trace-derived workload**：前缀关系和生成长度取自公开 trace，Prompt token IDs 和到达时间由课程脚本生成。热点比例是路由压力条件，不代表 Mooncake 原始业务的请求比例。

## 服务要求

准备四个相同配置的 SGLang 服务，每个 Ray Serve Replica 固定转发至其中一个服务：

```text
Client -> Ray Serve HTTP proxy -> Replica 0..3 -> SGLang backend 0..3
```

默认 token 生成范围为 `4096-64095`，可直接用于课程指定的 Qwen3 模型。使用词表较小的其他模型时，应通过 `--token-base` 和 `--token-span` 缩小范围，并在所有对照中保持一致。

Ray Serve 入口需透传 SGLang 的流式 `POST /generate` 响应。为统计请求分布，建议在响应中加入：

```text
X-Ray-Replica-ID
X-Ray-Node-ID
X-SGLang-Backend
```

流量发生器会为每个请求发送 `X-Session-Id`，其值为前缀族 ID。自定义路由器可读取这个请求头，但不应读取未来请求或修改 workload。

## 运行对照实验

每轮实验前清空四个 SGLang 后端的 Radix Cache，再启动当前 Ray Serve 配置。流量发生器先顺序发送 64 条预热请求，然后按固定到达时间发送 2048 条测量请求：

```bash
python run_workload.py \
  --policy serve \
  --run-name p2c_default \
  --base-url http://127.0.0.1:8000 \
  --workload mooncake_prefix_workload_v2_seed2026.jsonl \
  --max-in-flight 2048 \
  --output-dir results/p2c_default
```

修改 Ray Serve 配置或路由器后，使用不同的 `--run-name` 和输出目录回放同一文件。例如：

```bash
python run_workload.py \
  --policy serve \
  --run-name improved \
  --base-url http://127.0.0.1:8000 \
  --workload mooncake_prefix_workload_v2_seed2026.jsonl \
  --max-in-flight 2048 \
  --output-dir results/improved

python compare_runs.py \
  --baseline p2c_default \
  --run p2c_default=results/p2c_default/summary.json \
  --run improved=results/improved/summary.json \
  --output results/comparison.json
```

## 结果检查

每个配置会生成 `warmups.csv`、`requests.csv` 和 `summary.json`。报告至少检查：

1. 2048 条测量请求全部成功，并完整生成指定 token 数。
2. 四个 Replica 和四个 SGLang 后端均收到请求。
3. `dispatch_lag` 与 `client_queue` 相对服务时延足够小，客户端没有成为瓶颈。
4. 对比吞吐量、TTFT p50/p95/p99、端到端延迟 p95、缓存命中率和实际 Prefill token 数。
5. 同时给出四个后端的请求与 token 分布，解释服务质量变化来自哪里。
6. 分别查看 steady、burst 和 recovery 阶段，避免只用全程平均值掩盖突发流量。

绝对性能会随模型和 GPU 改变，作业关注同一设备上的相对结果。所有对照组必须复用同一个 workload；如确需调整速率，应重新生成一份文件，并让全部配置共同使用它。

## 复现固定负载

```bash
curl -L -o toolagent_trace.jsonl \
  https://raw.githubusercontent.com/kvcache-ai/Mooncake/main/FAST25-release/traces/toolagent_trace.jsonl

python sample_prefix_families.py \
  --trace toolagent_trace.jsonl \
  --seed 2026 \
  --output mooncake_prefix_families_v2_seed2026.jsonl

python make_arrivals.py \
  --sample mooncake_prefix_families_v2_seed2026.jsonl \
  --profile steady:512:40,burst:1024:120,recovery:512:60 \
  --seed 2026 \
  --output mooncake_prefix_workload_v2_seed2026.jsonl
```

复现后应再次运行 `validate_workload.py`，核对规模、分布与 SHA-256。
