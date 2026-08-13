# 作业二目标三：四副本在线路由负载

本目录提供课程固定负载和 HTTP/SSE 流量发生器，用于比较四种 Ray Serve 路由配置。A-D 四组实验均通过同一个 Ray Serve HTTP 入口发送请求；模型、SGLang 配置、四副本拓扑、请求内容和到达时间保持不变。

## 快速开始

需要 Python 3.10 或更高版本。进入本目录后安装客户端依赖并校验负载：

```bash
python -m pip install -r requirements.txt
python validate_workload.py mooncake_prefix_workload_v2_seed2026.jsonl
```

校验结果应包含以下内容：

```text
sha256: f154ba954e28ca9612257edf5deb0d5dfc6bf27fd0115a701e1f49e52df75b03
families: 64
warmups: 64
measured: 2048
planned_duration_s: 29.538768
```

Ray Serve 入口准备完成后，可以运行一轮 A 组实验：

```bash
python run_workload.py \
  --policy serve \
  --run-name A_default_run1 \
  --router-name p2c \
  --max-ongoing-requests 5 \
  --base-url http://127.0.0.1:8000 \
  --workload mooncake_prefix_workload_v2_seed2026.jsonl \
  --max-in-flight 2048 \
  --output-dir results/A_default/run-1
```

`run_workload.py` 只负责回放和测量，不会启动 Ray、Ray Serve 或 SGLang。

## 文件说明

| 文件 | 作用 |
| --- | --- |
| `mooncake_prefix_workload_v2_seed2026.jsonl` | A-D 正式实验使用的固定负载 |
| `run_workload.py` | 发送流式请求并记录逐请求指标 |
| `validate_workload.py` | 检查请求规模、前缀关系、到达序列和文件摘要 |
| `compare_runs.py` | 汇总每个配置的重复实验，并计算均值和范围 |
| `mooncake_prefix_families_v2_seed2026.jsonl` | 生成固定到达序列前的中间数据 |
| `sample_prefix_families.py` | 从 Mooncake trace 生成中间数据 |
| `make_arrivals.py` | 为中间数据生成 Poisson 到达序列 |
| `tests/` | 流量发生器和汇总器的自动测试 |

正式 A-D 结果只能使用 `mooncake_prefix_workload_v2_seed2026.jsonl`。校验器和流量发生器都会核对该文件的 SHA-256。中间数据和生成脚本用于核验数据来源，不用于修改正式实验的请求比例、到达速率或顺序。

## 负载内容

负载由 [Mooncake FAST'25 tool-agent trace](https://github.com/kvcache-ai/Mooncake/blob/main/FAST25-release/traces/toolagent_trace.jsonl) 派生，保留了 trace 中的前缀关系和生成长度。原始 trace 不含 Prompt token ID 和请求到达时间，因此这两部分由课程脚本确定性生成。

| 项目 | 数值 |
| --- | ---: |
| 前缀族 | 64 |
| 预热请求 | 64 |
| 测量请求 | 2048 |
| 输入 token 总量 | 3,233,280 |
| 请求输出 token 总量 | 268,831 |
| 单请求输入长度 | 1,216-1,600 tokens |
| 单请求输出长度 | 16-190 tokens |

每个前缀族至少共享 1024 个 token。负载包含 1 个超热、7 个热、24 个中等热度和 32 个冷前缀族；其中超热前缀族有 768 条测量请求，每条生成 190 tokens，用于同时形成缓存热点和持续生成压力。

测量请求按固定顺序分为三个阶段：

| 阶段 | 请求数 | 平均到达速率 |
| --- | ---: | ---: |
| `steady` | 512 | 40 req/s |
| `burst` | 1024 | 120 req/s |
| `recovery` | 512 | 60 req/s |

热点比例用于形成路由压力，不代表 Mooncake 原始业务的请求比例。

## 服务接口

实验服务应具有以下结构：

```text
Client -> Ray Serve HTTP Proxy -> Replica 0..3 -> SGLang backend 0..3
```

Ray Serve 入口需接收 `POST /generate`，并将请求体和以下请求头转发给 SGLang：

```text
X-Session-Id
X-Workload-Request-ID
X-Prefix-Family
```

`X-Session-Id` 和 `X-Prefix-Family` 的值均为当前请求的前缀族 ID。路由器可以读取这些值，但不得读取未来请求或修改 workload。

Ray Serve 应将 SGLang 的 SSE 响应流原样返回，并在响应中加入：

```text
X-Ray-Replica-ID
X-Ray-Node-ID
X-SGLang-Backend
```

三个值应分别稳定标识实际处理请求的 Serve Replica、Ray 节点和 SGLang 后端。缺少响应头、请求失败、输出 token 数不完整，或测量阶段没有覆盖四个 Replica、四个节点和四个后端时，流量发生器会以非零状态退出。

默认生成的输入 token ID 位于 `4096-64095`。课程模型 `Qwen/Qwen3-0.6B` 可以直接使用；更换模型时，应确认该范围没有超过模型词表，并保证所有对照组使用相同的 `--token-base` 和 `--token-span`。

## 运行 A-D

每一轮都按以下顺序执行：

1. 停止上一轮 Ray Serve 应用，使用本轮配置重新部署。
2. 分别调用四个 SGLang 后端的 `POST /flush_cache`，确认全部成功。
3. 启动流量发生器。它会先顺序完成 64 条预热请求，再发送 2048 条测量请求。
4. 检查进程退出状态和 `summary.json`，确认本轮有效。

所有正式实验统一使用 `--policy serve`。`--router-name` 和 `--max-ongoing-requests` 只记录配置，实际 Ray Serve 部署必须使用相同的值。

四组配置如下：

| 组别 | `--router-name` | `--max-ongoing-requests` | Ray Serve 配置 |
| --- | --- | ---: | --- |
| A | `p2c` | `5` | 默认请求路由器 |
| B 候选 | `p2c` | 两个非默认值 | 每个候选值运行一轮，并从中选择一个参数 |
| C | `consistent_hash` | B 的选定值 | `ConsistentHashRouter`，`num_fallback_replicas=0` |
| D | 自定义名称 | B 的选定值 | 自行扩展或实现的路由器 |

组 A 和 D 各运行两轮，两个 B 候选值和 C 各运行一轮，共七轮。组 B 选定参数后不再另行重复。每轮使用独立输出目录，例如：

```bash
python run_workload.py \
  --policy serve \
  --run-name C_affinity_run1 \
  --router-name consistent_hash \
  --max-ongoing-requests 32 \
  --base-url http://127.0.0.1:8000 \
  --workload mooncake_prefix_workload_v2_seed2026.jsonl \
  --max-in-flight 2048 \
  --output-dir results/C_affinity/run-1
```

结果目录中已有本工具生成的文件时，程序会停止，避免覆盖旧实验。确需重跑同一路径时可以添加 `--overwrite`。

## 输出与判定

每轮成功执行后生成四个文件：

| 文件 | 内容 |
| --- | --- |
| `config.json` | 命令、模型、路由器、并发参数、负载 SHA-256、校验状态和自定义备注 |
| `warmups.csv` | 64 条预热请求的结果 |
| `requests.csv` | 2048 条测量请求的逐请求结果 |
| `summary.json` | 全程、各流量阶段和各热度层级的汇总指标 |

预热阶段失败时会生成 `failure.json` 并停止，不再发送测量请求。

`requests.csv` 中主要字段如下：

| 字段 | 含义 |
| --- | --- |
| `ttft_s` | 从发送请求到收到首个输出 token 的时间 |
| `tpot_s` | 收到首个 token 后，平均生成一个 token 的时间 |
| `latency_s` | 请求端到端时间 |
| `dispatch_lag_s` | 实际开始发送相对计划到达时间的偏差 |
| `client_queue_s` | 请求等待客户端并发槽位的时间 |
| `cached_tokens` | SGLang 报告的缓存命中 token 数 |
| `replica_id`、`ray_node_id`、`backend` | 实际处理请求的路由位置 |

一轮正式结果应同时满足：

- `successful=2048`，`failed=0`，`incomplete=0`；
- `warmup_successful=64`，`warmup_failed=0`，`warmup_incomplete=0`；
- `validation_errors=[]`；
- `replica_distribution`、`node_distribution` 和 `backend_distribution` 各有四项；
- `dispatch_lag_s` 和 `client_queue_s` 相对服务延迟较小，客户端没有成为主要瓶颈。

运行命令可通过重复的 `--metadata KEY=VALUE` 将 GPU、SGLang 参数或代码版本写入 `config.json`。

## 汇总重复实验

同一个 `NAME` 可以传入多轮结果。A、D 使用相同名称汇总两轮，两个 B 候选分别记为 B1、B2：

```bash
python compare_runs.py \
  --baseline A \
  --run A=results/A_default/run-1/summary.json \
  --run A=results/A_default/run-2/summary.json \
  --run B1=results/B_candidate-1/summary.json \
  --run B2=results/B_candidate-2/summary.json \
  --run C=results/C_affinity/run-1/summary.json \
  --run D=results/D_improved/run-1/summary.json \
  --run D=results/D_improved/run-2/summary.json \
  --output results/comparison.json
```

`comparison.json` 给出每项指标的原始值、均值、最小值和最大值，并以 A 组两轮均值为基准计算相对变化。A、D 可据此报告两轮范围；B1、B2 和 C 为单轮结果。程序会拒绝失败、输出不完整、测量请求数不是 2048、重复轮次配置不一致，或模型与负载等对照条件不同的结果。

报告主表比较成功率、吞吐量、缓存命中率、实际 Prefill token 数、TTFT p95、端到端延迟 p95，以及四个后端的请求分布。其他逐请求指标和 `steady`、`burst`、`recovery` 分阶段结果按需用于解释缓存复用、负载分布或排队，无需全部放入正文。

## 核验数据来源

以下步骤仅用于核验固定负载的生成过程：

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

python validate_workload.py mooncake_prefix_workload_v2_seed2026.jsonl
```

核验得到的文件摘要应与本页开头一致。A-D 正式实验仍应使用仓库中发布的固定文件。

运行本目录的自动测试：

```bash
python -m unittest discover -s tests -v
```
