# MCP Server 开发规范

本文档定义在本项目中开发任何 MCP（Model Context Protocol）stdio 服务端的范式与注意事项，适用于所有当前和未来的 MCP 子项目。

---

## 目录

1. [概念速览](#概念速览)
2. [目录结构规范](#目录结构规范)
3. [协议线格式（必读）](#协议线格式必读)
4. [Server 生命周期](#server-生命周期)
5. [Tool 开发规范](#tool-开发规范)
6. [JSON-RPC Batch 处理](#json-rpc-batch-处理)
7. [错误处理规范](#错误处理规范)
8. [环境与部署](#环境与部署)
9. [mcp.json 配置规范](#mcpjson-配置规范)
10. [调试方法](#调试方法)
11. [常见错误速查](#常见错误速查)

---

## 概念速览

MCP（Model Context Protocol）是一个开放协议，允许 AI 客户端（Cursor、Claude Desktop、Codex 等）通过标准化接口调用外部工具和数据源。

**传输模式**：本项目统一使用 **stdio** 传输——宿主进程通过子进程的 stdin/stdout 与服务端通信，stderr 用于日志。

**消息协议**：JSON-RPC 2.0，承载在 NDJSON 上（见下一节）。

**两个核心概念**：
- **Tool**：AI 可以主动调用的函数，接受参数，返回文本结果（等同于"函数调用"）。
- **Resource**（可选）：AI 可以读取的数据，类似只读文件系统。本项目当前 MCP 只实现 Tool，Resource 返回空列表即可。

---

## 目录结构规范

每个 MCP 服务是 `tools/` 下的一个**独立子项目**：

```
tools/<mcp-name>/
├── <package_name>/
│   ├── __init__.py
│   └── server.py        # MCP stdio 主程序（入口）
├── pyproject.toml       # 包元数据、依赖声明
├── setup.py             # 兼容旧版 pip 的 editable install 支持
└── README.md            # 接入文档（Cursor / Claude / 远程部署）
```

**命名规则**：
- 子项目目录：`tools/<mcp-name>`，如 `tools/guanhe-mcp`
- Python 包名：下划线风格，如 `guanhe_mcp`
- `pyproject.toml` 中的 `[project.scripts]` 入口点：`<mcp-name> = "<package_name>.server:main"`

---

## 协议线格式（必读）

### ✅ 正确格式：NDJSON

Cursor V2、Claude Desktop、MCP Python SDK 全部使用 **NDJSON**（Newline-Delimited JSON）：

> 每条消息 = 一个合法 JSON 值 + 一个换行符 `\n`

**写入（stdout）**：

```python
import json, sys

def _write_message(payload):
    sys.stdout.write(json.dumps(payload, ensure_ascii=False) + "\n")
    sys.stdout.flush()   # 必须立即 flush，否则消息会滞留在缓冲区
```

**读取（stdin）**：

```python
def _read_message():
    line = sys.stdin.buffer.readline()
    if not line:
        return None
    stripped = line.strip()
    if not stripped:
        return _read_message()  # 跳过空行
    # 兼容旧版 Content-Length 帧（LSP 格式）
    if stripped.lower().startswith(b"content-length:"):
        n = int(stripped.split(b":", 1)[1].strip())
        sys.stdin.buffer.readline()  # 消耗空行分隔符
        return json.loads(sys.stdin.buffer.read(n).decode("utf-8"))
    # 标准 NDJSON
    return json.loads(stripped.decode("utf-8"))
```

### ❌ 禁止：Content-Length 帧（LSP 格式）

```
Content-Length: 123\r\n
\r\n
{"jsonrpc":"2.0",...}
```

若服务端向 stdout 写出 Content-Length 帧，Cursor 无法解析，会等满 **60 秒**后报：

```
MCP error -32001: Request timed out
```

**结论：stdout 只写 NDJSON，永远不写 Content-Length 头。**

---

## Server 生命周期

MCP 握手固定顺序如下，服务端必须正确响应每个阶段：

```
客户端 → initialize          带 protocolVersion、clientInfo
服务端 ← initialize result   回 protocolVersion、capabilities、serverInfo

客户端 → notifications/initialized   （通知，无需回包）

客户端 → resources/list      探针，回空列表即可
客户端 → prompts/list        探针，回空列表即可
客户端 → tools/list          返回工具列表

客户端 → tools/call          实际工具调用（可能多次）
```

**规则：**

1. **`initialize` 阶段不能有任何 I/O 阻塞**（数据库连接、HTTP 请求等），否则握手超时。所有外部连接必须**懒加载**，在第一次 `tools/call` 时初始化。
2. **`protocolVersion` 回显客户端发来的值**，不要硬编码服务端版本，否则版本协商失败。
3. `resources/list`、`prompts/list` 若不实现，必须回空列表而非报错。
4. 收到 `notifications/` 前缀的消息时**静默忽略**（不回包），这些是单向通知。
5. 遇到未知 method 时，若消息有 `id` 则回 `{"code": -32601, "message": "Method not found"}`；若无 `id`（通知）则忽略。

**标准 `initialize` 响应模板**：

```python
if method == "initialize":
    req_pv = params.get("protocolVersion") or "2024-11-05"
    return {
        "jsonrpc": "2.0",
        "id": mid,
        "result": {
            "protocolVersion": req_pv,
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {"name": "your-mcp-name", "version": "0.1.0"},
        },
    }
```

---

## Tool 开发规范

### 声明（`TOOLS` 列表）

每个 Tool 用一个 `dict` 描述，包含三个字段：

```python
{
    "name": "tool_name",          # 蛇形命名，唯一
    "description": "...",         # AI 依赖此字段选择 tool，要写清楚用途和参数语义
    "inputSchema": {              # JSON Schema，描述参数
        "type": "object",
        "properties": {
            "param1": {"type": "string", "description": "..."},
            "param2": {"type": "integer", "default": 20},
        },
        "required": ["param1"],   # 可选；若所有字段均非必填则省略整个 required
    },
}
```

**`description` 写作要点：**
- 说明 tool 的作用（做什么）
- 列出关键可选参数的含义
- 如有特殊值格式（日期格式、枚举等），在此说明
- AI 看不到代码，description 是它的全部依据

### 实现（`_call_tool` 函数）

```python
def _call_tool(name: str, arguments: dict, client) -> str:
    args = arguments or {}
    if name == "tool_name":
        # 调用后端 / 数据库 / 外部 API，返回字符串
        result = do_something(args.get("param1"), args.get("param2", 20))
        return json.dumps(result, ensure_ascii=False)
    raise ValueError(f"unknown tool {name}")
```

**返回值规范**：
- 必须返回 `str`（JSON 字符串、纯文本均可）
- 推荐返回 JSON 字符串，方便 AI 解析结构化数据
- 不要在返回值中包含敏感信息（token、密码等）

### 调用结果封装

`tools/call` 的响应区分正常结果和业务错误：

```python
# 成功
{
    "jsonrpc": "2.0", "id": mid,
    "result": {
        "content": [{"type": "text", "text": "...返回内容..."}],
        "isError": False,
    }
}

# 业务错误（如 HTTP 4xx，工具运行但结果是错误）
{
    "jsonrpc": "2.0", "id": mid,
    "result": {
        "content": [{"type": "text", "text": "错误详情"}],
        "isError": True,
    }
}

# 协议/系统错误（工具未能运行）
{
    "jsonrpc": "2.0", "id": mid,
    "error": {"code": -32603, "message": "Internal error: ..."}
}
```

---

## JSON-RPC Batch 处理

MCP 规范要求支持 batch：客户端可在**一条消息**里发 JSON 数组，服务端必须在**一条消息**里回 JSON 数组。

```python
if isinstance(raw, list):
    out = []
    for item in [m for m in raw if isinstance(m, dict)]:
        resp = dispatch(item)
        if resp is not None:
            out.append(resp)
    if out:
        _write_message(out)   # 一次写入，body 是数组
    continue
```

**禁止**对 batch 里每条消息分别调用 `_write_message`，那会发出多行，而客户端只等一行响应。

---

## 错误处理规范

1. **外部连接失败**（数据库、HTTP 等）：作为 `tools/call` 的 `error` 响应返回，不要让进程崩溃。
2. **未知 tool name**：返回 `-32601 Method not found` 风格的错误响应。
3. **顶层循环**：用 `try/except` 包裹整个消息处理，捕获后记录到 stderr，继续处理下一条消息，不要让一条坏消息终止进程。
4. **EOF / stdin 关闭**：捕获 `EOFError` / `readline()` 返回空字节，正常退出循环。
5. **stderr 用于日志**：进程日志全部写 `sys.stderr`，不要写 `sys.stdout`（会污染消息流）。

```python
while True:
    try:
        raw = _read_message()
    except (EOFError, json.JSONDecodeError):
        break
    if raw is None:
        break
    try:
        # 处理消息 ...
    except Exception as e:
        import traceback
        sys.stderr.write(f"[mcp] handler error: {e}\n")
        traceback.print_exc(file=sys.stderr)
        sys.stderr.flush()
        # 如有 id，回一个 error 响应
```

---

## 环境与部署

### 本地开发

MCP server 运行在任意 Python 环境中，只需满足以下条件：

1. Python >= 3.10
2. 已安装该 MCP 的依赖（通过 `pip install -e tools/<mcp-name>` 安装）
3. 所需环境变量已设置

本地开发时，`~/.cursor/mcp.json` 中的 `command` 必须指向**已安装该包的 Python 的绝对路径**：

```bash
# 查询当前激活环境的 Python 路径
which python
```

### 远程服务器部署

在远程服务器上，通常不使用 Cursor，而是由其他 AI 客户端（Claude Desktop、自定义 Agent 框架等）以 stdio 方式启动 MCP 进程。部署要点：

- 用 `pip install tools/<mcp-name>` 或发布到 PyPI 后 `pip install <mcp-name>` 安装
- 环境变量通过服务器配置文件（`.env`、系统 env、Docker `--env-file` 等）注入，**不要硬编码在代码里**
- 建议加 `"PYTHONUNBUFFERED": "1"`（或在启动命令前 `export PYTHONUNBUFFERED=1`），确保 stderr 日志实时输出
- 在无 conda 环境的服务器上，优先使用 `venv` + `pip install`

### 依赖声明

`pyproject.toml` 示例：

```toml
[project]
name = "your-mcp-name"
version = "0.1.0"
requires-python = ">=3.10"
dependencies = [
    "httpx>=0.27",      # 按需替换为实际依赖
]

[project.scripts]
your-mcp-name = "your_mcp_package.server:main"
```

同时提供 `setup.py` 作为旧版 pip 兼容兜底：

```python
from setuptools import setup
setup()
```

---

## mcp.json 配置规范

文件位置：`~/.cursor/mcp.json`（本地开发）

```json
{
  "mcpServers": {
    "<server-name>": {
      "command": "/absolute/path/to/python",
      "args": ["-m", "<package_name>.server"],
      "cwd": "/absolute/path/to/tools/<mcp-name>",
      "env": {
        "ENV_VAR_1": "value1",
        "PYTHONUNBUFFERED": "1"
      }
    }
  }
}
```

| 字段 | 规则 |
|------|------|
| `command` | **必须是绝对路径**。Cursor 子进程不继承 shell 的 `PATH`，`python3` 等相对命令会找不到。 |
| `args` | 推荐用 `-m <package>.server`，而非依赖入口脚本（入口脚本同样需要在 PATH 上）。 |
| `cwd` | 指向包含 `<package_name>/` 目录的路径，确保 Python 能找到包。若已 `pip install`，此字段可省略。 |
| `env` | 只写该 MCP 需要的变量。**不要在此处放密钥明文**（本地开发可以，但不要提交到 git）。 |
| `PYTHONUNBUFFERED` | 始终设为 `"1"`，保证 stderr 日志实时可见。 |

---

## 调试方法

### 1. 语法检查

```bash
python3 -m py_compile tools/<mcp-name>/<package_name>/server.py && echo OK
```

### 2. 本地握手测试

用 Python 子进程模拟客户端，验证 NDJSON 握手和 tools/list：

```python
import json, os, subprocess, sys

env = {**os.environ, "YOUR_ENV_VAR": "test_value"}
p = subprocess.Popen(
    [sys.executable, "-m", "<package_name>.server"],
    cwd="tools/<mcp-name>",
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    env=env,
)

def send(obj):
    p.stdin.write((json.dumps(obj) + "\n").encode("utf-8"))
    p.stdin.flush()

def recv():
    return json.loads(p.stdout.readline())

send({"jsonrpc":"2.0","id":1,"method":"initialize","params":{
    "protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test"}}})
r = recv()
assert "result" in r, f"initialize failed: {r}"
print("initialize OK:", r["result"]["protocolVersion"])

send({"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}})
r = recv()
print("tools:", [t["name"] for t in r["result"]["tools"]])

p.terminate()
```

### 3. Cursor Show Output

MCP 面板 → 点击服务名 → **Show Output**：显示进程 stderr 的实时日志。修改代码后需**完全退出 Cursor 再重启**（reload window 不重启 MCP 子进程）。

### 4. 常见 stderr 日志含义

| 日志内容 | 含义与解决 |
|---------|-----------|
| `ModuleNotFoundError: No module named '<pkg>'` | `command` 指向的 Python 未安装该包；运行 `pip install -e tools/<mcp-name>` |
| `ModuleNotFoundError: No module named 'httpx'`（或其他依赖） | 同上；该 Python 环境缺少依赖 |
| `XXX must be set` | 环境变量未注入；检查 `mcp.json` 的 `env` 字段 |
| `[mcp] handler error: ...` + traceback | `_call_tool` 逻辑 bug；看 traceback 定位 |
| 进程立即退出，无任何输出 | 启动崩溃；先跑语法检查，再手动执行 `command args` 看报什么错 |

---

## 常见错误速查

| 症状 | 根因 | 解决 |
|------|------|------|
| `MCP error -32001: Request timed out`（60 秒） | 服务端 stdout 写了 Content-Length 帧，客户端无法解析 | `_write_message` 改为 `stdout.write(json + "\n")` |
| 连接立即失败（非超时） | 进程启动即崩溃 | 看 Show Output；手动运行命令复现 |
| tools/list 正常，tools/call 全部报错 | 外部连接（HTTP/DB）在初始化时失败，或依赖未安装 | 检查环境变量；确认依赖已安装 |
| batch 请求只回了部分响应 | 对 batch 内每条消息分别调用了 `_write_message` | 收集所有响应后一次性写出数组 |
| `command: "python3"` 或入口脚本找不到 | Cursor 子进程 PATH 不含目标目录 | 改用绝对路径 |
| 修改代码后 Cursor 行为不变 | 旧进程仍在运行 | 完全退出 Cursor 再重开 |
| Show Output 日志延迟或不显示 | stdout/stderr 缓冲 | 设 `PYTHONUNBUFFERED=1`；写 stderr 后立即 `sys.stderr.flush()` |
