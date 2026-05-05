#!/usr/bin/env python3
"""
通达信MCP选股客户端
=========================
适配通达信问小达MCP服务（mcp.tdx.com.cn:3001）
替代东方财富妙想EM_API_KEY方案

工具：tdx_wenda_quotes
请求格式：JSON-RPC 2.0 + SSE transport
认证：tdx-api-key header + session-based

用法：
  python3 tdx_mcp_client.py --query "A股股价大于10元的前10只股票" --range AG --size 10
"""

import httpx
import json
import re
import uuid
import csv
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime


# ══════════════════════════════════════════════════════════════
# 通达信MCP配置
# ══════════════════════════════════════════════════════════════
TDX_API_KEY = "TDX-5aa5dd51dd0b28c38ed432c24e88d43c"
TDX_MCP_URL = "https://mcp.tdx.com.cn:3001/mcp"
TDX_TOOL_NAME = "tdx_wenda_quotes"

# 输出目录
OUTPUT_DIR = Path.cwd() / "tdx_mcp_output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


class TDXMCPClient:
    """通达信MCP客户端，处理session管理和JSON-RPC通信"""

    def __init__(self, api_key: str, mcp_url: str):
        self.api_key = api_key
        self.mcp_url = mcp_url
        self.session_id: Optional[str] = None
        self.protocol_version = "2025-03-26"
        self._client = httpx.Client(timeout=30.0)

    def _headers(self) -> Dict[str, str]:
        h = {
            "Content-Type": "application/json",
            "Accept": "text/event-stream, application/json",
            "tdx-api-key": self.api_key,
        }
        if self.session_id:
            h["mcp-session-id"] = self.session_id
        return h

    def initialize(self) -> Dict[str, Any]:
        """MCP初始化，获取session_id"""
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": self.protocol_version,
                "capabilities": {},
                "clientInfo": {"name": "tdx-mcp-client", "version": "1.0.0"},
            },
        }
        r = self._client.post(self.mcp_url, json=payload, headers=self._headers())
        # 从header提取session_id
        self.session_id = r.headers.get("mcp-session-id", "")
        # 解析SSE response: "event: message\ndata: {...}"
        text = r.text.strip()
        if "data:" in text:
            data_str = text.split("data:", 1)[1].strip()
            result = json.loads(data_str)
            return result.get("result", {})

    def send_notification(self, method: str, params: Dict = None) -> Dict:
        """发送MCP notification（如initialized）"""
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params or {},
        }
        r = self._client.post(self.mcp_url, json=payload, headers=self._headers())
        try:
            return r.json()
        except:
            return {"error": r.text}

    def call_tool(self, tool_name: str, arguments: Dict) -> Dict[str, Any]:
        """调用MCP工具"""
        payload = {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": tool_name,
                "arguments": arguments,
            },
        }
        r = self._client.post(self.mcp_url, json=payload, headers=self._headers())
        text = r.text.strip()

        # 解析SSE格式: "event: message\ndata: {...}"
        if "data:" in text:
            data_str = text.split("data:", 1)[1].strip()
            result = json.loads(data_str)
            return result.get("result", {})

        # 可能是普通JSON
        try:
            return json.loads(text)
        except:
            return {"raw": text}

    def close(self):
        self._client.close()


def query_tdx_wenda(
    question: str,
    range_market: str = "AG",
    size: str = "10",
    page: str = "1",
) -> Dict[str, Any]:
    """
    调用通达信问小达选股

    Args:
        question: 自然语言查询（如"A股股价大于10元的前10只股票"）
        range_market: 市场类别 AG(A股)/HK-GP(港股)/JJ(基金)/ZS(指数)
        size: 每页数量
        page: 页码

    Returns:
        包含 meta/headers/data/summary 的字典
    """
    client = TDXMCPClient(TDX_API_KEY, TDX_MCP_URL)

    # 1. 初始化获取session
    init_result = client.initialize()
    print(f"[TDX] 初始化完成, session={client.session_id}, server={init_result.get('serverInfo', {})}")

    # 2. 发送initialized通知
    client.send_notification("initialized", {})

    # 3. 调用选股工具
    result = client.call_tool(
        TDX_TOOL_NAME,
        {
            "question": question,
            "range": range_market,
            "size": size,
            "page": page,
        },
    )

    client.close()
    return result


def parse_result_to_csv(inner: Dict, output_path: Path = None) -> tuple:
    """
    将TDX查询结果解析为CSV

    Returns: (csv_path, row_count, description)
    """
    meta = inner.get("meta", {})
    headers = inner.get("headers", [])
    data = inner.get("data", [])
    summary = inner.get("summary", "")

    if not headers or not data:
        return None, 0, summary

    # 生成文件
    if output_path is None:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        uid = uuid.uuid4().hex[:8]
        output_path = OUTPUT_DIR / f"tdx_query_{ts}_{uid}.csv"

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for row in data:
            writer.writerow(row)

    desc_path = output_path.with_name(output_path.stem + "_description.txt")
    desc_path.write_text(f"查询结果\n时间: {datetime.now().isoformat()}\n摘要: {summary}\n总行数: {len(data)}\n列数: {len(headers)}\n列名: {','.join(headers)}", encoding="utf-8")

    return str(output_path), len(data), summary


def run_query(query: str, select_type: str = "A股", size: str = "50") -> Dict[str, Any]:
    """
    对外暴露的查询接口，适配 mx-stocks-screener 的调用方式

    Args:
        query: 自然语言选股条件（如"股价大于10元"、"市盈率小于20"）
        select_type: A股/港股/基金/ETF等，对应 range 参数
        size: 返回数量

    Returns:
        包含 csv_path, row_count, query, selectType 的字典
    """
    # 映射 select_type 到 range
    range_map = {
        "A股": "AG",
        "港股": "HK-GP",
        "基金": "JJ",
        "ETF": "JJ",
        "指数": "ZS",
        "美股": "AG",  # TDX暂不支持美股，用AG代替
    }
    range_val = range_map.get(select_type, "AG")

    # 转换查询条件格式（添加市场前缀便于TDX理解）
    if select_type == "A股" and "A股" not in query and "股价" not in query and "市盈率" not in query:
        question = f"A股{query}"
    elif select_type == "港股" and "港股" not in query:
        question = f"港股{query}"
    else:
        question = query

    print(f"[TDX Query] question={question}, range={range_val}, size={size}")

    raw_result = query_tdx_wenda(question, range_val, size=size)

    # 解析content字段（JSON字符串）
    content = raw_result.get("content", [])
    if content and isinstance(content, list) and len(content) > 0:
        first_item = content[0]
        if isinstance(first_item, dict) and "text" in first_item:
            try:
                inner = json.loads(first_item["text"])
            except json.JSONDecodeError:
                return {"error": f"无法解析响应: {first_item['text'][:200]}"}
        else:
            inner = first_item if isinstance(first_item, dict) else {}
    else:
        return {"error": f"无效响应格式: {raw_result}"}

    if not inner.get("data"):
        return {"error": f"查询无结果: {inner.get('meta', {})}"}

    csv_path, row_count, summary = parse_result_to_csv(inner)

    return {
        "csv_path": csv_path,
        "row_count": row_count,
        "query": query,
        "selectType": select_type,
        "description": summary,
        "raw_response": inner,
    }


# ─────────────────────────────────────────────
# CLI 入口
# ─────────────────────────────────────────────
def run_cli():
    import argparse

    parser = argparse.ArgumentParser(description="通达信MCP选股客户端")
    parser.add_argument("--query", "-q", required=True, help="自然语言查询条件")
    parser.add_argument("--select-type", "-t", default="A股", help="市场类型: A股/港股/基金/ETF")
    parser.add_argument("--size", "-s", default="50", help="返回数量")
    parser.add_argument("--output", "-o", default=None, help="CSV输出路径")

    args = parser.parse_args()

    result = run_query(args.query, args.select_type, args.size)

    if "error" in result:
        print(f"错误: {result['error']}")
        return

    print(f"\n查询成功!")
    print(f"CSV路径: {result['csv_path']}")
    print(f"行数: {result['row_count']}")
    print(f"摘要: {result['description']}")


if __name__ == "__main__":
    run_cli()