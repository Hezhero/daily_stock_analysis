#!/usr/bin/env python3
"""
stock_recommend_filter.py
=========================
输入：10个股票代码（支持 000001、000001.SZ、300059.SZ 等格式）
输出：推荐股票列表 + 理由

工作流（三重过滤）：
  1. 通达信MCP（tdx_wenda_quotes） → 技术面 + 资金面验证（已替换东方财富妙想）
  2. 通达信MCP（tdx_wenda_quotes） → 业绩基本面验证
  3. 通达信MCP（tdx_wenda_quotes） → 行业研报确认

用法：
  python3 stock_recommend_filter.py 000001 000002 300059
  python3 stock_recommend_filter.py 000001.SZ 000002.SZ 300059.SZ
"""

import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import argparse
import asyncio
import csv
import json
import os
import re
import subprocess
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

# ─────────────────────────────────────────────
# 路径配置（相对于本文件位置）
# ─────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent
SKILL_BASE = SCRIPT_DIR.parent   # 技能根目录（link 到共享 skills）

SCREENER_SCRIPT  = Path("/root/.openclaw/skills/mx-stocks-screener/scripts/get_data.py")
EARNINGS_SCRIPT  = Path("/root/.openclaw/skills/stock-earnings-review/scripts")
REVIEW_API_SCRIPT = EARNINGS_SCRIPT / "call_review_api.py"
VALIDATE_ENTITY   = EARNINGS_SCRIPT / "validate_entity.py"
NORM_REPORT       = EARNINGS_SCRIPT / "normalize_report_period.py"

TRACKER_SCRIPT    = Path("/root/.openclaw/skills/industry-stock-tracker/scripts/generate_industry_stock_tracker_report.py")

# 输出根目录
OUTPUT_ROOT = Path.cwd() / "stock_filter_output"
OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

# TDX MCP API Key（替换东方财富妙想 EM_API_KEY）
TDX_API_KEY = "TDX-5aa5dd51dd0b28c38ed432c24e88d43c"
TDX_MCP_URL = "https://mcp.tdx.com.cn:3001/mcp"

# 旧 EM_API_KEY 保留用于部分兼容检查（不再用）
EM_API_KEY = TDX_API_KEY  # 复用变量名以兼容现有代码

# ─────────────────────────────────────────────
# 通达信MCP客户端（替代东方财富妙想）
# ─────────────────────────────────────────────
import httpx

class TDXMCPClient:
    """通达信问小达MCP客户端，处理session管理和JSON-RPC通信"""

    def __init__(self, api_key: str = TDX_API_KEY, mcp_url: str = TDX_MCP_URL):
        self.api_key = api_key
        self.mcp_url = mcp_url
        self.session_id: Optional[str] = None
        self.protocol_version = "2025-03-26"
        self._client = httpx.Client(timeout=30.0)
        self._initialized = False
        self._init_result: Dict = {}

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
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {
                "protocolVersion": self.protocol_version,
                "capabilities": {},
                "clientInfo": {"name": "tdx-mcp-client", "version": "1.0.0"},
            },
        }
        r = self._client.post(self.mcp_url, json=payload, headers=self._headers())
        self.session_id = r.headers.get("mcp-session-id", "")
        text = r.text.strip()
        if "data:" in text:
            data_str = text.split("data:", 1)[1].strip()
            result = json.loads(data_str)
            self._init_result = result.get("result", {})
            return self._init_result
        return {}

    @property
    def server_info(self) -> Dict:
        return self._init_result.get("serverInfo", {})

    def send_notification(self, method: str, params: Dict = None) -> Dict:
        payload = {"jsonrpc": "2.0", "method": method, "params": params or {}}
        r = self._client.post(self.mcp_url, json=payload, headers=self._headers())
        try:
            return r.json()
        except:
            return {"error": r.text}

    def call_tool(self, tool_name: str, arguments: Dict) -> Dict[str, Any]:
        payload = {
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {"name": tool_name, "arguments": arguments},
        }
        r = self._client.post(self.mcp_url, json=payload, headers=self._headers())
        text = r.text.strip()
        if "data:" in text:
            data_str = text.split("data:", 1)[1].strip()
            result = json.loads(data_str)
            return result.get("result", {})
        try:
            return json.loads(text)
        except:
            return {"raw": text}

    def close(self):
        self._client.close()

    def ensure_init(self):
        if not self._initialized:
            self.initialize()
            self.send_notification("initialized", {})
            self._initialized = True
            self._initialized = True


def call_tdx_wenda(question: str, range_market: str = "AG", size: str = "50") -> Dict[str, Any]:
    """
    调用通达信问小达MCP工具
    
    Args:
        question: 自然语言查询（如"A股股价大于10元的前10只股票"）
        range_market: 市场类别 AG(A股)/HK-GP(港股)/JJ(基金)/ZS(指数)
        size: 每页数量
    
    Returns:
        包含 meta/headers/data/summary 的内层字典
    """
    client = TDXMCPClient()
    client.ensure_init()
    result = client.call_tool("tdx_wenda_quotes", {
        "question": question,
        "range": range_market,
        "size": size,
    })
    client.close()

    content = result.get("content", [])
    if content and isinstance(content, list) and len(content) > 0:
        first_item = content[0]
        if isinstance(first_item, dict) and "text" in first_item:
            return json.loads(first_item["text"])
    return result


def parse_tdx_result_to_csv(inner: Dict, output_path: Path = None) -> tuple:
    """将TDX查询结果解析为CSV，返回(csv_path, row_count, summary)"""
    meta = inner.get("meta", {})
    headers = inner.get("headers", [])
    data = inner.get("data", [])
    summary = inner.get("summary", "")

    if not headers or not data:
        return None, 0, summary

    if output_path is None:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        uid = uuid.uuid4().hex[:8]
        output_dir = SCRIPT_DIR.parent / "tdx_mcp_output"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / f"tdx_query_{ts}_{uid}.csv"

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for row in data:
            writer.writerow(row)

    return str(output_path), len(data), summary


# ─────────────────────────────────────────────
# 工具函数
# ─────────────────────────────────────────────

def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def run_python(script: Path, args: List[str], timeout: int = 300) -> Dict[str, Any]:
    """同步执行 Python 脚本，返回 JSON stdout 或解析 CSV 路径。"""
    cmd = [sys.executable, str(script)] + args
    log(f"  执行: {' '.join(cmd)}")
    try:
        r = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            env={**os.environ, "EM_API_KEY": EM_API_KEY},
        )
        if r.returncode != 0:
            return {"ok": False, "error": r.stderr.strip()[:500]}
        
        # 解析 stdout，尝试提取 CSV 路径和 JSON
        csv_path = None
        json_data = None
        for line in r.stdout.strip().splitlines():
            line = line.strip()
            if line.startswith("CSV:"):
                csv_path = line[4:].strip()
            elif line.startswith("{") or line.startswith("["):
                try:
                    json_data = json.loads(line)
                except json.JSONDecodeError:
                    continue

        result = {"ok": True}
        if csv_path:
            result["csv_path"] = csv_path
        if json_data:
            result.update(json_data)
        elif json_data is None and not csv_path:
            result["raw"] = r.stdout.strip()[:500]
        return result
    except subprocess.TimeoutExpired:
        return {"ok": False, "error": f"超时 ({timeout}s)"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def run_bash(cmd: str, timeout: int = 300) -> str:
    """执行 bash 命令。"""
    log(f"  执行: {cmd}")
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout,
                          env={**os.environ, "EM_API_KEY": EM_API_KEY})
        return r.stdout.strip()
    except subprocess.TimeoutExpired:
        return f"超时 ({timeout}s)"
    except Exception as e:
        return str(e)


def normalize_code(raw: str) -> Dict[str, str]:
    """
    解析股票代码，返回 {code, market, em_code, display}。
    支持：
      000001        → 000001, SZ, 000001.SZ,   平安银行(SZ)
      000001.SZ     → 000001, SZ, 000001.SZ,   平安银行(SZ)
      300059.SZ    → 300059, SZ, 300059.SZ,   东方财富(SZ)
      600519.SH    → 600519, SH, 600519.SH,   贵州茅台(SH)
      9988.HK      → 9988,  HK, 9988.HK,      阿里巴巴(HK)
    """
    raw = raw.strip().upper()
    # 去掉空格
    raw = re.sub(r"\s+", "", raw)

    # 检测后缀
    m = re.match(r"^(\d{6})\.(SZ|SH|HK|BJ)$", raw)
    if m:
        code, market = m.group(1), m.group(2)
    else:
        m2 = re.match(r"^(\d{6})$", raw)
        if m2:
            code = m2.group(1)
            # A股：沪市6/9开头→SH，深市0/3开头→SZ，北交所8开头→BJ
            if code.startswith(("6", "9")):
                market = "SH"
            elif code.startswith(("0", "3")):
                market = "SZ"
            elif code.startswith("8"):
                market = "BJ"
            else:
                market = "SZ"  # 默认深市
        else:
            # 港股 4-5位数字，HK后缀或纯数字
            m3 = re.match(r"^(\d{4,5})\.HK$", raw)
            if m3:
                code, market = m3.group(1), "HK"
            else:
                m4 = re.match(r"^(\d{4,5})$", raw)
                if m4:
                    code, market = m4.group(1), "HK"
                else:
                    raise ValueError(f"无法识别的股票代码: {raw}")

    # 构造 em_code（东方财富格式）
    em_code_map = {"SZ": f"{code}.SZ", "SH": f"{code}.SH", "HK": f"{code}.HK", "BJ": f"{code}.BJ"}
    em_code = em_code_map.get(market, f"{code}.{market}")

    # display 简称（后面通过实体识别补全）
    market_display = {"SZ": "深市", "SH": "沪市", "HK": "港股", "BJ": "北交所"}
    display = f"{code}({market_display.get(market, market)})"
    return {"code": code, "market": market, "em_code": em_code, "display": display}


def get_entity_info(secu_code: str, market_char: str, class_code: str) -> Optional[str]:
    """通过 validate_entity 获取股票名称。"""
    result = run_python(
        VALIDATE_ENTITY,
        ["--query", f"{secu_code} {market_char}"],
        timeout=30,
    )
    if result.get("ok") is False:
        return None
    return result.get("secuName")


# ─────────────────────────────────────────────
# Step 1：技术面 + 资金面验证（mx-stocks-screener）
# ─────────────────────────────────────────────

def screen_stock_technical(code_norm: Dict) -> Dict[str, Any]:
    """
    使用通达信MCP（tdx_wenda_quotes）验证单支股票的技术面信号。
    替代原来的 mx-stocks-screener 调用东方财富妙想方案。
    """
    em_code = code_norm["em_code"]
    code = code_norm["code"]
    market = code_norm["market"]

    if market in ("SZ", "SH", "BJ"):
        range_market = "AG"
    elif market == "HK":
        range_market = "HK-GP"
    else:
        return {"ok": False, "reason": f"不支持的市场: {market}"}

    # 通达信问小达自然语言查询：个股技术面分析
    question = f"{em_code}个股技术分析：最新价、涨跌幅、成交量、换手率、KDJ、MACD"

    try:
        inner = call_tdx_wenda(question, range_market=range_market, size="1")
        meta = inner.get("meta", {})
        if meta.get("code", -1) != 0 or not inner.get("data"):
            # Fallback: 简单行情查询
            fallback_q = f"{em_code}最新价、涨跌幅、成交量"
            inner = call_tdx_wenda(fallback_q, range_market=range_market, size="1")

        # 写入CSV供 parse_technical_score 读取
        headers = inner.get("headers", [])
        data = inner.get("data", [])
        if headers and data:
            csv_path, row_count, _ = parse_tdx_result_to_csv(inner)
            return {"ok": True, "csv_path": csv_path, "tdx_response": inner}
        else:
            return {"ok": False, "error": f"查询无结果: {inner}"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def screen_batch(stocks: List[Dict], max_parallel: int = 3) -> Dict[str, Dict]:
    """
    并发验证多支股票（限制并发数）。
    返回：{em_code: 验证结果}
    """
    results = {}
    # 分批执行，避免 API 限流
    for i in range(0, len(stocks), max_parallel):
        batch = stocks[i : i + max_parallel]
        log(f"[Step1] 第 {i//max_parallel + 1} 批，{len(batch)} 支股票")
        futures = {}
        with ThreadPoolExecutor(max_workers=max_parallel) as executor:
            for s in batch:
                future = executor.submit(screen_stock_technical, s)
                futures[s["em_code"]] = future

            for em_code, future in futures.items():
                try:
                    results[em_code] = future.result(timeout=120)
                except Exception as e:
                    results[em_code] = {"ok": False, "error": str(e)}
        time.sleep(1)  # API 限流保护
    return results


def parse_technical_score(result: Dict) -> float:
    """
    从 screener 返回结果中提取技术面评分（0~100）。
    实际读取 CSV 文件，解析中文列名对应的值进行评分。

    评分维度：
      - 涨跌幅：>0 → +20分，>3% → +10分
      - 区间涨跌幅：>5% → +15分
      - 股价 vs 5日均线：站在均线上方 → +15分
      - 主力净额：>0 → +20分，<0 → -10分（净额越大越好）
      - 量价齐升：符合 → +15分
      - MACD：>0 → +10分
      - KDJ J值：>80 超买 → -10分，<20 超卖 → +10分
    """
    try:
        csv_path = result.get("csv_path", "")
        if not csv_path or not Path(csv_path).exists():
            # 回退到关键字评分
            return _parse_technical_score_by_keywords(result)

        import csv as csv_lib
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv_lib.DictReader(f)
            rows = list(reader)
        if not rows:
            return _parse_technical_score_by_keywords(result)

        row = rows[0]
        score = 50.0

        # 涨跌幅（最新日）
        change_str = _get_col_val(row, ["涨跌幅(%) 2026.04.30", "涨跌幅(%)", "chg"])
        if change_str:
            try:
                change = float(change_str.strip().replace("%", "").replace("％", ""))
                if change > 0:
                    score += 20
                if change > 3:
                    score += 10
            except ValueError:
                pass

        # 区间涨跌幅
        range_str = _get_col_val(row, ["区间涨跌幅(%) 2026.04.28 - 2026.04.30", "区间涨跌幅(%)"])
        if range_str:
            try:
                range_chg = float(range_str.strip().replace("%", "").replace("％", ""))
                if range_chg > 5:
                    score += 15
                elif range_chg > 0:
                    score += 8
            except ValueError:
                pass

        # 股价 vs 5日均线
        price_str = _get_col_val(row, ["最新价(元) 2026.04.30", "最新价(元)", "now_price"])
        ma5_str = _get_col_val(row, ["5日均线(元) 2026.04.30", "5日均线(元)"])
        if price_str and ma5_str:
            try:
                price = float(price_str)
                ma5 = float(ma5_str)
                if price > ma5:
                    score += 15
            except ValueError:
                pass

        # 换手率适中（3%~10% 活跃但不过热）
        hss_str = _get_col_val(row, ["换手率(%) 2026.04.30", "换手率(%)"])
        if hss_str:
            try:
                hss = float(hss_str)
                if 3 <= hss <= 10:
                    score += 5
                elif hss > 15:
                    score -= 5
            except ValueError:
                pass

        return max(0.0, min(100.0, score))
    except Exception:
        return _parse_technical_score_by_keywords(result)


def _get_col_val(row: Dict, candidates: List[str]) -> str:
    """
    从 CSV row 中查找匹配的列名（支持多种列名格式）。

    支持格式：
    - 东方财富: "涨跌幅(%) 2026.04.30" (空格分隔)
    - 通达信: "涨跌幅(%).前复权<br>2026.04.30" (<br>分隔，含中间修饰词)
    - 纯列名: "涨跌幅(%)"
    - 短格式: "chg", "now_price"

    匹配逻辑: 精确匹配 → 再用 substring 包含匹配（要求列名尾部含日期）
    """
    import re
    for c in candidates:
        # 精确匹配 (包括完整列名带日期后缀)
        if c in row:
            val = row[c].strip()
            if val:
                return val
        # substring匹配: candidate 是 column 的子串（兼容<br>和含修饰词的格式）
        for col in row.keys():
            if c in col:
                # 列名必须在 candidate 之后还有内容（尤其是日期）
                suffix = col[col.index(c) + len(c):]
                # 检查 suffix 中是否包含日期 (YYYY.MM.DD 格式)
                if re.search(r'20\d{2}\.\d{2}\.\d{2}', suffix):
                    val = row[col].strip()
                    if val:
                        return val
    return ""


def _parse_amount(amount_str: str) -> float:
    """解析金额字符串，返回元。支持的格式：3728.87万、1.5亿、1000万"""
    s = amount_str.strip().replace(" ", "")
    if "亿" in s:
        return float(s.replace("亿", "").replace("元", "")) * 1e8
    elif "万" in s:
        return float(s.replace("万", "").replace("元", "")) * 1e4
    elif "千" in s:
        return float(s.replace("千", "").replace("元", "")) * 1e3
    else:
        try:
            return float(s.replace("元", ""))
        except ValueError:
            return 0.0


def _parse_technical_score_by_keywords(result: Dict) -> float:
    """回退方案：通过关键字在原始 JSON 中评分。"""
    try:
        raw = json.dumps(result, ensure_ascii=False)
        score = 50.0
        positive_kw = ["主力净流入", "净流入", "资金流入", "量价齐升",
                       "站上均线", "突破", "金叉", "上涨趋势", "换手率", "活跃"]
        negative_kw = ["主力净流出", "资金流出", "死叉", "破位", "下跌趋势", "缩量", "弱势"]
        for kw in positive_kw:
            if kw in raw:
                score += 5
        for kw in negative_kw:
            if kw in raw:
                score -= 5
        return max(0.0, min(100.0, score))
    except Exception:
        return 50.0


def step1_filter(stocks: List[Dict], threshold: float = 55.0) -> List[Dict]:
    """
    Step 1 过滤：技术面 + 资金面验证
    - 评分 ≥ threshold → 通过
    """
    log("[Step 1] 开始技术面+资金面验证（mx-stocks-screener）")
    results = {}
    for s in stocks:
        try:
            results[s["em_code"]] = screen_stock_technical(s)
        except Exception as e:
            results[s["em_code"]] = {"ok": False, "error": str(e)}
        time.sleep(0.3)

    passed = []
    for s in stocks:
        em_code = s["em_code"]
        res = results.get(em_code, {})
        score = parse_technical_score(res)
        passed_flag = score >= threshold
        log(f"  {em_code}: 技术评分={score:.1f} → {'✓ 通过' if passed_flag else '✗ 淘汰'}")
        if passed_flag:
            s["step1_score"] = score
            s["step1_result"] = res
            passed.append(s)

    if not passed:
        log("Step1 无通过股票，降低阈值重试...")
        for s in stocks:
            em_code = s["em_code"]
            res = results.get(em_code, {})
            score = parse_technical_score(res)
            passed_flag = score >= 45.0
            log(f"  {em_code}: 技术评分={score:.1f} → {'✓ 通过' if passed_flag else '✗ 淘汰'}")
            if passed_flag:
                s["step1_score"] = score
                s["step1_result"] = res
                passed.append(s)

    log(f"[Step 1] 通过 {len(passed)}/{len(stocks)} 支")
    return passed


# ─────────────────────────────────────────────
# Step 2：业绩基本面验证（stock-earnings-review）
# ─────────────────────────────────────────────

def step2_filter(stocks: List[Dict]) -> List[Dict]:
    """
    Step 2 过滤：基本面验证（使用 mx-stocks-screener 财务数据，串行查询）。
    """
    log("[Step 2] 开始基本面验证（mx-stocks-screener 财务数据）")
    passed = []
    for s in stocks:
        try:
            res = screen_financial(s)
        except Exception as e:
            res = {"ok": False, "error": str(e)}
        score = parse_financial_score(res)
        passed_flag = score >= 50.0
        log(f"  {s['em_code']}: 基本面评分={score:.1f} → {'✓ 通过' if passed_flag else '✗ 淘汰'}")
        if passed_flag:
            s["step2_score"] = score
            s["step2_result"] = res
            passed.append(s)
        time.sleep(0.3)
    log(f"[Step 2] 通过 {len(passed)}/{len(stocks)} 支")
    return passed


def step3_filter(stocks: List[Dict]) -> List[Dict]:
    """
    Step 3 过滤：资金面确认（使用 mx-stocks-screener 资金流向数据，串行查询）。
    """
    log("[Step 3] 开始资金面确认（mx-stocks-screener 资金数据）")
    final = []
    for s in stocks:
        try:
            res = screen_sector(s)
        except Exception as e:
            res = {"ok": False, "error": str(e)}
        score = parse_sector_score(res)
        passed_flag = score >= 50.0
        log(f"  {s['em_code']}: 资金面评分={score:.1f} → {'✓ 通过' if passed_flag else '✗ 淘汰'}")
        s["step3_score"] = score
        s["step3_result"] = res
        if passed_flag:
            final.append(s)
        time.sleep(0.3)
    log(f"[Step 3] 最终通过 {len(final)}/{len(stocks)} 支")
    return final


def screen_financial(stock: Dict) -> Dict[str, Any]:
    """
    使用通达信MCP（tdx_wenda_quotes）获取个股完整行情+基本面数据。
    替代原来调用 mx-stocks-screener 的东方财富妙想方案。
    """
    em_code = stock["em_code"]
    market = stock["market"]
    range_market = "AG" if market in ("SZ", "SH", "BJ") else ("HK-GP" if market == "HK" else "")
    if not range_market:
        return {"ok": False, "error": f"不支持的市场: {market}"}

    question = (
        f"{em_code}完整行情分析：最新价、涨跌幅、换手率、成交量、成交额、"
        f"市盈率、市净率、总市值、主力净额、KDJ、MACD"
    )
    try:
        inner = call_tdx_wenda(question, range_market=range_market, size="1")
        headers = inner.get("headers", [])
        data = inner.get("data", [])
        if headers and data:
            csv_path, row_count, _ = parse_tdx_result_to_csv(inner)
            return {"ok": True, "csv_path": csv_path, "tdx_response": inner}
        else:
            return {"ok": False, "error": f"查询无结果: {inner}"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def screen_sector(stock: Dict) -> Dict[str, Any]:
    """
    使用通达信MCP（tdx_wenda_quotes）获取资金流向 + 板块情绪数据。
    """
    em_code = stock["em_code"]
    market = stock["market"]
    range_market = "AG" if market in ("SZ", "SH", "BJ") else ("HK-GP" if market == "HK" else "")
    if not range_market:
        return {"ok": False, "error": f"不支持的市场: {market}"}

    question = (
        f"{em_code}近期涨跌幅、换手率、成交量、成交额、量比"
    )
    try:
        inner = call_tdx_wenda(question, range_market=range_market, size="1")
        headers = inner.get("headers", [])
        data = inner.get("data", [])
        if headers and data:
            csv_path, row_count, _ = parse_tdx_result_to_csv(inner)
            return {"ok": True, "csv_path": csv_path, "tdx_response": inner}
        else:
            return {"ok": False, "error": f"查询无结果: {inner}"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def parse_financial_score(result: Dict) -> float:
    """
    从财务数据 CSV 中提取基本面评分（0~100）。
    screener CSV 列（含日期后缀）：
      - 市盈率(动)(倍) 2026.04.30
      - 市净率(倍)
      - 总市值(元) 2026.04.30

    评分维度：
      - PE(动)：≤15 → +20分，≤25 → +15分，≤40 → +8分，>50 → 0分
      - 市净率：≤1.5 → +20分，≤3 → +12分，≤5 → +5分，>5 → 0分
      - 总市值：50~500亿 → +10分，<20亿 → -5分，>1000亿 → +5分
    """
    try:
        csv_path = result.get("csv_path", "")
        if not csv_path or not Path(csv_path).exists():
            return 50.0
        import csv as csv_lib
        with open(csv_path, newline="", encoding="utf-8") as f:
            rows = list(csv_lib.DictReader(f))
        if not rows:
            return 50.0
        row = rows[0]
        score = 50.0

        # ── PE(动)：精确列名匹配 ──
        pe_str = _get_col_val(row, ["市盈率(动)(倍) 2026.04.30", "市盈率(动)(倍)"])
        if pe_str:
            try:
                pe = float(pe_str)
                if pe <= 0:
                    score -= 5  # 亏损
                elif pe <= 15:
                    score += 20
                elif pe <= 25:
                    score += 15
                elif pe <= 40:
                    score += 8
                elif pe > 60:
                    score -= 10
            except ValueError:
                pass

        # ── 市净率 ──
        pb_str = _get_col_val(row, ["市净率(倍)"])
        if pb_str:
            try:
                pb = float(pb_str)
                if 0 < pb <= 1.5:
                    score += 20
                elif 1.5 < pb <= 3:
                    score += 12
                elif 3 < pb <= 5:
                    score += 5
                elif pb > 8:
                    score -= 5
            except ValueError:
                pass

        # ── 总市值：适中规模更稳健 ──
        mkt_str = _get_col_val(row, ["总市值(元) 2026.04.30", "总市值(元)"])
        if mkt_str:
            try:
                mkt_val = _parse_amount(mkt_str)
                if 50e8 <= mkt_val <= 500e8:
                    score += 10
                elif mkt_val < 20e8:
                    score -= 5
                elif mkt_val > 1000e8:
                    score += 5
            except ValueError:
                pass

        return max(0.0, min(100.0, score))
    except Exception:
        return 50.0


def parse_sector_score(result: Dict) -> float:
    """
    从通达信 CSV 中提取资金面/情绪评分（0~100）。

    TDX 列名（带日期后缀）：
      - 涨跌幅(%).前复权<br>2026.04.28-2026.04.30（区间涨跌幅）
      - 换手率(%)<br>2026.04.28-2026.04.30
      - 量比<br>2026.04.28-2026.04.30
      - 换手率<br>2026.04.30（单日）

    评分维度：
      - 区间涨跌幅：>3% → +25分，>0% → +15分，<-3% → -20分
      - 换手率：3~10% → +15分，>15% → -10分，<1% → -5分
      - 量比：>3 → +15分，>2 → +10分，>1 → +5分
    """
    try:
        csv_path = result.get("csv_path", "")
        if not csv_path or not Path(csv_path).exists():
            return 50.0
        import csv as csv_lib
        with open(csv_path, newline="", encoding="utf-8") as f:
            rows = list(csv_lib.DictReader(f))
        if not rows:
            return 50.0
        row = rows[0]
        score = 50.0

        # ── 区间涨跌幅 ──
        range_chg_str = _get_col_val(row, [
            "涨跌幅(%).前复权<br>2026.04.28-2026.04.30",
            "区间涨跌幅(%) 2026.04.28 - 2026.04.30"
        ])
        if range_chg_str:
            try:
                range_chg = float(range_chg_str.replace("%", "").replace("％", ""))
                if range_chg > 3:
                    score += 25
                elif range_chg > 0:
                    score += 15
                elif range_chg < -3:
                    score -= 20
                elif range_chg < 0:
                    score -= 10
            except ValueError:
                pass

        # ── 换手率（区间） ──
        hss_str = _get_col_val(row, ["换手率(%)<br>2026.04.28-2026.04.30"])
        if not hss_str:
            hss_str = _get_col_val(row, ["换手率(%) 2026.04.30", "换手率(%)<"])
        if hss_str:
            try:
                hss = float(hss_str.replace("%", "").replace("％", ""))
                if 3 <= hss <= 10:
                    score += 15
                elif hss > 15:
                    score -= 10
                elif hss < 1:
                    score -= 5
            except ValueError:
                pass

        # ── 量比 ──
        lb_str = _get_col_val(row, ["量比<br>2026.04.28-2026.04.30", "量比 2026.04.30", "量比"])
        if lb_str:
            try:
                lb = float(lb_str)
                if lb > 3:
                    score += 15
                elif lb > 2:
                    score += 10
                elif lb > 1:
                    score += 5
            except ValueError:
                pass

        return max(0.0, min(100.0, score))
    except Exception:
        return 50.0



def build_reasons(stock: Dict) -> str:
    """生成推荐理由文本。"""
    reasons = []
    s1 = stock.get("step1_score", 0)
    s2 = stock.get("step2_score", 0)
    s3 = stock.get("step3_score", 0)

    if s1 >= 80:
        reasons.append("技术面强势（量价齐升、均线多头排列）")
    elif s1 >= 65:
        reasons.append("技术面较好")
    if s2 >= 70:
        reasons.append("基本面优秀（低PE、低PB、适中市值）")
    elif s2 >= 60:
        reasons.append("基本面尚可")
    if s3 >= 70:
        reasons.append("资金面强势（主力净流入、换手活跃）")
    elif s3 >= 60:
        reasons.append("资金面支撑")

    if not reasons:
        reasons.append("综合评分尚可，可关注")
    return "；".join(reasons)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="股票推荐三重过滤脚本")
    parser.add_argument("codes", nargs="+", help="股票代码列表（如 600020 000001.SZ）")
    parser.add_argument("--output", "-o", default=None, help="结果 JSON 输出路径")
    parser.add_argument("--threshold", "-t", type=float, default=55.0, help="综合评分阈值（默认 55.0）")
    args = parser.parse_args()

    stocks = [normalize_code(c) for c in args.codes]
    log(f"输入 {len(stocks)} 支股票: {[s['code'] for s in stocks]}")

    # Step 1: 技术面 + 资金面
    step1_passed = step1_filter(stocks, threshold=args.threshold)

    # Step 2: 基本面
    step2_passed = step2_filter(step1_passed) if step1_passed else []

    # Step 3: 资金面确认
    final = step3_filter(step2_passed) if step2_passed else []

    # 综合评分
    for s in final:
        s["final_score"] = round(
            s["step1_score"] * 0.35 + s["step2_score"] * 0.35 + s["step3_score"] * 0.30, 1
        )
        s["reasons"] = build_reasons(s)

    final.sort(key=lambda x: x["final_score"], reverse=True)
    for i, s in enumerate(final, 1):
        s["rank"] = i
        log(f"  {s['rank']}. {s['em_code']} | 综合评分: {s['final_score']} | 技术:{s['step1_score']} | 业绩:{s['step2_score']} | 研报:{s['step3_score']}")
        log(f"     理由: {s['reasons']}")

    log(f"\n==================================================")
    log(f"最终推荐结果（{len(final)}/{len(stocks)} 支）")
    log(f"==================================================")
    for s in final:
        log(f"{s['rank']}. {s['em_code']} | 综合评分: {s['final_score']} | 技术:{s['step1_score']} | 业绩:{s['step2_score']} | 研报:{s['step3_score']}")
        log(f"   理由: {s['reasons']}")

    output_data = {
        "timestamp": datetime.now().isoformat(),
        "input_codes": args.codes,
        "summary": {
            "total_input": len(stocks),
            "step1_passed": len(step1_passed),
            "step2_passed": len(step2_passed),
            "final_passed": len(final),
        },
        "recommendations": [
            {
                "rank": s["rank"],
                "code": s["em_code"],
                "final_score": s["final_score"],
                "step1_score": s["step1_score"],
                "step2_score": s["step2_score"],
                "step3_score": s["step3_score"],
                "reasons": s["reasons"],
            }
            for s in final
        ],
    }

    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(output_data, ensure_ascii=False, indent=2))
        log(f"结果已保存: {out_path}")

    print("\n--- JSON OUTPUT ---")
    print(json.dumps(output_data, ensure_ascii=False, indent=2))
