#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import math
import os
import sys
from datetime import datetime
from typing import Dict, List, Any

import pandas as pd

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from data_provider import DataFetcherManager
from data_provider.base import normalize_stock_code
from src.stock_analyzer import StockTrendAnalyzer
from src.services.backtest_service import BacktestService
from src.agent.memory import AgentMemory


DEFAULT_STRATEGIES = [
    'bull_trend',
    'ma_golden_cross',
    'shrink_pullback',
    'volume_breakout',
    'bottom_volume',
    'box_oscillation',
    'dragon_head',
    'emotion_cycle',
    'one_yang_three_yin',
    'wave_theory',
    'chan_theory',
]


class CandidateEnhancer:
    def __init__(self, strategy_ids: List[str] | None = None):
        self.strategy_ids = strategy_ids or DEFAULT_STRATEGIES
        self.fetcher = DataFetcherManager()
        self.trend_analyzer = StockTrendAnalyzer()
        self.backtest_service = BacktestService()
        self.memory = AgentMemory.from_config()
        self.strategy_weights = self.memory.compute_strategy_weights(self.strategy_ids, use_backtest=True)

    def load_candidates(self, input_path: str) -> Dict[str, Any]:
        with open(input_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def enhance(self, payload: Dict[str, Any], top_n: int = 10) -> Dict[str, Any]:
        candidate_codes = payload.get('codes') or []
        trade_date = payload.get('trade_date')
        strategy_code_map = self._build_strategy_code_map(payload.get('strategies', {}))

        enhanced_items = []
        for code in candidate_codes:
            try:
                item = self._score_candidate(code, strategy_code_map)
                if item:
                    enhanced_items.append(item)
            except Exception as e:
                enhanced_items.append({
                    'code': code,
                    'status': 'error',
                    'error': str(e),
                    'final_score': 0,
                })

        enhanced_items.sort(key=lambda x: x.get('final_score', 0), reverse=True)
        top_items = [x for x in enhanced_items if x.get('status') == 'ok'][:top_n]

        return {
            'trade_date': trade_date,
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'strategy_weights': self.strategy_weights,
            'candidate_count': len(candidate_codes),
            'scored_count': len([x for x in enhanced_items if x.get('status') == 'ok']),
            'top_n': top_n,
            'top_candidates': top_items,
            'all_candidates': enhanced_items,
        }

    def _build_strategy_code_map(self, strategies_payload: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[str]]:
        result: Dict[str, List[str]] = {}
        for strategy_name, records in (strategies_payload or {}).items():
            codes = []
            for record in records or []:
                code = str(record.get('code', '')).strip()
                if code:
                    codes.append(code)
                    codes.append(normalize_stock_code(code))
            result[strategy_name] = sorted(set(codes))
        return result

    def _build_quote_from_daily(self, code: str, df: pd.DataFrame):
        if df is None or df.empty:
            return None
        latest = df.sort_values('date').iloc[-1]
        prev = df.sort_values('date').iloc[-2] if len(df) >= 2 else latest

        class QuoteSnapshot:
            pass

        q = QuoteSnapshot()
        q.code = code
        q.name = code
        q.price = self._safe_float(latest.get('close'))
        q.change_pct = self._safe_float(latest.get('pct_chg'))
        if q.change_pct == 0 and self._safe_float(prev.get('close')) > 0:
            q.change_pct = round((q.price - self._safe_float(prev.get('close'))) / self._safe_float(prev.get('close')) * 100, 2)
        q.change_amount = round(q.price - self._safe_float(prev.get('close')), 4)
        q.volume = self._safe_float(latest.get('volume'))
        q.amount = self._safe_float(latest.get('amount'))
        avg_vol_5 = self._safe_float(df.tail(5)['volume'].mean())
        q.volume_ratio = round(q.volume / avg_vol_5, 2) if avg_vol_5 else 1.0
        q.turnover_rate = self._safe_float(latest.get('turn'))
        q.amplitude = round((self._safe_float(latest.get('high')) - self._safe_float(latest.get('low'))) / max(self._safe_float(prev.get('close')), 0.01) * 100, 2)
        q.open_price = self._safe_float(latest.get('open'))
        q.high = self._safe_float(latest.get('high'))
        q.low = self._safe_float(latest.get('low'))
        q.pre_close = self._safe_float(prev.get('close'))
        q.pe_ratio = None
        q.pb_ratio = None
        q.total_mv = None
        q.circ_mv = None
        q.change_60d = None
        q.source = 'daily_fallback'
        return q

    def _safe_float(self, value, default=0.0):
        try:
            if value is None:
                return default
            if isinstance(value, float) and math.isnan(value):
                return default
            return float(value)
        except Exception:
            return default

    def _score_candidate(self, code: str, strategy_code_map: Dict[str, List[str]]) -> Dict[str, Any]:
        normalized_code = normalize_stock_code(code)
        df, source = self.fetcher.get_daily_data(normalized_code, days=180)
        if df is None or df.empty or len(df) < 60:
            raise RuntimeError(f'历史数据不足: {code}')

        quote = self.fetcher.get_realtime_quote(normalized_code)
        if quote is None:
            quote = self._build_quote_from_daily(normalized_code, df)

        if quote is None:
            raise RuntimeError(f'无法构造行情快照: {code}')

        if df is None or df.empty or len(df) < 60:
            raise RuntimeError(f'历史数据不足: {code}')

        trend = self.trend_analyzer.analyze(df.copy(), normalized_code)
        chip = self.fetcher.get_chip_distribution(normalized_code)
        stock_info = self.fetcher.get_fundamental_context(normalized_code)
        stock_summary = self.backtest_service.get_stock_summary(normalized_code)

        matched_source_strategies = sorted([name for name, codes in strategy_code_map.items() if code in codes or normalized_code in codes])

        score_breakdown = {}
        strategy_hits = {}

        trend_score = self._score_trend(trend)
        score_breakdown['trend'] = trend_score

        position_score = self._score_position(trend, quote)
        score_breakdown['position'] = position_score

        chip_score = self._score_chip(chip)
        score_breakdown['chip'] = chip_score

        volume_pattern_score, volume_flags = self._score_volume_pattern(df, quote, trend)
        score_breakdown['volume_pattern'] = volume_pattern_score

        risk_penalty, risk_flags = self._score_risk(trend, quote, stock_info, chip)
        score_breakdown['risk_penalty'] = risk_penalty

        backtest_score = self._score_stock_backtest(stock_summary)
        score_breakdown['stock_backtest'] = backtest_score

        for strategy_id in self.strategy_ids:
            hit_score, hit_reason = self._evaluate_strategy_hit(strategy_id, df, quote, trend, chip, stock_info, matched_source_strategies)
            weighted = round(hit_score * self.strategy_weights.get(strategy_id, 1.0), 2)
            strategy_hits[strategy_id] = {
                'raw_score': hit_score,
                'weighted_score': weighted,
                'reason': hit_reason,
            }

        strategy_total = round(sum(x['weighted_score'] for x in strategy_hits.values()), 2)
        score_breakdown['strategy_total'] = strategy_total

        final_score = trend_score + position_score + chip_score + volume_pattern_score + backtest_score + strategy_total - risk_penalty
        final_score = max(0, min(100, round(final_score, 2)))

        confidence = self._estimate_confidence(final_score, stock_summary)
        action = self._decide_action(final_score, trend, risk_penalty)

        return {
            'status': 'ok',
            'code': normalized_code,
            'name': quote.name,
            'price': quote.price,
            'change_pct': quote.change_pct,
            'turnover_rate': quote.turnover_rate,
            'volume_ratio': quote.volume_ratio,
            'trend_status': trend.trend_status.value,
            'buy_signal': trend.buy_signal.value,
            'signal_score': trend.signal_score,
            'matched_source_strategies': matched_source_strategies,
            'score_breakdown': score_breakdown,
            'strategy_hits': strategy_hits,
            'risk_flags': risk_flags,
            'volume_flags': volume_flags,
            'final_score': final_score,
            'profit_probability': confidence,
            'action': action,
            'avg_cost': None if chip is None else chip.avg_cost,
            'chip_profit_ratio': None if chip is None else chip.profit_ratio,
            'stock_backtest': stock_summary,
            'summary': self._build_summary(action, final_score, trend, matched_source_strategies, risk_flags),
            'data_source': source,
        }

    def _score_trend(self, trend) -> float:
        score = 0.0
        if trend.trend_status.value in ('强势多头', '多头排列'):
            score += 18
        elif trend.trend_status.value == '弱势多头':
            score += 10
        elif trend.trend_status.value == '盘整':
            score += 6
        else:
            score += 1

        if trend.macd_status.value in ('零轴上金叉', '金叉', '多头'):
            score += 6
        elif trend.macd_status.value == '空头':
            score -= 4

        if trend.rsi_status.value in ('强势买入', '中性'):
            score += 4
        elif trend.rsi_status.value == '超买':
            score -= 2
        elif trend.rsi_status.value == '超卖':
            score += 1

        if trend.current_price >= trend.ma20:
            score += 4
        else:
            score -= 3

        return round(max(0, score), 2)

    def _score_position(self, trend, quote) -> float:
        bias_ma5 = self._safe_float(trend.bias_ma5)
        score = 0.0
        if abs(bias_ma5) <= 2:
            score += 10
        elif abs(bias_ma5) <= 5:
            score += 6
        else:
            score += 1

        if trend.support_ma5 or trend.support_ma10:
            score += 4

        if self._safe_float(quote.change_pct) > 7:
            score -= 5
        elif self._safe_float(quote.change_pct) < -3:
            score -= 1
        return round(max(0, score), 2)

    def _score_chip(self, chip) -> float:
        if chip is None:
            return 3.0
        score = 0.0
        concentration_90 = self._safe_float(chip.concentration_90)
        profit_ratio = self._safe_float(chip.profit_ratio)
        avg_cost = self._safe_float(chip.avg_cost)
        if concentration_90 < 0.15:
            score += 6
        elif concentration_90 < 0.22:
            score += 3
        if 0.25 <= profit_ratio <= 0.75:
            score += 5
        elif profit_ratio < 0.15:
            score += 2
        if avg_cost > 0:
            score += 2
        return round(score, 2)

    def _score_volume_pattern(self, df: pd.DataFrame, quote, trend) -> tuple[float, List[str]]:
        flags: List[str] = []
        score = 0.0
        if self._safe_float(quote.volume_ratio) >= 1.5:
            score += 4
            flags.append('量比活跃')
        if trend.volume_status.value in ('放量上涨', '缩量回调'):
            score += 5
            flags.append(trend.volume_status.value)
        if trend.volume_status.value == '放量下跌':
            score -= 4
            flags.append('放量下跌')

        recent = df.tail(6).copy()
        if len(recent) >= 6:
            latest = recent.iloc[-1]
            prev5 = recent.iloc[:-1]
            avg_vol = prev5['volume'].mean()
            if avg_vol and latest['volume'] > avg_vol * 1.8 and latest['close'] > prev5['high'].max() * 0.98:
                score += 6
                flags.append('近端放量突破迹象')
            if avg_vol and latest['volume'] < avg_vol * 0.75 and latest['close'] >= trend.ma10 * 0.99:
                score += 4
                flags.append('缩量回踩迹象')
        return round(max(0, score), 2), flags

    def _score_risk(self, trend, quote, stock_info, chip) -> tuple[float, List[str]]:
        penalty = 0.0
        flags: List[str] = []
        if self._safe_float(trend.bias_ma5) > 5:
            penalty += 8
            flags.append('偏离MA5过大，疑似追高')
        if trend.current_price < trend.ma20:
            penalty += 6
            flags.append('跌破MA20')
        if trend.macd_status.value in ('死叉', '空头'):
            penalty += 4
            flags.append('MACD偏空')
        pe_ratio = self._safe_float((stock_info or {}).get('valuation', {}).get('data', {}).get('pe_ratio'))
        if pe_ratio > 120:
            penalty += 4
            flags.append('估值偏高')
        if self._safe_float(quote.turnover_rate) > 12:
            penalty += 3
            flags.append('换手过热')
        if chip is not None and self._safe_float(chip.profit_ratio) > 0.85:
            penalty += 3
            flags.append('获利盘过多')
        return round(penalty, 2), flags

    def _score_stock_backtest(self, stock_summary: Dict[str, Any] | None) -> float:
        if not stock_summary:
            return 3.0
        total = int(stock_summary.get('total_evaluations') or 0)
        win_rate = self._safe_float(stock_summary.get('win_rate'))
        avg_return = self._safe_float(stock_summary.get('avg_return'))
        if total < 5:
            return 4.0
        score = 0.0
        if win_rate >= 0.6:
            score += 6
        elif win_rate >= 0.5:
            score += 4
        else:
            score += 1
        if avg_return > 0.03:
            score += 4
        elif avg_return > 0:
            score += 2
        return round(score, 2)

    def _evaluate_strategy_hit(self, strategy_id: str, df: pd.DataFrame, quote, trend, chip, stock_info, matched_source_strategies: List[str]) -> tuple[float, str]:
        price = self._safe_float(quote.price)
        vr = self._safe_float(quote.volume_ratio)
        turnover = self._safe_float(quote.turnover_rate)
        hit = 0.0
        reason = '未命中'

        if strategy_id == 'bull_trend':
            if trend.trend_status.value in ('强势多头', '多头排列'):
                hit, reason = 8, '标准多头趋势'
            elif trend.trend_status.value == '弱势多头':
                hit, reason = 5, '弱多头趋势'
        elif strategy_id == 'ma_golden_cross':
            if trend.ma5 > trend.ma10 > trend.ma20 and trend.macd_status.value in ('金叉', '零轴上金叉', '多头'):
                hit, reason = 8, '均线与MACD共振'
            elif trend.ma5 > trend.ma10:
                hit, reason = 4, '短均线占优'
        elif strategy_id == 'shrink_pullback':
            if trend.trend_status.value in ('强势多头', '多头排列') and abs(self._safe_float(trend.bias_ma5)) <= 2 and trend.volume_status.value == '缩量回调':
                hit, reason = 9, '缩量回踩MA5/10'
            elif abs(self._safe_float(trend.bias_ma10)) <= 2:
                hit, reason = 4, '接近MA10支撑'
        elif strategy_id == 'volume_breakout':
            if vr >= 2 and price >= max(trend.resistance_levels or [0]):
                hit, reason = 9, '放量突破阻力'
            elif vr >= 1.5:
                hit, reason = 4, '量能开始放大'
        elif strategy_id == 'bottom_volume':
            drawdown = (df['close'].tail(20).max() - df['close'].tail(20).min()) / max(df['close'].tail(20).max(), 0.01)
            if drawdown > 0.15 and vr >= 2.5 and self._safe_float(quote.change_pct) > 0:
                hit, reason = 7, '底部放量反转特征'
        elif strategy_id == 'box_oscillation':
            recent = df.tail(60)
            low = recent['low'].quantile(0.1)
            high = recent['high'].quantile(0.9)
            if low > 0 and (price - low) / low <= 0.05:
                hit, reason = 7, '接近箱体底部'
            elif high > 0 and abs(price - high) / high <= 0.05:
                hit, reason = 2, '接近箱体顶部'
        elif strategy_id == 'dragon_head':
            if turnover > 5 and vr > 1.5:
                hit, reason = 7, '换手与量比符合龙头活跃特征'
        elif strategy_id == 'emotion_cycle':
            if turnover < 1 and vr < 0.8:
                hit, reason = 6, '情绪冷淡，可能处于低位'
            elif turnover > 8:
                hit, reason = 2, '情绪偏热'
        elif strategy_id == 'one_yang_three_yin':
            recent = df.tail(5).reset_index(drop=True)
            if len(recent) == 5:
                cond = recent.iloc[0]['close'] > recent.iloc[0]['open'] and recent.iloc[4]['close'] > recent.iloc[4]['open']
                middle = recent.iloc[1:4]
                if cond and (middle['close'] < middle['open']).sum() >= 2:
                    hit, reason = 6, '近5日类似一阳夹三阴'
        elif strategy_id == 'wave_theory':
            if trend.trend_status.value in ('多头排列', '强势多头') and self._safe_float(trend.bias_ma5) < 3:
                hit, reason = 5, '疑似推动浪初期'
            elif trend.current_price < trend.ma20:
                hit, reason = 1, '疑似调整浪中'
        elif strategy_id == 'chan_theory':
            if trend.macd_bar > 0 and trend.current_price >= trend.ma10 and trend.current_price <= trend.ma20 * 1.03:
                hit, reason = 6, '疑似二买/三买区'
            elif trend.macd_status.value in ('死叉', '空头'):
                hit, reason = 1, '缠论视角偏弱'

        if strategy_id in matched_source_strategies:
            hit += 2
            reason += ' + 原始策略入选'

        return round(hit, 2), reason

    def _estimate_confidence(self, final_score: float, stock_summary: Dict[str, Any] | None) -> float:
        base = min(0.9, max(0.2, final_score / 100))
        if stock_summary and int(stock_summary.get('total_evaluations') or 0) >= 10:
            base += min(0.08, self._safe_float(stock_summary.get('win_rate')) * 0.1)
        return round(min(0.95, base), 4)

    def _decide_action(self, final_score: float, trend, risk_penalty: float) -> str:
        if trend.current_price < trend.ma20 or risk_penalty >= 12:
            return '观察'
        if final_score >= 75:
            return '优先关注'
        if final_score >= 60:
            return '可跟踪'
        return '观察'

    def _build_summary(self, action: str, final_score: float, trend, matched_source_strategies: List[str], risk_flags: List[str]) -> str:
        source_text = '、'.join(matched_source_strategies[:3]) if matched_source_strategies else '候选池'
        risk_text = '；'.join(risk_flags[:2]) if risk_flags else '风险可控'
        return f'{action}，总分{final_score}，{trend.trend_status.value}，来源于{source_text}，{risk_text}'


def save_outputs(result: Dict[str, Any], output_json: str, output_csv: str | None = None):
    os.makedirs(os.path.dirname(output_json), exist_ok=True)
    with open(output_json, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    if output_csv:
        rows = []
        for item in result.get('top_candidates', []):
            rows.append({
                'code': item.get('code'),
                'name': item.get('name'),
                'price': item.get('price'),
                'change_pct': item.get('change_pct'),
                'final_score': item.get('final_score'),
                'profit_probability': item.get('profit_probability'),
                'action': item.get('action'),
                'trend_status': item.get('trend_status'),
                'buy_signal': item.get('buy_signal'),
                'summary': item.get('summary'),
            })
        pd.DataFrame(rows).to_csv(output_csv, index=False, encoding='utf-8-sig')


def main():
    parser = argparse.ArgumentParser(description='候选股增强融合评分')
    parser.add_argument('--input', required=True, help='strategy.py 导出的候选JSON')
    parser.add_argument('--output', required=True, help='增强结果JSON输出路径')
    parser.add_argument('--output_csv', help='增强结果CSV输出路径')
    parser.add_argument('--top_n', type=int, default=10, help='输出前N只股票')
    parser.add_argument('--strategies', nargs='*', help='指定融合策略ID列表')
    args = parser.parse_args()

    enhancer = CandidateEnhancer(strategy_ids=args.strategies or DEFAULT_STRATEGIES)
    payload = enhancer.load_candidates(args.input)
    result = enhancer.enhance(payload, top_n=args.top_n)
    save_outputs(result, args.output, args.output_csv)
    print(json.dumps({
        'status': 'ok',
        'candidate_count': result.get('candidate_count'),
        'scored_count': result.get('scored_count'),
        'top_n': len(result.get('top_candidates', [])),
        'output': args.output,
        'output_csv': args.output_csv,
    }, ensure_ascii=False))


if __name__ == '__main__':
    main()
