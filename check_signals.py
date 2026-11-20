# -*- coding: utf-8 -*-
"""
持仓信号监控（模拟盘安全网）
================================
用法：python check_signals.py [--no-pause]
数据：纯本地（K线缓存 + snapshot + emotion + positions.csv），零网络请求

规则实现（出处）：
  止损/补仓/棘轮：07-模拟盘跟踪/CLAUDE.md §3（2026-07-19 用户裁决）
  放量滞涨减半/双信号清仓：技术面报告 §2.4#1（出货最核心信号）
  洗盘三维判定：技术面报告 §2.7 + §1.2（有效跌破=收盘破幅>1%且3日不收回）
  长上影告警：技术面报告 §2.4#3
  情绪四阶段：技术面报告 §3.3 + 信息面框架 §4.5
  事件仓（京蓝）：跳过量价信号，只查 -15% 止损线，公告新闻由日报人工补
"""
import json
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
DATA = BASE_DIR / "data"
ACCOUNT_DIR = BASE_DIR.parent.parent / "07-模拟盘跟踪" / "01-账户"
POS_PATH = ACCOUNT_DIR / "positions.csv"
ACCOUNT_JSON = ACCOUNT_DIR / "account.json"
ACCOUNT_MD = ACCOUNT_DIR / "账户状态.md"
OUT_DIR = BASE_DIR.parent.parent / "07-模拟盘跟踪" / "03-日报"
AUTO_BEGIN, AUTO_END = "<!-- AUTO-BEGIN -->", "<!-- AUTO-END -->"

TOP_SHADOW_PCT = 0.03    # 长上影定义：上影线>3%
TOP_SHADOW_DAYS = 3      # 近10日≥3天=告警
BREAK_CONFIRM = 0.01     # 有效跌破=收盘破幅>1%
WASH_MAX_DAYS = 7        # 洗盘时长上限


def load_kline(code: str) -> pd.DataFrame:
    return pd.read_csv(DATA / "kline" / f"{code}.csv")


def check_position(row: pd.Series) -> dict:
    """单持仓信号检查，返回信号字典"""
    code, name, ptype = row["代码"], row["名称"], row["类型"]
    cost, shares = float(row["成本价"]), int(row["股数"])
    platform_old = float(row["平台低点"])
    df = load_kline(code)
    close = df["收盘"].iloc[-1]
    date = df["日期"].iloc[-1]

    # 棘轮平台低点：只上不下（用户裁决2026-07-19）
    platform_new = max(platform_old, df["最低"].tail(20).min())
    pnl = (close / cost - 1) * 100
    r = {"代码": code, "名称": name, "类型": ptype, "日期": date, "现价": close,
         "股数": shares, "盈亏%": round(pnl, 1), "平台低点": round(platform_new, 2),
         "棘轮变动": "↑" if platform_new > platform_old else "",
         "信号": [], "动作": "持有"}

    if ptype == "事件":
        # 事件仓：只查-15%止损线，量价信号不适用；平台低点不棘轮（显示与账目一致）
        stop = cost * 0.85
        r["平台低点"] = round(platform_old, 2)
        r["棘轮变动"] = ""
        r["止损线"] = round(stop, 2)
        if close < stop:
            r["动作"] = "清仓"
            r["信号"].append(f"跌破事件止损线{stop:.2f}（成本-15%）")
        return r

    r["止损线"] = round(platform_new, 2)

    # ① 止损：收盘跌破棘轮平台
    if close < platform_new:
        r["动作"] = "清仓"
        r["信号"].append(f"收盘{close}跌破棘轮平台{platform_new:.2f}，无条件止损")
        return r

    # ② 补仓：未补过 且 收盘在平台上方0~2%区间企稳
    if row["补仓状态"] == "未补" and platform_new <= close <= platform_new * 1.02:
        r["动作"] = "补仓"
        r["信号"].append(f"回踩平台{platform_new:.2f}上方企稳（{close}），补至10万满额")

    # ③ 放量滞涨：近5日量创60日新高 且 收盘价5日不创新高
    # 位置过滤（2026-07-19 首跑修正）：低位（250日位置<60%）的"量增价平"=C1吸筹形态本身，
    # 只有相对高位的放量滞涨才判出货（技术面§2.4#1语境为高位）
    w5, w60 = df.tail(5), df.tail(60)
    w250 = df.tail(250)
    pos250 = (close - w250["最低"].min()) / max(w250["最高"].max() - w250["最低"].min(), 1e-9)
    if pos250 > 0.60 and w5["成交量"].max() >= w60["成交量"].max() and close < w5["收盘"].max():
        r["信号"].append(f"高位放量滞涨（量创60日新高+价不新高，位置{pos250*100:.0f}%）→ 减半告警")
        if r["动作"] == "持有":
            r["动作"] = "减半"

    # ④ 长上影频率：近10日上影>3%天数≥3
    w10 = df.tail(10)
    upper = (w10["最高"] - w10[["开盘", "收盘"]].max(axis=1)) / w10["收盘"]
    if (upper > TOP_SHADOW_PCT).sum() >= TOP_SHADOW_DAYS:
        r["信号"].append(f"长上影{(upper > TOP_SHADOW_PCT).sum()}天/10日（出货嫌疑）")

    # ⑤ 洗盘判定（5日跌>5%时触发）
    chg5 = close / df["收盘"].iloc[-6] - 1 if len(df) > 5 else 0
    if chg5 < -0.05:
        down = df.tail(min(WASH_MAX_DAYS, 7))
        vol_shrink = down["成交量"].mean() < df.iloc[-27:-7]["成交量"].mean()
        # 有效跌破=收盘破平台>1% 且 连续3日不收回
        broke = (df["收盘"].tail(3) < platform_new * (1 - BREAK_CONFIRM)).all()
        if vol_shrink and not broke:
            r["信号"].append("急跌缩量+未有效破平台=洗盘特征，持有（止损线不变）")
        elif broke:
            r["信号"].append("有效跌破平台（收盘破1%且3日未收回）=真跌嫌疑")

    return r


def market_regime() -> dict:
    """大盘过滤器（信息面框架§1：宏观定仓位）。新浪指数源，不依赖东财。
    判定结果决定每日操作指引：恐慌冻结补仓/下跌从严/企稳正常"""
    import akshare as ak
    idx = ak.stock_zh_index_daily(symbol="sh000300")
    close = idx["close"].iloc[-1]
    w250 = idx.tail(250)
    pos = (close - w250["low"].min()) / max(w250["high"].max() - w250["low"].min(), 1e-9) * 100
    c5 = close / idx["close"].iloc[-6] - 1
    c20 = close / idx["close"].iloc[-21] - 1
    vr = idx.tail(20)["volume"].mean() / idx.iloc[-40:-20]["volume"].mean()
    if c5 < -0.04 or c20 < -0.08:
        regime, guide = "恐慌急跌", "冻结：暂停补仓/新买入，只持有+盯止损"
    elif c20 < -0.04:
        regime, guide = "下跌段", "从严：观察仓不补，核心仓减半信号从严"
    elif pos < 30 and c5 > 0:
        regime, guide = "低位企稳", "正常：冰点分批建仓窗口"
    elif c20 > 0.03:
        regime, guide = "上升段", "正常：持有为主，不追高"
    else:
        regime, guide = "震荡", "正常"
    return {"日期": str(idx["date"].iloc[-1])[:10], "收盘": round(close, 0),
            "250日位置%": round(pos, 0), "5日%": round(c5 * 100, 1),
            "20日%": round(c20 * 100, 1), "量比": round(vr, 2),
            "状态": regime, "仓位指引": guide}


def emotion_meter() -> dict:
    """情绪温度计：技术面§3.3 四阶段（数据：emotion/ 最新一天三池）"""
    files = sorted((DATA / "emotion").glob("*-zt.csv"))
    if not files:
        return {}
    day = files[-1].stem[:8]
    zt = pd.read_csv(DATA / "emotion" / f"{day}-zt.csv")
    dt = pd.read_csv(DATA / "emotion" / f"{day}-dt.csv")
    zb = pd.read_csv(DATA / "emotion" / f"{day}-zb.csv")
    n_zt, n_dt = len(zt), len(dt)
    max_lb = int(zt["连板数"].max()) if "连板数" in zt.columns and len(zt) else 0
    zb_rate = len(zb) / max(len(zt) + len(zb), 1)
    # 判定（§3.3表多维简化 + 2026-07-19首跑修正：跌停极值优先）
    if n_dt > 150:
        stage = "冰点" if n_zt < 40 else "退潮"   # 跌停>150=弱势极值，涨停多少都暖不了
    elif n_zt < 20 and n_dt > 50:
        stage = "冰点"
    elif n_zt > 80 and zb_rate < 0.20:
        stage = "高潮"
    elif n_zt < 30 and n_dt > 30:
        stage = "退潮"
    else:
        stage = "发酵"
    return {"日期": day, "涨停": n_zt, "跌停": n_dt, "最高连板": max_lb,
            "炸板率%": round(zb_rate * 100, 1), "阶段": stage}


def render_account(results: list[dict]):
    """从positions+最新价自动生成账户状态.md的AUTO区（账本SSOT规则，2026-07-19用户确认）"""
    acc = json.loads(ACCOUNT_JSON.read_text(encoding="utf-8"))
    cash, init = acc["现金"], acc["初始资金"]
    pos_val = sum(r["现价"] * r["股数"] for r in results)
    total, pnl = cash + pos_val, cash + pos_val - init
    date = results[0]["日期"]

    sys_rows = [r for r in results if r["类型"] != "事件"]
    evt_rows = [r for r in results if r["类型"] == "事件"]

    def fmt(rows, cols):
        lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
        for r in rows:
            mv = r["现价"] * r["股数"]
            lines.append(f"| {r['代码']} | {r['名称']} | {r['现价']} | {r['股数']:,} | "
                         f"{mv:,.0f} | {r['盈亏%']}% | {r.get('止损线', r['平台低点'])} | {r['动作']} |")
        return "\n".join(lines)

    auto = f"""# 账户状态（check_signals.py 自动生成，禁止手改本区）

> 数据截至：{date} ｜ 源：positions.csv + account.json + 最新K线

## 总览

| 项 | 金额 |
|---|---|
| 初始资金 | ¥{init:,.0f} |
| 持仓市值 | ¥{pos_val:,.0f} |
| 现金 | ¥{cash:,.0f} |
| **总资产** | **¥{total:,.0f}** |
| 累计盈亏 | **{pnl:+,.0f}（{pnl/init*100:+.2f}%）** |
| 仓位 | {pos_val/total*100:.1f}% |

## 体系仓（{len(sys_rows)}只）

{fmt(sys_rows, ["代码","名称","现价","股数","市值","盈亏%","止损线","动作"])}

## 事件仓（单独核算）

{fmt(evt_rows, ["代码","名称","现价","股数","市值","盈亏%","止损线","动作"])}

"""
    md = ACCOUNT_MD.read_text(encoding="utf-8") if ACCOUNT_MD.exists() else f"{AUTO_BEGIN}\n{AUTO_END}\n"
    if AUTO_BEGIN in md and AUTO_END in md:
        pre, _, rest = md.partition(AUTO_BEGIN)
        _, _, post = rest.partition(AUTO_END)
        md = pre + AUTO_BEGIN + "\n" + auto + AUTO_END + post
    else:
        md = md.rstrip() + f"\n\n{AUTO_BEGIN}\n{auto}{AUTO_END}\n"
    ACCOUNT_MD.write_text(md, encoding="utf-8")
    print(f"[账本] 账户状态.md AUTO区已更新（总资产 {total:,.0f}元，{pnl:+,.0f}元）", flush=True)


def main():
    pos = pd.read_csv(POS_PATH, dtype={"代码": str})
    results = [check_position(row) for _, row in pos.iterrows()]
    emo = emotion_meter()
    try:
        mkt = market_regime()
    except Exception as e:
        mkt = {}
        print(f"[大盘] 数据获取失败（{type(e).__name__}），今日跳过环境判定", flush=True)

    # 棘轮平台低点回写（事件仓不回写：其止损线为成本-15%固定值，非棘轮平台）
    for i, r in enumerate(results):
        if r["类型"] != "事件":
            pos.loc[i, "平台低点"] = r["平台低点"]
    pos.to_csv(POS_PATH, index=False, encoding="utf-8-sig")

    # 账户状态.md 自动生成（AUTO区）
    render_account(results)

    # 输出
    lines = []
    if mkt:
        lines.append(f"大盘环境（沪深300，{mkt['日期']}）：{mkt['收盘']:.0f}点 "
                     f"250日位置{mkt['250日位置%']:.0f}% 5日{mkt['5日%']:+.1f}% 20日{mkt['20日%']:+.1f}% "
                     f"→ 【{mkt['状态']}】{mkt['仓位指引']}")
    if emo:
        lines.append(f"情绪温度计（{emo['日期']}）：涨停{emo['涨停']} 跌停{emo['跌停']} "
                     f"连板{emo['最高连板']} 炸板率{emo['炸板率%']}% → 【{emo['阶段']}】")
    lines.append("")
    df_out = pd.DataFrame(results)
    lines.append(df_out[["代码", "名称", "类型", "现价", "盈亏%", "平台低点", "棘轮变动", "动作"]].to_string(index=False))
    lines.append("")
    for r in results:
        if r["信号"]:
            lines.append(f"[告警] {r['名称']}({r['代码']}): " + "；".join(r["信号"]))
    text = "\n".join(lines)
    print(text, flush=True)

    day = results[0]["日期"].replace("-", "")
    (OUT_DIR).mkdir(parents=True, exist_ok=True)
    (OUT_DIR / f"{day}-信号检查.txt").write_text(text, encoding="utf-8")
    print(f"\n已存 03-日报/{day}-信号检查.txt", flush=True)
    if "--no-pause" not in sys.argv:
        input("按回车退出...")


if __name__ == "__main__":
    main()
