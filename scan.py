# -*- coding: utf-8 -*-
"""
选股流水线 · 全市场扫描脚本
================================
用法：
  python scan.py            # 默认 quick：快照粗筛 → 精筛（每日用这个）
  python scan.py quick      # 同上
  python scan.py update     # 全量更新K线缓存（5500只，慢，仅首次/重建用）
  python scan.py scan       # 只扫描（基于现有缓存，全池）
  加 --no-pause 结尾不等待回车（供脚本调用）

数据源：腾讯K线直连API（主，web.ifzq.gtimg.cn，实测2026-07-19可用）
        东财历史K线 ak.stock_zh_a_hist（备份，风控恢复后自动接管失败重试）
  踩坑：东财快照接口（stock_zh_a_spot_em）及K线接口对频繁调用会临时封IP
       （RemoteDisconnected），故全量走腾讯源；东财仅作单只失败时的降级。

条件 C1「量增价平」（吸筹信号）
  出处：02-研究报告/技术面分析与抓庄手法深度研究-2026-06-01.md §2.1（报告评级：最可靠#1）
  量化定义：
    1. 低位：收盘价处于近250日区间下 40%
    2. 横盘：近20日振幅 < 15% 且 |20日累计涨跌幅| < 5%
    3. 量增：近20日均量 > 前20日均量 × 1.3
  排除：ST/退市股、上市不足290个交易日、北交所（4/8/920开头）
"""
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import akshare as ak
import pandas as pd
import requests

# ---------- 路径 ----------
BASE_DIR = Path(__file__).resolve().parent          # 02-扫描脚本/
KLINE_DIR = BASE_DIR / "data" / "kline"             # K线缓存
RESULT_DIR = BASE_DIR.parent / "03-选股结果"         # 输出
KLINE_DIR.mkdir(parents=True, exist_ok=True)
RESULT_DIR.mkdir(parents=True, exist_ok=True)

# ---------- 参数 ----------
HISTORY_DAYS = 500        # 首次下载拉500个自然日（≈340交易日，覆盖250日窗口+40日量窗）
REFRESH_DAYS = 10         # 增量更新时回补最近10天（防漏交易日）
MAX_WORKERS = 16          # 并发线程（腾讯源耐操；若失败率上升就调低）
EXCLUDE_PREFIX = ("4", "8", "920")   # 北交所排除

TX_URL = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
STD_COLS = ["日期", "开盘", "收盘", "最高", "最低", "成交量"]   # 统一中文列名（量=手）

# C1 条件阈值（改动必须写迭代记录！）
POS_MAX = 0.40      # 250日位置上限
AMP_MAX = 0.15      # 20日振幅上限
CHG_MAX = 0.05      # 20日累计涨跌幅上限（绝对值）
VOL_RATIO_MIN = 1.3 # 量比下限（近20日均量/前20日均量）

# 粗筛参数（快照层，改动必须写迭代记录！）
# 2026-10-15 放宽：POS 40→50、VR 1.2→1.0（08中台用户拍板；粗筛只预滤，C1精筛 POS_MAX/VOL_RATIO_MIN 兜底，放宽只增计算量不掺信号）
ROUGH_POS = 50          # 52周位置上限（%）：粗筛只替体系总纲把"明显高位"大门，40%那根线是C1精筛 POS_MAX 的职责
ROUGH_TURN = (0.3, 10)  # 换手率区间（排除死股/过热股，工程参数）
ROUGH_VR = 1.0          # 当日量比下限（量增弱代理，可能错杀，复盘评估漏网率）
ROUGH_PRICE = 2         # 排除仙股
ROUGH_MCAP = 20         # 流通市值下限（亿），体系总纲排除规则


def fetch_snapshot(codes: list[str]) -> pd.DataFrame:
    """腾讯批量快照（80只/批）：52周高低/换手/量比/市值，粗筛专用"""
    rows = []
    for i in range(0, len(codes), 80):
        batch = codes[i:i + 80]
        try:
            r = requests.get("https://qt.gtimg.cn/q=" + ",".join(_tx_symbol(c) for c in batch),
                             headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
            for line in r.text.strip().split(";"):
                line = line.strip()
                if "~" not in line:
                    continue
                p = line.split("~")
                if len(p) < 50:
                    continue
                try:
                    rows.append({"代码": p[2], "名称": p[1], "现价": float(p[3] or 0),
                                 "换手%": float(p[38] or 0), "流通市值亿": float(p[44] or 0),
                                 "52周高": float(p[47] or 0), "52周低": float(p[48] or 0),
                                 "量比": float(p[49] or 0)})
                except ValueError:
                    continue
        except Exception as e:
            print(f"  快照批{i//80}失败: {type(e).__name__}")
        time.sleep(0.15)
    df = pd.DataFrame(rows)
    df = df[(df["现价"] > 0) & (df["52周高"] > df["52周低"])]
    df["52周位置%"] = ((df["现价"] - df["52周低"]) / (df["52周高"] - df["52周低"]) * 100).round(1)
    return df


def get_universe() -> pd.DataFrame:
    """全市场股票列表（沪深A股，排除北交所）。本地缓存7天内有效（列表变化极慢，不必每天拉）"""
    cache = BASE_DIR / "data" / "universe.csv"
    if cache.exists() and (time.time() - cache.stat().st_mtime) < 7 * 86400:
        df = pd.read_csv(cache, dtype={"code": str})
    else:
        df = ak.stock_info_a_code_name()
        df.to_csv(cache, index=False, encoding="utf-8-sig")
    df = df[~df["code"].str.startswith(EXCLUDE_PREFIX)]
    return df.reset_index(drop=True)


def _tx_symbol(code: str) -> str:
    return ("sh" if code.startswith(("60", "68")) else "sz") + code


def fetch_tx(code: str, start: str, end: str) -> pd.DataFrame:
    """腾讯K线直连（主源）。start/end 格式 YYYYMMDD，返回统一中文列"""
    s = f"{start[:4]}-{start[4:6]}-{start[6:]}"
    e = f"{end[:4]}-{end[4:6]}-{end[6:]}"
    r = requests.get(TX_URL, params={"param": f"{_tx_symbol(code)},day,{s},{e},640,qfq"},
                     headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
    data = r.json()["data"][_tx_symbol(code)]
    rows = data.get("qfqday") or data.get("day") or []
    if not rows:
        return pd.DataFrame(columns=STD_COLS)
    df = pd.DataFrame([row[:6] for row in rows], columns=STD_COLS)
    for c in STD_COLS[1:]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def fetch_em(code: str, start: str, end: str) -> pd.DataFrame:
    """东财K线（备份源，akshare），列名裁剪成与主源一致"""
    df = ak.stock_zh_a_hist(symbol=code, period="daily",
                            start_date=start, end_date=end, adjust="qfq")
    return df[STD_COLS] if not df.empty else df


_SINA_LOCK = threading.Lock()   # 新浪接口内部用 py_mini_racer（V8引擎），多线程并发必崩，全局串行


def fetch_sina(code: str, start: str, end: str) -> pd.DataFrame:
    """新浪K线（第三备份，慢约5s/只但稳定），列名标准化，量纲统一为手"""
    with _SINA_LOCK:
        df = ak.stock_zh_a_daily(symbol=_tx_symbol(code), start_date=start,
                                 end_date=end, adjust="qfq")
    if df.empty:
        return df
    df = df.rename(columns={"date": "日期", "open": "开盘", "close": "收盘",
                            "high": "最高", "low": "最低", "volume": "成交量"})
    df["成交量"] = df["成交量"] / 100   # 新浪单位=股，统一为手（腾讯口径，防增量混合分裂）
    return df[STD_COLS]


SOURCES = {"tx": fetch_tx, "em": fetch_em, "sina": fetch_sina}
_FAIL = {"tx": 0, "em": 0, "sina": 0}   # 各源连续失败计数（动态调序用）


def fetch_one(code: str, start: str, end: str) -> pd.DataFrame:
    """三源获取，按连续失败数动态调序：被风控的源自动沉底，健康源优先"""
    for name in sorted(SOURCES, key=lambda k: _FAIL[k]):
        try:
            df = SOURCES[name](code, start, end)
            if not df.empty:
                _FAIL[name] = 0
                return df
            _FAIL[name] += 1
        except Exception:
            _FAIL[name] += 1
            time.sleep(random.uniform(0.05, 0.15))   # 礼貌间隔+风控规避
    raise ConnectionError(f"{code} 三源均失败")


def update_kline(code: str) -> str:
    """单只股票增量更新：有缓存从最后日期续拉，无缓存全量拉"""
    path = KLINE_DIR / f"{code}.csv"
    end = datetime.now().strftime("%Y%m%d")
    try:
        if path.exists():
            old = pd.read_csv(path, dtype={"股票代码": str})
            last = str(old["日期"].iloc[-1]).replace("-", "")
            start = (datetime.strptime(last, "%Y%m%d") - timedelta(days=REFRESH_DAYS)).strftime("%Y%m%d")
            new = fetch_one(code, start, end)
            if new.empty:
                return "ok"
            merged = pd.concat([old, new]).drop_duplicates(subset=["日期"], keep="last")
            merged = merged.sort_values("日期").reset_index(drop=True)
            merged.to_csv(path, index=False, encoding="utf-8-sig")
        else:
            start = (datetime.now() - timedelta(days=HISTORY_DAYS)).strftime("%Y%m%d")
            df = fetch_one(code, start, end)
            if df.empty:
                return "empty"
            df.to_csv(path, index=False, encoding="utf-8-sig")
        return "ok"
    except Exception:
        return "fail"


def cmd_update(codes: list[str]):
    print(f"[更新] 开始，共 {len(codes)} 只，{MAX_WORKERS} 线程并发...")
    t0 = time.time()
    stats = {"ok": 0, "empty": 0, "fail": 0}
    failed = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(update_kline, c): c for c in codes}
        for i, fut in enumerate(as_completed(futures), 1):
            r = fut.result()
            stats[r] = stats.get(r, 0) + 1
            if r == "fail":
                failed.append(futures[fut])
            if i % 500 == 0 or i == len(codes):
                print(f"  进度 {i}/{len(codes)}  ok={stats['ok']} fail={stats['fail']}  已用{time.time()-t0:.0f}s", flush=True)
                if i % 500 == 0 and i < len(codes):
                    time.sleep(25)   # 每500只休息25秒，防新浪风控（2026-07-19全量建仓教训）
    if failed:
        (KLINE_DIR / "_failed.txt").write_text("\n".join(failed), encoding="utf-8")
        print(f"[更新] 失败 {len(failed)} 只已记录 _failed.txt，下次 update 自动重试")
    print(f"[更新] 完成，耗时 {time.time()-t0:.0f}s")


# ---------- C2 股东人数（筹码维度，条件卡：01-策略条件库/C2-股东人数.md） ----------
C2_DROP_MIN = -0.15   # 累计降幅阈值（两季连降且累计>15%）


def load_c2() -> dict:
    """跨报告期对齐"股东户数-本次"，per-stock回退链取最新3点。
    返回 {code: 累计降幅(负值%)}，仅含命中股票"""
    frames = []
    for p in sorted((BASE_DIR / "data" / "gdhs").glob("*.csv")):
        df = pd.read_csv(p, dtype={"代码": str})
        df["期"] = p.stem
        frames.append(df[["代码", "期", "股东户数-本次"]])
    if not frames:
        return {}
    all_g = pd.concat(frames)
    hits = {}
    for code, g in all_g.groupby("代码"):
        g = g.dropna(subset=["股东户数-本次"]).sort_values("期").tail(3)
        if len(g) < 3:
            continue
        v = g["股东户数-本次"].values
        if v[2] < v[1] and v[1] < v[0]:            # 最近2次期际变化均为负
            cum = v[2] / v[0] - 1
            if cum < C2_DROP_MIN:
                hits[code] = cum * 100
    return hits


# ---------- C3 融资余额（资金维度，条件卡：01-策略条件库/C3-融资余额.md） ----------
C3_NET_MIN = 0.05     # 20日净增≥5%
C3_CAP_MAX = 0.10     # 占流通市值>10%排除
C3_HOT_WEEK = 0.05    # 周环比>5%过热降排序


def load_c3(snapshot: pd.DataFrame) -> dict:
    """近20交易日融资余额判定。返回 {code: (命中True/不命中False/不可用None, 净增%, 过热bool)}"""
    frames = []
    for mkt, code_col in (("sse", "标的证券代码"), ("szse", "证券代码")):
        for p in sorted((BASE_DIR / "data" / "margin" / mkt).glob("*.csv")):
            df = pd.read_csv(p, dtype={code_col: str})
            df = df[[code_col, "融资余额"]].rename(columns={code_col: "代码"})
            df["日"] = p.stem          # szse文件无日期列，文件名注入
            frames.append(df)
    if not frames:
        return {}
    all_m = pd.concat(frames).dropna(subset=["融资余额"])
    mcap = dict(zip(snapshot["代码"], snapshot["流通市值亿"]))
    out = {}
    for code, g in all_m.groupby("代码"):
        g = g.sort_values("日").tail(20)
        if len(g) < 20:
            out[code] = (None, None, None)       # 数据不足=不可用（≠不命中）
            continue
        v = g["融资余额"].values
        if v[0] <= 0:
            continue
        net = v[-1] / v[0] - 1
        cap = mcap.get(code)
        if cap and v[-1] / (cap * 1e8) > C3_CAP_MAX:
            out[code] = (False, net * 100, None)  # 杠杆占比过高，排除
            continue
        trend = v[10:].mean() > v[:10].mean()     # 后半窗>前半窗，防单日脉冲
        hot = len(v) >= 6 and (v[-1] / v[-6] - 1) > C3_HOT_WEEK
        out[code] = (net >= C3_NET_MIN and trend, net * 100, hot)
    return out


def check_c1(df: pd.DataFrame) -> dict | None:
    """C1 量增价平判定，命中返回指标字典，否则 None"""
    if len(df) < 290:          # 250日窗口 + 前20日量窗，不够=次新/长期停牌
        return None
    close, high, low, vol = df["收盘"], df["最高"], df["最低"], df["成交量"]
    if (vol.tail(20) <= 0).any():   # 近20日有停牌
        return None

    win250 = df.tail(250)
    lo250, hi250 = win250["最低"].min(), win250["最高"].max()
    if hi250 <= lo250:
        return None
    pos = (close.iloc[-1] - lo250) / (hi250 - lo250)
    if pos >= POS_MAX:
        return None

    w20, p20 = df.tail(20), df.iloc[-40:-20]
    amp = (w20["最高"].max() - w20["最低"].min()) / w20["最低"].min()
    chg = close.iloc[-1] / close.iloc[-21] - 1
    if amp >= AMP_MAX or abs(chg) >= CHG_MAX:
        return None

    v_prev = p20["成交量"].mean()
    if v_prev <= 0:
        return None
    vratio = w20["成交量"].mean() / v_prev
    if vratio < VOL_RATIO_MIN:
        return None

    # 综合分：量比40 + 位置30 + 平稳30
    s_vol = 60 + min(vratio - 1.3, 1.2) / 1.2 * 40 if vratio <= 2.5 else max(100 - (vratio - 2.5) * 40, 40)
    s_pos = (POS_MAX - pos) / POS_MAX * 100
    s_amp = (AMP_MAX - amp) / AMP_MAX * 100
    score = s_vol * 0.4 + s_pos * 0.3 + s_amp * 0.3
    return {"收盘": round(close.iloc[-1], 2), "20日涨跌幅%": round(chg * 100, 1),
            "20日振幅%": round(amp * 100, 1), "量比": round(vratio, 2),
            "250日位置%": round(pos * 100, 1), "综合分": round(score, 1)}


def cmd_scan(universe: pd.DataFrame, only: set[str] | None = None):
    """C1精筛。only 非空时只扫指定代码（quick模式的粗筛幸存者）"""
    paths = [KLINE_DIR / f"{c}.csv" for c in only] if only else list(KLINE_DIR.glob("*.csv"))
    print(f"[扫描] C1量增价平，候选 {len(paths)} 只")
    hits = []
    name_map = dict(zip(universe["code"], universe["name"]))
    for path in paths:
        if not path.exists():
            continue
        code = path.stem
        name = name_map.get(code, "")
        if any(t in name for t in ("ST", "退")):
            continue
        try:
            df = pd.read_csv(path, dtype={"股票代码": str})
            r = check_c1(df)
            if r:
                hits.append({"代码": code, "名称": name, **r})
        except Exception:
            continue
    if not hits:
        print("[扫描] 今日无命中（正常，量增价平本来就是少数派信号）")
        return
    # 三维合并（2026-07-19 验证者校准）：C1命中票标注C2筹码/C3资金维度
    print("[扫描] 合并 C2 股东人数 / C3 融资余额 维度...", flush=True)
    c2_map = load_c2()
    snap = pd.read_csv(BASE_DIR / "data" / "snapshot.csv", dtype={"代码": str})
    c3_map = load_c3(snap)
    for h in hits:
        code = h["代码"]
        c2cum = c2_map.get(code)
        h["C2筹码"] = "✓" if c2cum is not None else "—"
        if c2cum is not None:
            h["C2降幅%"] = round(c2cum, 1)
        c3r = c3_map.get(code)
        if c3r is None or c3r[0] is None:
            h["C3资金"] = "∅"           # 不可用（非两融/数据不足）
        elif c3r[0]:
            h["C3资金"] = "✓"
            h["C3净增%"] = round(c3r[1], 1)
            if c3r[2]:
                h["C3资金"] = "✓热"      # 周环比>5%过热标注
        else:
            h["C3资金"] = "—"
        h["维度数"] = 2 + (1 if c2cum is not None else 0) + (1 if c3r and c3r[0] else 0)
    out = pd.DataFrame(hits).sort_values(["维度数", "综合分"], ascending=False)
    # 文件名带时间戳：同日多次扫描不互相覆盖（03-选股结果只追加不覆盖）
    csv_path = RESULT_DIR / f"{datetime.now():%Y%m%d-%H%M}-C1量增价平-候选名单.csv"
    out.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"[扫描] 命中 {len(out)} 只 → {csv_path.name}\n")
    print(out.head(30).to_string(index=False))


def cmd_quick(universe: pd.DataFrame):
    """每日主流水线：快照粗筛（约90秒）→ 只对幸存者拉K线精筛"""
    snap_cache = BASE_DIR / "data" / "snapshot.csv"
    fresh = (snap_cache.exists()
             and datetime.fromtimestamp(snap_cache.stat().st_mtime).date() == datetime.now().date())
    if fresh:
        print("[粗筛] 复用今日快照缓存")
        snap = pd.read_csv(snap_cache, dtype={"代码": str})
    else:
        print("[粗筛] 拉腾讯批量快照...", flush=True)
        snap = fetch_snapshot(universe["code"].tolist())
        snap.to_csv(snap_cache, index=False, encoding="utf-8-sig")
    print(f"[粗筛] 快照 {len(snap)} 只", flush=True)
    mask = ((~snap["名称"].str.contains("ST|退", na=False)) & (snap["现价"] >= ROUGH_PRICE)
            & (snap["52周位置%"] < ROUGH_POS) & (snap["换手%"] >= ROUGH_TURN[0])
            & (snap["换手%"] <= ROUGH_TURN[1]) & (snap["量比"] >= ROUGH_VR)
            & (snap["流通市值亿"] >= ROUGH_MCAP))
    pool = snap[mask]
    print(f"[粗筛] 幸存 {len(pool)} 只（位置<{ROUGH_POS}% + 量比≥{ROUGH_VR} + 市值≥{ROUGH_MCAP}亿）→ 精筛")
    if pool.empty:
        print("[粗筛] 今日无幸存者，流程结束")
        return
    cmd_update(pool["代码"].tolist())
    cmd_scan(universe, only=set(pool["代码"]))


def main():
    no_pause = "--no-pause" in sys.argv
    mode = next((a for a in sys.argv[1:] if not a.startswith("--")), "quick")
    try:
        universe = get_universe()
        print(f"[股票池] {len(universe)} 只（沪深A股，已排除北交所）")
        if mode in ("update", "all"):
            cmd_update(universe["code"].tolist())
        if mode in ("scan", "all"):
            cmd_scan(universe)
        if mode == "quick":
            cmd_quick(universe)
    except Exception as e:
        print(f"[错误] {type(e).__name__}: {e}")
    if not no_pause:
        input("\n按回车退出...")


if __name__ == "__main__":
    main()
