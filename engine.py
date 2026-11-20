# -*- coding: utf-8 -*-
"""
08-股票中台 · 面板引擎
========================
按需扫描 + 五维打分 + 规则研判 + 情绪温度 + 持仓监控。
数据主权在 06（本文件只读复用其 scan.py 与 data/），净值主权在 07（只读）。
"""
import json
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

BASE_DIR = Path(__file__).resolve().parent          # 01-面板服务/
DATA_DIR = BASE_DIR / "data"
HIST_DIR = DATA_DIR / "history"
for d in (DATA_DIR, HIST_DIR, (BASE_DIR / "static")):
    d.mkdir(parents=True, exist_ok=True)

# ---- 复用 06 扫描脚本（数据主权在 06，口径以 06 为准）----
SCAN06_DIR = BASE_DIR.parent.parent / "06-选股流水线" / "02-扫描脚本"
sys.path.insert(0, str(SCAN06_DIR))
import scan as scan06                      # noqa: E402

ACCOUNT_DIR = BASE_DIR.parent.parent / "07-模拟盘跟踪" / "01-账户"
ROTATION_JSON = BASE_DIR.parent / "04-情绪与轮动" / "rotation.json"

KLINE_DAYS = 500          # 与 scan.py HISTORY_DAYS 一致（500自然日≈340交易日，过290日窗口下限）
# 粗筛参数一律从 06 scan.py 读（主权在06，禁止本地硬编码漂移）
ROUGH = dict(pos=scan06.ROUGH_POS, vr=scan06.ROUGH_VR, price=scan06.ROUGH_PRICE,
             mcap=scan06.ROUGH_MCAP, turn=scan06.ROUGH_TURN)

# ---------- 扫描状态机（server 轮询用） ----------
STATUS = {"state": "idle", "phase": "", "message": "", "started": None,
          "rough": 0, "survivors": 0, "done": 0, "result_file": None, "error": None}


# ========== 行情与情绪 ==========
def _tx_symbol(code: str) -> str:
    if code.startswith(("sh", "sz")):
        return code
    return ("sh" if code.startswith(("60", "68")) else "sz") + code


def fetch_snapshot_full(codes: list[str]) -> pd.DataFrame:
    """腾讯批量快照（80/批），含涨跌幅——粗筛+情绪双用（06 版无涨跌字段，此处扩展）"""
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
                    chg = float(p[32] or 0)
                    if not (-30 <= chg <= 30):      # 字段错位防御
                        chg = 0.0
                    rows.append({"代码": p[2], "名称": p[1], "现价": float(p[3] or 0),
                                 "涨跌幅%": chg, "涨跌额": float(p[31] or 0),
                                 "换手%": float(p[38] or 0), "流通市值亿": float(p[44] or 0),
                                 "52周高": float(p[47] or 0), "52周低": float(p[48] or 0),
                                 "量比": float(p[49] or 0)})
                except ValueError:
                    continue
        except Exception as e:
            print(f"[快照] 批次{i//80+1} 失败: {type(e).__name__}: {e}", flush=True)
        if (i // 80) % 10 == 0:
            print(f"[快照] 批次 {i//80+1}/{(len(codes)+79)//80} 已收{len(rows)}行", flush=True)
        time.sleep(0.15)
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df[(df["现价"] > 0) & (df["52周高"] > df["52周低"])]
    df["52周位置%"] = ((df["现价"] - df["52周低"]) / (df["52周高"] - df["52周低"]) * 100).round(1)
    return df


def fetch_index_quotes() -> list[dict]:
    idx = ["sh000001", "sz399001", "sz399006"]
    out = []
    try:
        r = requests.get("https://qt.gtimg.cn/q=" + ",".join(idx),
                         headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        for line in r.text.strip().split(";"):
            if "~" not in line:
                continue
            p = line.split("~")
            if len(p) > 33:
                out.append({"名称": p[1], "点位": float(p[3] or 0),
                            "涨跌幅%": round(float(p[32] or 0), 2)})
    except Exception:
        pass
    return out


def sentiment_phase(zt: int, dt: int) -> tuple[str, str]:
    """情绪四阶段（出处：02-研究报告/技术面分析§3.3 + SKILL.md 1.4）"""
    if dt >= 100:
        return "恐慌", "冰点·极端（跌停≥100，等跌停收敛再评估）"
    if zt < 20:
        return "冰点", "布局窗口（仓位3-5成）"
    if zt <= 80:
        return "回暖", "积极做多（仓位6-8成）"
    if zt > 100:
        return "高潮", "减仓信号（仓位<5成）"
    return "回暖偏热", "接近高潮，警惕滞涨"


def compute_sentiment(snap: pd.DataFrame | None = None) -> dict:
    """全市场行情快照 → 情绪温度计（实时）。传入 snap 避免重复拉取"""
    if snap is None:
        uni = scan06.get_universe()
        snap = fetch_snapshot_full(uni["code"].tolist())
    if snap.empty:
        return {"error": "快照拉取失败"}
    up = int((snap["涨跌幅%"] > 0).sum())
    down = int((snap["涨跌幅%"] < 0).sum())
    flat = int(len(snap) - up - down)
    zt = int((snap["涨跌幅%"] >= 9.8).sum())
    dt = int((snap["涨跌幅%"] <= -9.8).sum())
    phase, advice = sentiment_phase(zt, dt)
    return {"时间": datetime.now().strftime("%H:%M:%S"), "总数": len(snap),
            "上涨": up, "下跌": down, "平盘": flat, "涨停": zt, "跌停": dt,
            "情绪阶段": phase, "阶段建议": advice,
            "指数": fetch_index_quotes()}


# ========== 持仓（只读 07） ==========
def load_holdings() -> dict:
    pos = pd.read_csv(ACCOUNT_DIR / "positions.csv", dtype={"代码": str})
    account = json.load(open(ACCOUNT_DIR / "account.json", encoding="utf-8"))
    quotes = fetch_snapshot_full(pos["代码"].tolist())
    qmap = quotes.set_index("代码").to_dict("index") if not quotes.empty else {}
    rows, alerts = [], 0
    for _, r in pos.iterrows():
        code = str(r["代码"]).zfill(6)
        q = qmap.get(code, {})
        price = q.get("现价", 0) or float(r["成本价"])
        pnl = (price / float(r["成本价"]) - 1) * 100
        platform = float(r["平台低点"])
        broke = price < platform * 0.99            # 有效跌破=收盘破幅>1%（技术面报告§1.2）
        sig = "⚠️ 破平台" if broke else ("📉 浮亏≥5%" if pnl <= -5 else "持有")
        if broke or pnl <= -8:
            alerts += 1
        rows.append({"代码": code, "名称": r["名称"], "类型": r["类型"], "现价": round(price, 2),
                     "成本": float(r["成本价"]), "股数": int(r["股数"]),
                     "市值": round(price * int(r["股数"]), 0), "盈亏%": round(pnl, 1),
                     "平台低点": platform, "信号": sig, "涨跌幅%": q.get("涨跌幅%", 0)})
    total_mv = sum(r["市值"] for r in rows)
    cash = float(account["现金"])
    total = total_mv + cash
    init = float(account["初始资金"])
    try:
        dims = dim_hits_for([r["代码"] for r in rows], quotes)
    except Exception:
        dims = {}
    for r in rows:
        r["五维"] = dims.get(r["代码"])
    return {"持仓": rows, "告警数": alerts,
            "总览": {"持仓市值": round(total_mv, 0), "现金": cash, "总资产": round(total, 0),
                      "累计盈亏": round(total - init, 0), "累计盈亏%": round((total / init - 1) * 100, 2),
                      "仓位%": round(total_mv / total * 100, 1)},
            "行情时间": datetime.now().strftime("%H:%M:%S")}


def load_fills() -> list[dict]:
    """交易流水（07 主权，只读）——FILLS 卡"""
    p = ACCOUNT_DIR / "sold_stocks.csv"
    if not p.exists():
        return []
    df = pd.read_csv(p, dtype={"代码": str})
    df = df.tail(8).iloc[::-1]
    return [{"代码": str(r["代码"]).zfill(6), "买价": r["买价"], "平仓价": r["平仓价"],
             "盈亏": r["盈亏"], "卖出日期": r["卖出日期"]} for _, r in df.iterrows()]


# ========== 打分引擎（硬门槛+软分，规则见 02-打分引擎/条件卡/） ==========
def _s2(c2cum):
    if c2cum is None:
        return 0.4, "未命中"
    return 0.7 + min(abs(c2cum) / 30, 0.3), f"✓ 降幅{round(c2cum,1)}%"


def _s3(c3r):
    if c3r is None or c3r[0] is None:
        return 0.4, "不可用"
    if not c3r[0]:
        return 0.0, "排除(杠杆占比高)"
    s = 0.7 + min(c3r[1] / 20, 0.3)
    if c3r[2]:
        s -= 0.15
    return max(s, 0.3), ("✓热" if c3r[2] else "✓") + f" 净增{round(c3r[1],1)}%"


def _s5_hit(code: str) -> bool | None:
    """龙虎榜近30天是否有记录；库不存在/读失败=None（未知），有库无记录=False"""
    try:
        files = sorted((SCAN06_DIR / "data" / "lhb").glob("*.csv"))[-3:]
        if not files:
            return None
        for p in files:
            df = pd.read_csv(p, dtype=str)
            col = next((c for c in df.columns if "代码" in c), None)
            if col and code in set(df[col].str.zfill(6)):
                return True
        return False
    except Exception:
        return None


def _s5(code: str) -> float:
    """龙虎榜加分项（权重0.1）——近30天本地库有记录记1.0"""
    return 1.0 if _s5_hit(code) else 0.3


# ---------- 五维命中展示（量/位/筹/资/榜，口径一律取 scan06，禁本地硬编码） ----------
# 量=check_c1 量增价平；位=250日位置<POS_MAX；筹=load_c2 两季连降；资=load_c3 20日净增；榜=_s5_hit
_DIM_CACHE = {"ts": 0.0, "key": None, "data": {}}
DIM_TTL = 600                       # K线/本地库指标 10 分钟有效（行情快照 30s 刷新不重复拉K线）


def dim_hits_for(codes: list[str], snap: pd.DataFrame) -> dict:
    """任意代码列表 → 五维命中 dict。未知维度=null（数据不可用），单票失败不阻塞整体。"""
    key = tuple(sorted(codes))
    if time.time() - _DIM_CACHE["ts"] < DIM_TTL and _DIM_CACHE["key"] == key:
        return _DIM_CACHE["data"]
    out = {}
    gdhs_ok = any((SCAN06_DIR / "data" / "gdhs").glob("*.csv"))
    try:
        c2_map = scan06.load_c2() if gdhs_ok else {}
    except Exception:
        c2_map = {}
    try:
        c3_map = scan06.load_c3(snap) if not snap.empty else {}
    except Exception:
        c3_map = {}
    start = (datetime.now() - pd.Timedelta(days=KLINE_DAYS + 100)).strftime("%Y%m%d")
    end = datetime.now().strftime("%Y%m%d")
    for code in codes:
        d = {"量": None, "位": None, "筹": None, "资": None, "榜": None}
        try:
            df = scan06.fetch_one(code, start, end)
            if not df.empty and len(df) >= 250:
                win = df.tail(250)
                lo, hi = win["最低"].min(), win["最高"].max()
                if hi > lo:
                    pos = (win["收盘"].iloc[-1] - lo) / (hi - lo)
                    d["位"] = bool(pos < scan06.POS_MAX)
                    d["位%"] = round(pos * 100, 1)
                d["量"] = scan06.check_c1(df) is not None
        except Exception:
            pass                                                      # 该票量/位未知，其余维照常
        d["筹"] = True if code in c2_map else False
        c3r = c3_map.get(code)
        if c3r is None or c3r[0] is None:
            d["资"] = None                                            # 无两融数据/窗口不足=不可用
        else:
            d["资"] = bool(c3r[0])
        d["榜"] = _s5_hit(code)
        d["命中"] = sum(1 for k in ("量", "位", "筹", "资", "榜") if d[k] is True)
        out[code] = d
    _DIM_CACHE.update(ts=time.time(), key=key, data=out)
    return out


def confidence(hit: dict) -> tuple[float, list]:
    """硬门槛：维度数≥3（体系总纲，调用方判）。软分：0~1 置信度"""
    c1s = hit.get("综合分", 0) / 100
    s2, t2 = _s2(hit.get("C2降幅%") if hit.get("C2筹码") == "✓" else None)
    s3, t3 = _s3(hit.get("_c3"))
    s5 = _s5(hit["代码"])
    conf = 0.5 * c1s + 0.2 * s2 + 0.2 * s3 + 0.1 * s5
    dims = [f"C1量增价平 量比{hit.get('量比','')}",
            f"C2筹码 {t2}", f"C3资金 {t3}",
            f"位置 {hit.get('250日位置%','')}%",
            f"龙虎榜 {'✓近30天上榜' if _s5_hit(hit['代码']) else '—未上榜'}"]
    return round(min(max(conf, 0), 1), 2), dims


def judgment_card(hit: dict, sentiment: dict) -> dict:
    """规则版三判断（V1）。出处：03-研判模板/三判断规则与提示词-V1.md"""
    risks, chances = [], []
    pos = hit.get("250日位置%", 100)
    vr = hit.get("量比", 1)
    amp = hit.get("20日振幅%", 99)
    if pos >= 35:
        risks.append(f"位置偏高（{pos}%，接近40%上限）")
    if vr >= 2.5:
        risks.append(f"量比{vr}过热，警惕对倒放量")
    if amp >= 12:
        risks.append(f"20日振幅{amp}%接近破裂阈值")
    c3r = hit.get("_c3")
    if c3r and c3r[2]:
        risks.append("融资余额周环比>5%，杠杆过热")
    if not risks:
        risks.append("常规波动风险，止损线=平台低点-3%")
    if hit.get("C2筹码") == "✓":
        chances.append("股东人数两季连降，筹码集中")
    if c3r and c3r[0]:
        chances.append("融资余额20日净增，杠杆资金流入")
    if pos < 20:
        chances.append(f"250日低位（{pos}%），吸筹区")
    chances.append(f"量增价平（量比{vr}），主力成本痕迹")
    phase = sentiment.get("情绪阶段", "未知")
    timing = {"冰点": "情绪冰点，分批布局窗口，先入1/3仓",
              "恐慌": "恐慌极端，不抄底，等跌停收敛再评估",
              "回暖": "回暖期可积极，突破确认后加仓",
              "回暖偏热": "偏热，等回踩再加",
              "高潮": "高潮期，只减不加"}.get(phase, "观望，等情绪明朗")
    return {"风险": risks, "机遇": chances, "时点": f"{phase}：{timing}",
            "止损参考": f"{round(hit.get('收盘', 0) * 0.97, 2)}（现价-3%）"}


def prompt_text(hit: dict) -> str:
    """LLM 深度研判提示词（复制到会话使用；面板不直连 LLM）"""
    return (f"请对 {hit['名称']}（{hit['代码']}）做抓主力体系深度研判。现价{hit.get('收盘')}，"
            f"量比{hit.get('量比')}，250日位置{hit.get('250日位置%')}%，20日振幅{hit.get('20日振幅%')}%，"
            f"20日涨跌幅{hit.get('20日涨跌幅%')}%。五维：{'; '.join(hit.get('五维明细', []))}。"
            f"请输出：①吸筹/洗盘/拉升/出货阶段判定及证据 ②未来2-4周风险清单 "
            f"③买入时点与仓位建议 ④止损位 ⑤需人工核实的信息缺口。"
            f"规则依据：02-研究报告/技术面分析与抓庄手法深度研究。")


# ========== 扫描主流程 ==========
def run_scan():
    """按需全市场扫描：快照粗筛 → 幸存者K线精筛 → C1+C2/C3 → 打分 → 落盘"""
    global STATUS
    STATUS.update(state="running", started=datetime.now().isoformat(timespec="seconds"),
                  error=None, result_file=None, done=0)
    _SENTIMENT["refreshing"] = True          # 扫描持有快照权，禁情绪任务插进来 doubling 请求
    snap = pd.DataFrame()
    try:
        STATUS.update(phase="universe", message="取股票池…")
        uni = scan06.get_universe()
        STATUS.update(rough=len(uni))

        STATUS.update(phase="snapshot", message=f"腾讯全市场快照（{len(uni)}只，约60-90秒）…")
        snap = fetch_snapshot_full(uni["code"].tolist())
        if snap.empty:
            raise ConnectionError("快照为空（网络或风控）")
        snap.to_csv(SCAN06_DIR / "data" / "snapshot.csv", index=False, encoding="utf-8-sig")

        # 粗筛漏斗：全池逐只评估，逐步记录边际淘汰数（透明化"每只都筛过"）
        alive = pd.Series(True, index=snap.index)
        steps = []
        for label, cond in [("ST/退市", snap["名称"].str.contains("ST|退", na=False)),
                            (f"现价<{ROUGH['price']}元", snap["现价"] < ROUGH["price"]),
                            (f"52周位置≥{ROUGH['pos']}%", snap["52周位置%"] >= ROUGH["pos"]),
                            (f"换手<{ROUGH['turn'][0]}%", snap["换手%"] < ROUGH["turn"][0]),
                            (f"换手>{ROUGH['turn'][1]}%", snap["换手%"] > ROUGH["turn"][1]),
                            (f"量比<{ROUGH['vr']}", snap["量比"] < ROUGH["vr"]),
                            (f"流通市值<{ROUGH['mcap']}亿", snap["流通市值亿"] < ROUGH["mcap"])]:
            dead = alive & cond
            steps.append({"条件": label, "淘汰": int(dead.sum())})
            alive = alive & ~cond
        funnel = {"已评估": len(snap), "快照缺失": max(int(STATUS["rough"]) - len(snap), 0),
                  "步骤": steps, "幸存": int(alive.sum())}

        mask = ((~snap["名称"].str.contains("ST|退", na=False)) & (snap["现价"] >= ROUGH["price"])
                & (snap["52周位置%"] < ROUGH["pos"]) & (snap["换手%"] >= ROUGH["turn"][0])
                & (snap["换手%"] <= ROUGH["turn"][1]) & (snap["量比"] >= ROUGH["vr"])
                & (snap["流通市值亿"] >= ROUGH["mcap"]))
        pool = snap[mask]
        STATUS.update(phase="kline", survivors=len(pool),
                      message=f"粗筛幸存 {len(pool)} 只，拉K线精筛（约2-4分钟）…")
        if pool.empty:
            raise StopIteration("今日无幸存者（正常，量增价平本来就是少数派信号）")

        start = (datetime.now() - pd.Timedelta(days=KLINE_DAYS + 100)).strftime("%Y%m%d")
        end = datetime.now().strftime("%Y%m%d")
        klines, fails = {}, 0
        done_cnt, cnt_lock = 0, threading.Lock()
        surv_codes = pool["代码"].tolist()

        def _fetch_one(code):
            nonlocal fails, done_cnt
            try:
                df = scan06.fetch_one(code, start, end)
                if not df.empty:
                    klines[code] = df
            except Exception:
                with cnt_lock:
                    fails += 1
            with cnt_lock:
                done_cnt += 1
                if done_cnt % 10 == 0 or done_cnt == len(surv_codes):
                    STATUS.update(done=done_cnt, message=f"K线 {done_cnt}/{len(surv_codes)}（失败{fails}）")
            time.sleep(0.1)

        STATUS.update(message=f"粗筛幸存 {len(surv_codes)} 只，6线程拉K线精筛…")
        with ThreadPoolExecutor(max_workers=6) as pool_x:
            list(pool_x.map(_fetch_one, surv_codes))
        STATUS.update(done=len(surv_codes), message=f"K线完成（失败{fails}），五维合并+打分…")

        STATUS.update(phase="score", message="五维合并+打分…")
        c2_map = scan06.load_c2()
        c3_map = scan06.load_c3(snap)
        name_map = dict(zip(uni["code"], uni["name"]))
        hits = []
        for code, df in klines.items():
            h = scan06.check_c1(df)
            if not h:
                continue
            c2cum = c2_map.get(code)
            h["C2筹码"] = "✓" if c2cum is not None else "—"
            if c2cum is not None:
                h["C2降幅%"] = round(c2cum, 1)
            c3r = c3_map.get(code)
            h["_c3"] = c3r
            if c3r is None or c3r[0] is None:
                h["C3资金"] = "∅"
            elif c3r[0]:
                h["C3资金"] = "✓热" if c3r[2] else "✓"
                h["C3净增%"] = round(c3r[1], 1)
            else:
                h["C3资金"] = "—"
            h["维度数"] = 2 + (1 if c2cum is not None else 0) + (1 if c3r and c3r[0] else 0)
            h["龙虎榜"] = "✓" if _s5_hit(code) else "—"
            h["命中数"] = h["维度数"] + (1 if h["龙虎榜"] == "✓" else 0)
            if h["维度数"] < 3:                      # 硬门槛：五维至少三维命中
                continue
            h["代码"] = code
            h["名称"] = name_map.get(code, "")
            conf, dims = confidence(h)
            h["置信度"] = conf
            h["五维明细"] = dims
            hits.append(h)
        hits.sort(key=lambda x: (x["维度数"], x["置信度"]), reverse=True)

        senti = compute_sentiment(snap)              # 复用快照，不重复拉
        _SENTIMENT["data"] = senti                   # 扫描结果顺手灌入情绪缓存
        _SENTIMENT["ts"] = time.time()
        _SENTIMENT["refreshing"] = False             # 释放快照持有权
        public_keys = ["收盘", "20日涨跌幅%", "20日振幅%", "量比", "250日位置%", "综合分",
                       "C2筹码", "C2降幅%", "C3资金", "C3净增%", "维度数", "命中数", "龙虎榜",
                       "代码", "名称", "置信度", "五维明细"]
        candidates, cards = [], {}
        for h in hits:
            j = judgment_card(h, senti)
            row = {k: h[k] for k in public_keys if k in h}
            candidates.append(row)
            cards[h["代码"]] = {"研判": j, "提示词": prompt_text({**row, "五维明细": h["五维明细"]})}

        stamp = datetime.now().strftime("%Y%m%d-%H%M")
        payload = {"扫描时间": STATUS["started"], "粗筛总数": int(STATUS["rough"]),
                   "幸存数": len(pool), "K线失败": fails, "硬门槛后命中": len(candidates),
                   "情绪": senti, "候选": candidates, "研判卡": cards, "漏斗": funnel,
                   "数据说明": {"C1": "实时K线", "C2": "股东人数·本地库最新报告期",
                                 "C3": "融资融券·本地库30天窗口", "龙虎榜": "本地库近30天"}}
        result_file = HIST_DIR / f"{stamp}-扫描结果.json"
        result_file.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
        (DATA_DIR / "last_result.json").write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        STATUS.update(state="done", phase="done", result_file=str(result_file),
                      message=f"命中 {len(candidates)} 只")
    except StopIteration as e:
        senti = compute_sentiment(snap)
        _SENTIMENT["data"] = senti
        _SENTIMENT["ts"] = time.time()
        _SENTIMENT["refreshing"] = False
        payload = {"扫描时间": STATUS["started"], "粗筛总数": int(STATUS["rough"]),
                   "幸存数": 0, "硬门槛后命中": 0, "情绪": senti, "候选": [], "研判卡": {},
                   "漏斗": funnel, "备注": str(e)}
        (DATA_DIR / "last_result.json").write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        STATUS.update(state="done", message=str(e))
    except Exception as e:
        _SENTIMENT["refreshing"] = False
        STATUS.update(state="error", error=f"{type(e).__name__}: {e}")


def start_scan_thread() -> bool:
    if STATUS["state"] == "running":
        return False
    t = threading.Thread(target=run_scan, daemon=True)
    t.start()
    return True


def load_last_result() -> dict | None:
    p = DATA_DIR / "last_result.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def list_history() -> list[dict]:
    """历史扫描列表（最新20份，供面板下拉翻看）"""
    out = []
    for p in sorted(HIST_DIR.glob("*-扫描结果.json"), reverse=True)[:20]:
        try:
            d = json.loads(p.read_text(encoding="utf-8"))
            out.append({"file": p.name, "扫描时间": d.get("扫描时间", ""),
                        "命中": d.get("硬门槛后命中"), "幸存": d.get("幸存数")})
        except Exception:
            out.append({"file": p.name})
    return out


def load_history(name: str) -> dict | None:
    safe = Path(name).name                    # 剥掉路径，防穿越
    if not safe.endswith("-扫描结果.json"):
        return None
    p = HIST_DIR / safe
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


# ---------- 情绪缓存（扫描进行时跳过，避免双份全量快照互抢） ----------
_SENTIMENT = {"ts": 0.0, "data": None, "refreshing": False}
SENTIMENT_TTL = 300


def _refresh_sentiment_job():
    if STATUS["state"] == "running":        # 扫描优先：扫描进行中不抢全量快照
        return
    _SENTIMENT["refreshing"] = True
    try:
        _SENTIMENT["data"] = compute_sentiment()
        _SENTIMENT["ts"] = time.time()
    except Exception as e:
        _SENTIMENT["data"] = {"error": f"{type(e).__name__}: {e}"}
    finally:
        _SENTIMENT["refreshing"] = False


def get_sentiment() -> dict:
    age = time.time() - _SENTIMENT["ts"]
    cold = _SENTIMENT["data"] is None or age > SENTIMENT_TTL
    if cold and not _SENTIMENT["refreshing"] and STATUS["state"] != "running":
        threading.Thread(target=_refresh_sentiment_job, daemon=True).start()
    return {"数据": _SENTIMENT["data"],
            "缓存秒数": int(age) if _SENTIMENT["data"] is not None else None,
            "刷新中": _SENTIMENT["refreshing"]}


def load_rotation() -> dict:
    if ROTATION_JSON.exists():
        return json.loads(ROTATION_JSON.read_text(encoding="utf-8"))
    return {"说明": "rotation.json 未生成"}
