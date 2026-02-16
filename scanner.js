import nodemailer from "nodemailer";

const BASE = "https://fapi.binance.com";

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchJson(url) {
    const res = await fetch(url, { method: "GET" });
    if (!res.ok) {
        const text = await res.text().catch(() => "");
        throw new Error(`HTTP ${res.status} ${res.statusText} :: ${text.slice(0, 200)}`);
    }
    return res.json();
}

function clamp(x, lo, hi) {
    return Math.min(hi, Math.max(lo, x));
}

function pct(a, b) {
    if (!isFinite(a) || !isFinite(b) || b === 0) return 0;
    return (a / b - 1) * 100;
}

function ema(values, period) {
    if (!values || values.length === 0) return [];
    const k = 2 / (period + 1);
    const out = new Array(values.length);
    let prev = values[0];
    out[0] = prev;

    for (let i = 1; i < values.length; i += 1) {
        const v = values[i];
        prev = v * k + prev * (1 - k);
        out[i] = prev;
    }
    return out;
}

function atr(high, low, close, period) {
    const n = close.length;
    const tr = new Array(n).fill(0);

    for (let i = 1; i < n; i += 1) {
        const h = high[i];
        const l = low[i];
        const pc = close[i - 1];
        const a = h - l;
        const b = Math.abs(h - pc);
        const c = Math.abs(l - pc);
        tr[i] = Math.max(a, b, c);
    }

    const out = new Array(n).fill(0);
    let sum = 0;

    for (let i = 1; i < n; i += 1) {
        sum += tr[i];
        if (i >= period) {
            sum -= tr[i - period];
            out[i] = sum / period;
        } else {
            out[i] = sum / Math.max(1, i);
        }
    }
    return out;
}

function rsi14(close, period = 14) {
    const n = close.length;
    const out = new Array(n).fill(50);

    if (n < period + 2) return out;

    let gain = 0;
    let loss = 0;

    for (let i = 1; i <= period; i += 1) {
        const d = close[i] - close[i - 1];
        if (d >= 0) gain += d;
        else loss -= d;
    }

    let avgGain = gain / period;
    let avgLoss = loss / period;

    out[period] = avgLoss === 0 ? 100 : 100 - 100 / (1 + avgGain / avgLoss);

    for (let i = period + 1; i < n; i += 1) {
        const d = close[i] - close[i - 1];
        const g = d > 0 ? d : 0;
        const l = d < 0 ? -d : 0;

        avgGain = (avgGain * (period - 1) + g) / period;
        avgLoss = (avgLoss * (period - 1) + l) / period;

        if (avgLoss === 0) out[i] = 100;
        else {
            const rs = avgGain / avgLoss;
            out[i] = 100 - 100 / (1 + rs);
        }
    }

    return out;
}

function parseKlines(raw) {
    const o = [];
    const h = [];
    const l = [];
    const c = [];
    const v = [];

    for (const k of raw) {
        o.push(Number(k[1]));
        h.push(Number(k[2]));
        l.push(Number(k[3]));
        c.push(Number(k[4]));
        v.push(Number(k[5]));
    }

    return { o, h, l, c, v };
}

function candleQuality(oo, hh, ll, cc) {
    const range = Math.max(1e-12, hh - ll);
    const body = Math.abs(cc - oo);

    const bodyPct = (body / range) * 100;
    const upper = hh - Math.max(oo, cc);
    const lower = Math.min(oo, cc) - ll;

    const upperPct = (upper / range) * 100;
    const lowerPct = (lower / range) * 100;

    const clv = (cc - ll) / range; // 0~1
    return { bodyPct, upperPct, lowerPct, clv };
}

async function getSymbolsUSDTPerp() {
    const ex = await fetchJson(`${BASE}/fapi/v1/exchangeInfo`);
    const out = [];

    for (const s of ex.symbols || []) {
        if (s.contractType !== "PERPETUAL") continue;
        if (s.quoteAsset !== "USDT") continue;
        if (s.status !== "TRADING") continue;
        out.push(s.symbol);
    }

    return out;
}

async function getTickers24h() {
    return fetchJson(`${BASE}/fapi/v1/ticker/24hr`);
}

async function getKlines(symbol, interval, limit) {
    const url =
        `${BASE}/fapi/v1/klines?symbol=${encodeURIComponent(symbol)}` +
        `&interval=${encodeURIComponent(interval)}` +
        `&limit=${encodeURIComponent(String(limit))}`;
    return fetchJson(url);
}

async function getPremiumIndex(symbol) {
    return fetchJson(`${BASE}/fapi/v1/premiumIndex?symbol=${encodeURIComponent(symbol)}`);
}

async function getOpenInterestHist(symbol, period, limit) {
    const url =
        `${BASE}/futures/data/openInterestHist?symbol=${encodeURIComponent(symbol)}` +
        `&period=${encodeURIComponent(period)}` +
        `&limit=${encodeURIComponent(String(limit))}`;
    return fetchJson(url);
}

/**
 * 손절률 기반 “청산 회피” 최대 레버리지(보수 상한)
 * - 대략적인 청산 폭 ≈ 1 / leverage
 * - lossPct% (손절폭) 보다 청산폭이 넓어야 손절 전에 청산될 확률이 낮아짐
 * - 안전 버퍼를 더해 더 보수적으로 제한
 *
 * 공식 청산가(유지증거금/MMR, 수수료, 교차/격리, 마크프라이스)는 Binance 계산이 정답.
 */
function calcMaxLevSafe(lossPct, extraBufferPct = 0.35, hardCap = 125) {
    const d = Math.max(0.01, lossPct + extraBufferPct); // 최소 0.01% 방어
    const lev = Math.floor(100 / d); // lev < 100/(loss%+buffer)
    return clamp(lev, 1, hardCap);
}

/**
 * PASS 세팅
 * PASS1: 극단
 * PASS2: 한쪽 후보가 0개면 아주 소폭 완화(그래도 승률 필터 유지)
 */
function buildSettings(passNo) {
    const base = {
        minOiChgPct: 0.70,
        minAbsPxChgPct: 0.25,

        minBodyPct: 70,
        minAbsRetPct: 0.60,
        minVolSpike: 2.6,
        minCLVLong: 0.72,
        maxCLVShort: 0.28,
        maxOppWickPct: 22,
        breakN: 26,
        pullbackLookback: 10,
        pullbackBandBp: 28,

        maxRsiLong: 64,
        minRsiShort: 36,

        minFundingLong: -0.030,
        maxFundingLong: 0.005,
        minFundingShort: -0.004,
        maxFundingShort: 0.035,

        minAtrPct: 0.22,
        maxAtrPct: 1.70,

        tpAtrMult: 0.80,
        slAtrMult: 1.30,

        btLookahead: 22,
        btMaxSignals: 160,
        btMinTrades: 85,
        btMinWinrate: 78,
        btMinWrHalf: 76,
        btMaxConsecLoss: 2,
        minExpectancy: 0.03,

        minTrendScore: 82
    };

    if (passNo === 1) return base;

    return {
        ...base,
        minOiChgPct: 0.60,
        minAbsPxChgPct: 0.20,
        minBodyPct: 66,
        minAbsRetPct: 0.50,
        minVolSpike: 2.2,
        breakN: 22,
        maxOppWickPct: 26,
        maxRsiLong: 66,
        minRsiShort: 34,
        maxAtrPct: 2.05,
        minTrendScore: 80
    };
}

function computeTrendScoreDirectional(k1d, k4h, k1h, price, side) {
    const e1d20 = ema(k1d.c, 20);
    const e1d50 = ema(k1d.c, 50);
    const e4h20 = ema(k4h.c, 20);
    const e4h50 = ema(k4h.c, 50);
    const e1h20 = ema(k1h.c, 20);
    const e1h50 = ema(k1h.c, 50);

    const i1d = k1d.c.length - 2;
    const i4h = k4h.c.length - 2;
    const i1h = k1h.c.length - 2;

    if (i1d < 80 || i4h < 80 || i1h < 80) return { score: 0, detail: "not enough" };

    function slopeUp(arr, idx, back) {
        const j = idx - back;
        if (j < 0) return false;
        return arr[idx] > arr[j];
    }

    function slopeDown(arr, idx, back) {
        const j = idx - back;
        if (j < 0) return false;
        return arr[idx] < arr[j];
    }

    let ok1d = false;
    let ok4h = false;
    let ok1h = false;
    let s1d = false;
    let s4h = false;
    let s1h = false;

    if (side === "LONG") {
        ok1d = e1d20[i1d] > e1d50[i1d] && price > e1d20[i1d];
        ok4h = e4h20[i4h] > e4h50[i4h] && price > e4h20[i4h];
        ok1h = e1h20[i1h] > e1h50[i1h] && price > e1h20[i1h];
        s1d = slopeUp(e1d20, i1d, 6);
        s4h = slopeUp(e4h20, i4h, 6);
        s1h = slopeUp(e1h20, i1h, 10);
    } else {
        ok1d = e1d20[i1d] < e1d50[i1d] && price < e1d20[i1d];
        ok4h = e4h20[i4h] < e4h50[i4h] && price < e4h20[i4h];
        ok1h = e1h20[i1h] < e1h50[i1h] && price < e1h20[i1h];
        s1d = slopeDown(e1d20, i1d, 6);
        s4h = slopeDown(e4h20, i4h, 6);
        s1h = slopeDown(e1h20, i1h, 10);
    }

    const dist1h = Math.abs((price - e1h20[i1h]) / Math.max(1e-12, e1h20[i1h]));
    const over = Math.max(0, dist1h - 0.010);
    const penalty = clamp(over * 3000, 0, 22);

    let score = 0;
    score += ok1d ? 38 : 0;
    score += ok4h ? 30 : 0;
    score += ok1h ? 22 : 0;
    score += s1d ? 4 : 0;
    score += s4h ? 3 : 0;
    score += s1h ? 3 : 0;
    score -= penalty;

    score = clamp(score, 0, 100);

    const detail =
        `${side} ${ok1d ? "1D✓" : "1D×"} ${ok4h ? "4H✓" : "4H×"} ${ok1h ? "1H✓" : "1H×"} slope:${s1d && s4h && s1h ? "OK" : "mix"}`;

    return { score, detail };
}

function triggerAtIndexDirectional(k15, i, side, s) {
    if (i < 60 || i >= k15.c.length - 2) return false;

    const oo = k15.o[i];
    const hh = k15.h[i];
    const ll = k15.l[i];
    const cc = k15.c[i];

    const q = candleQuality(oo, hh, ll, cc);
    const retPct = pct(cc, oo);

    if (q.bodyPct < s.minBodyPct) return false;
    if (Math.abs(retPct) < s.minAbsRetPct) return false;

    if (side === "LONG") {
        if (!(cc > oo && retPct > 0)) return false;
        if (q.clv < s.minCLVLong) return false;
        if (q.upperPct > s.maxOppWickPct) return false;
    } else {
        if (!(cc < oo && retPct < 0)) return false;
        if (q.clv > s.maxCLVShort) return false;
        if (q.lowerPct > s.maxOppWickPct) return false;
    }

    let sum = 0;
    let cnt = 0;
    for (let j = i - 1; j >= 0 && cnt < 20; j -= 1) {
        sum += k15.v[j];
        cnt += 1;
    }
    const volAvg = cnt > 0 ? sum / cnt : 0;
    const volSpike = volAvg > 0 ? k15.v[i] / volAvg : 0;
    if (volSpike < s.minVolSpike) return false;

    if (side === "LONG") {
        let prevMax = -Infinity;
        for (let j = i - 1; j >= 0 && j >= i - s.breakN; j -= 1) {
            prevMax = Math.max(prevMax, k15.h[j]);
        }
        if (!(hh > prevMax)) return false;
    } else {
        let prevMin = Infinity;
        for (let j = i - 1; j >= 0 && j >= i - s.breakN; j -= 1) {
            prevMin = Math.min(prevMin, k15.l[j]);
        }
        if (!(ll < prevMin)) return false;
    }

    const e20 = ema(k15.c, 20);
    const emaNow = e20[i];
    const band = emaNow * (s.pullbackBandBp / 10000);

    if (side === "LONG") {
        let touched = false;
        for (let j = i - 1; j >= 0 && j >= i - s.pullbackLookback; j -= 1) {
            if (k15.l[j] <= e20[j] + band) {
                touched = true;
                break;
            }
        }
        if (!(touched && cc > emaNow)) return false;
    } else {
        let touched = false;
        for (let j = i - 1; j >= 0 && j >= i - s.pullbackLookback; j -= 1) {
            if (k15.h[j] >= e20[j] - band) {
                touched = true;
                break;
            }
        }
        if (!(touched && cc < emaNow)) return false;
    }

    return true;
}

function proposeTPSLDirectional(k15, tpMult, slMult, entryNow, side) {
    const a = atr(k15.h, k15.l, k15.c, 14);
    const i = k15.c.length - 2;
    const atr15 = a[i] || 0;

    if (side === "LONG") {
        return { atr15, tp: entryNow + atr15 * tpMult, sl: entryNow - atr15 * slMult };
    }
    return { atr15, tp: entryNow - atr15 * tpMult, sl: entryNow + atr15 * slMult };
}

function backtestUltraDirectional(k15, tpMult, slMult, lookahead, maxSignals, side, triggerEvalAtIndex) {
    const n = k15.c.length;
    if (n < 320) return { winrate: 0, trades: 0, wrHalf: 0, maxConsecLoss: 999, expectancy: 0 };

    const a = atr(k15.h, k15.l, k15.c, 14);

    const sig = [];
    for (let i = 90; i < n - 2; i += 1) {
        if (triggerEvalAtIndex(i)) sig.push(i);
    }

    const picked = sig.slice(-maxSignals);

    function evalList(list) {
        let wins = 0;
        let losses = 0;
        let consecLoss = 0;
        let maxConsecLoss = 0;
        let sumR = 0;

        for (let x = 0; x < list.length; x += 1) {
            const i = list[x];
            const entryIdx = i + 1;
            if (entryIdx >= n) continue;

            const entry = k15.o[entryIdx];
            const atrAt = a[i] || 0;
            if (atrAt <= 0) continue;

            let tp = 0;
            let sl = 0;

            if (side === "LONG") {
                tp = entry + atrAt * tpMult;
                sl = entry - atrAt * slMult;
            } else {
                tp = entry - atrAt * tpMult;
                sl = entry + atrAt * slMult;
            }

            let outcome = 0;

            for (let j = entryIdx; j < Math.min(n, entryIdx + lookahead); j += 1) {
                const hh = k15.h[j];
                const ll = k15.l[j];

                const hitTP = side === "LONG" ? hh >= tp : ll <= tp;
                const hitSL = side === "LONG" ? ll <= sl : hh >= sl;

                if (hitTP && hitSL) {
                    outcome = -1;
                    break;
                }
                if (hitSL) {
                    outcome = -1;
                    break;
                }
                if (hitTP) {
                    outcome = 1;
                    break;
                }
            }

            if (outcome === 1) {
                wins += 1;
                consecLoss = 0;
                sumR += tpMult;
            } else if (outcome === -1) {
                losses += 1;
                consecLoss += 1;
                maxConsecLoss = Math.max(maxConsecLoss, consecLoss);
                sumR -= slMult;
            }
        }

        const trades = wins + losses;
        const winrate = trades > 0 ? (wins / trades) * 100 : 0;
        const expectancy = trades > 0 ? sumR / trades : 0;

        return { winrate, trades, maxConsecLoss, expectancy };
    }

    const all = evalList(picked);
    const halfCount = Math.max(1, Math.floor(picked.length / 2));
    const halfList = picked.slice(-halfCount);
    const halfEval = evalList(halfList);

    return {
        winrate: all.winrate,
        trades: all.trades,
        wrHalf: halfEval.winrate,
        maxConsecLoss: all.maxConsecLoss,
        expectancy: all.expectancy
    };
}

function scoreRow(trendScore, bt) {
    const s1 = clamp(bt.winrate, 0, 95);
    const s2 = clamp(bt.wrHalf, 0, 95);
    const s3 = clamp(bt.trades, 0, 160);
    const s4 = 100 - clamp(bt.maxConsecLoss * 22, 0, 85);
    const s5 = clamp(bt.expectancy * 140, 0, 40);

    const score = s1 * 0.40 + s2 * 0.20 + trendScore * 0.18 + s3 * 0.06 + s4 * 0.12 + s5 * 0.04;
    return clamp(score, 0, 100);
}

function calcProfitLossPct(entry, tp, sl, side) {
    if (!isFinite(entry) || entry <= 0) return { profitPct: 0, lossPct: 0 };

    let profitPct = 0;
    let lossPct = 0;

    if (side === "LONG") {
        profitPct = (tp / entry - 1) * 100;
        lossPct = (1 - sl / entry) * 100;
    } else {
        profitPct = (1 - tp / entry) * 100;
        lossPct = (sl / entry - 1) * 100;
    }

    return { profitPct, lossPct };
}

async function scanPass({ passNo, universe, concurrency, minPerSideTarget, needLong, needShort, cache }) {
    const s = buildSettings(passNo);
    const results = [];
    let longCount = 0;
    let shortCount = 0;

    let idx = 0;

    async function loadSymbolData(symbol, qv24) {
        if (cache.has(symbol)) return cache.get(symbol);

        const prem = await getPremiumIndex(symbol);
        const fundingPct = Number(prem.lastFundingRate || 0) * 100;

        const oiHist = await getOpenInterestHist(symbol, "15m", 3);
        if (!Array.isArray(oiHist) || oiHist.length < 2) return null;

        const oiPrev = Number(oiHist[oiHist.length - 2].sumOpenInterest || 0);
        const oiNow = Number(oiHist[oiHist.length - 1].sumOpenInterest || 0);
        if (!(oiPrev > 0 && oiNow > 0)) return null;

        const oiChgPct = pct(oiNow, oiPrev);

        const k15 = parseKlines(await getKlines(symbol, "15m", 720));
        const k1h = parseKlines(await getKlines(symbol, "1h", 520));
        const k4h = parseKlines(await getKlines(symbol, "4h", 420));
        const k1d = parseKlines(await getKlines(symbol, "1d", 340));

        const i15 = k15.c.length - 2;
        const price = k15.c[i15] || 0;
        if (!isFinite(price) || price <= 0) return null;

        const d = { fundingPct, oiChgPct, k15, k1h, k4h, k1d, price, qv24 };
        cache.set(symbol, d);
        return d;
    }

    function trySide(d, side) {
        if (d.oiChgPct < s.minOiChgPct) return null;

        const k15 = d.k15;
        const i15 = k15.c.length - 2;

        const pxChgPct = pct(k15.c[i15], k15.o[i15]);
        if (Math.abs(pxChgPct) < s.minAbsPxChgPct) return null;

        const rsiArr = rsi14(k15.c, 14);
        const rsiNow = rsiArr[i15] || 50;

        if (side === "LONG" && rsiNow > s.maxRsiLong) return null;
        if (side === "SHORT" && rsiNow < s.minRsiShort) return null;

        if (side === "LONG") {
            if (d.fundingPct < s.minFundingLong || d.fundingPct > s.maxFundingLong) return null;
        } else {
            if (d.fundingPct < s.minFundingShort || d.fundingPct > s.maxFundingShort) return null;
        }

        const atrArr = atr(k15.h, k15.l, k15.c, 14);
        const atr15 = atrArr[i15] || 0;
        const atrPct = (atr15 / Math.max(1e-12, d.price)) * 100;

        if (atrPct < s.minAtrPct) return null;
        if (atrPct > s.maxAtrPct) return null;

        const tr = computeTrendScoreDirectional(d.k1d, d.k4h, d.k1h, d.price, side);
        if (tr.score < s.minTrendScore) return null;

        const trigOk = triggerAtIndexDirectional(k15, i15, side, s);
        if (!trigOk) return null;

        const triggerEval = (i) => triggerAtIndexDirectional(k15, i, side, s);
        const bt = backtestUltraDirectional(
            k15,
            s.tpAtrMult,
            s.slAtrMult,
            s.btLookahead,
            s.btMaxSignals,
            side,
            triggerEval
        );

        if (bt.trades < s.btMinTrades) return null;
        if (bt.winrate < s.btMinWinrate) return null;
        if (bt.wrHalf < s.btMinWrHalf) return null;
        if (bt.maxConsecLoss > s.btMaxConsecLoss) return null;
        if (bt.expectancy < s.minExpectancy) return null;

        // ✅ 진입가/익절가/손절가
        // 실전: "신호 뜨자마자 시장가"라고 가정하고 entry=현재가(완성봉 종가)로 둠.
        // (원하면 entry를 다음 15m 시가로 바꾸는 것도 가능하지만, 실시간에선 확정이 안 돼서 현재가가 더 현실적)
        const entry = d.price;

        const tpsl = proposeTPSLDirectional(k15, s.tpAtrMult, s.slAtrMult, entry, side);
        const score = scoreRow(tr.score, bt);

        // ✅ 예상 수익률/손실률(단순 %)
        const pl = calcProfitLossPct(entry, tpsl.tp, tpsl.sl, side);

        // ✅ 손실률 기반 “청산 회피” 권장 최대 레버리지(보수 상한)
        // 버퍼는 기본 0.35% 적용 (너무 빡세면 0.2로 줄여도 됨)
        const maxLevSafe = calcMaxLevSafe(pl.lossPct, 0.35, 125);

        return {
            score,
            side,
            symbol: null,
            pass: passNo === 1 ? "P1" : "P2",

            // backtest
            winrate: bt.winrate,
            trades: bt.trades,
            wrHalf: bt.wrHalf,
            maxConsecLoss: bt.maxConsecLoss,
            expectancy: bt.expectancy,

            // prices
            entry,
            tp: tpsl.tp,
            sl: tpsl.sl,
            profitPct: pl.profitPct,
            lossPct: pl.lossPct,
            maxLevSafe,

            // context
            price: d.price,
            fundingPct: d.fundingPct,
            oiChgPct: d.oiChgPct,
            pxChgPct,
            rsi: rsiNow,
            atrPct,
            trendScore: tr.score,
            qv24: d.qv24,

            why:
                `Trend ${tr.detail}` +
                ` · Funding ${d.fundingPct.toFixed(3)}%` +
                ` · OI15m ${d.oiChgPct.toFixed(2)}%` +
                ` · Px15m ${pxChgPct.toFixed(2)}%` +
                ` · RSI ${rsiNow.toFixed(1)}` +
                ` · ATR% ${atrPct.toFixed(2)}` +
                ` · BT Win ${bt.winrate.toFixed(1)}% (N=${Math.round(bt.trades)})` +
                ` · Half ${bt.wrHalf.toFixed(1)}%` +
                ` · MaxCL ${Math.round(bt.maxConsecLoss)}` +
                ` · Exp ${bt.expectancy.toFixed(3)}`
        };
    }

    async function worker() {
        while (idx < universe.length) {
            const my = idx;
            idx += 1;

            const longNeed = needLong && longCount < minPerSideTarget;
            const shortNeed = needShort && shortCount < minPerSideTarget;

            if (!longNeed && !shortNeed) return;

            const { symbol, qv24 } = universe[my];

            try {
                const d = await loadSymbolData(symbol, qv24);
                if (!d) {
                    await sleep(70);
                    continue;
                }

                if (longNeed) {
                    const r = trySide(d, "LONG");
                    if (r) {
                        r.symbol = symbol;
                        results.push(r);
                        longCount += 1;
                    }
                }

                if (shortNeed) {
                    const r = trySide(d, "SHORT");
                    if (r) {
                        r.symbol = symbol;
                        results.push(r);
                        shortCount += 1;
                    }
                }

                await sleep(55);
            } catch {
                await sleep(120);
            }
        }
    }

    const workers = [];
    for (let i = 0; i < concurrency; i += 1) {
        workers.push(worker());
    }
    await Promise.all(workers);

    return { results, longCount, shortCount };
}

function buildEmailHtml(rows) {
    const ts = new Date().toISOString();

    let html = `<h2>[Scanner] Candidates: ${rows.length}</h2>`;
    html += `<div style="color:#666;font-family:monospace;font-size:12px">${ts}</div>`;
    html += `<br/>`;

    html += `<table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;font-family:monospace;font-size:12px">`;
    html += `<tr>
        <th>Score</th><th>Side</th><th>Symbol</th><th>Pass</th>

        <th>Entry</th><th>TP</th><th>SL</th>
        <th>Profit%</th><th>Loss%</th><th>MaxLevSafe</th>

        <th>BT Win%</th><th>BT N</th><th>BT Half%</th><th>BT MaxCL</th><th>BT Exp</th>

        <th>Funding%</th><th>OI15m%</th><th>Px15m%</th><th>RSI15</th><th>ATR%</th><th>24h QVol</th>
    </tr>`;

    for (const r of rows) {
        const priceDigits = r.price < 1 ? 6 : 3;

        html += `<tr>
            <td><b>${r.score.toFixed(1)}</b></td>
            <td><b>${r.side}</b></td>
            <td><b>${r.symbol}</b></td>
            <td>${r.pass}</td>

            <td>${r.entry.toFixed(priceDigits)}</td>
            <td>${r.tp.toFixed(priceDigits)}</td>
            <td>${r.sl.toFixed(priceDigits)}</td>
            <td>${r.profitPct.toFixed(2)}</td>
            <td>${r.lossPct.toFixed(2)}</td>
            <td><b>${Math.round(r.maxLevSafe)}x</b></td>

            <td>${r.winrate.toFixed(1)}</td>
            <td>${Math.round(r.trades)}</td>
            <td>${r.wrHalf.toFixed(1)}</td>
            <td>${Math.round(r.maxConsecLoss)}</td>
            <td>${r.expectancy.toFixed(3)}</td>

            <td>${r.fundingPct.toFixed(3)}</td>
            <td>${r.oiChgPct.toFixed(2)}</td>
            <td>${r.pxChgPct.toFixed(2)}</td>
            <td>${r.rsi.toFixed(1)}</td>
            <td>${r.atrPct.toFixed(2)}</td>
            <td>${Math.round(r.qv24).toLocaleString()}</td>
        </tr>`;
    }

    html += `</table>`;

    html += `<br/><div style="color:#666;font-family:monospace;font-size:12px">Why(요약):</div>`;
    html += `<ul style="font-family:monospace;font-size:12px">`;
    for (const r of rows.slice(0, 10)) {
        html += `<li><b>${r.symbol} ${r.side}</b> — ${r.why}</li>`;
    }
    html += `</ul>`;

    html += `<hr/>`;
    html += `<div style="color:#666;font-family:monospace;font-size:12px">
        <b>MaxLevSafe</b>는 손절률 기반 “손절 전에 청산될 확률을 낮추기 위한 보수 상한”입니다.
        실제 청산가는 Binance의 유지증거금(MMR), 수수료, 교차/격리, 포지션 크기, 마크프라이스에 의해 달라질 수 있으니 주문창의 예상 청산가를 최종 확인하세요.
    </div>`;

    return html;
}

async function sendMail(rows) {
    const user = process.env.GMAIL_USER;
    const pass = process.env.GMAIL_APP_PASSWORD;
    const to = process.env.MAIL_TO || user;

    if (!user || !pass) throw new Error("Missing env: GMAIL_USER or GMAIL_APP_PASSWORD");

    const transporter = nodemailer.createTransport({
        service: "gmail",
        auth: { user, pass }
    });

    const subject = `[Scanner] ${rows.length} candidates @ ${new Date().toISOString()}`;
    const html = buildEmailHtml(rows);

    await transporter.sendMail({
        from: user,
        to,
        subject,
        html
    });
}

async function scanOnce() {
    const LIMIT_SYMBOLS = Number(process.env.LIMIT_SYMBOLS || 260);
    const MIN_QVOL = Number(process.env.MIN_QVOL || 90000000);
    const CONCURRENCY = Math.max(1, Number(process.env.CONCURRENCY || 4));
    const MIN_PER_SIDE = Math.max(1, Number(process.env.MIN_PER_SIDE || 1));

    const symbols = await getSymbolsUSDTPerp();
    const tickers = await getTickers24h();

    const tickerMap = new Map();
    for (const t of tickers) tickerMap.set(t.symbol, t);

    const ranked = [];
    for (const s of symbols) {
        const t = tickerMap.get(s);
        const qv24 = t ? Number(t.quoteVolume) : 0;
        if (qv24 < MIN_QVOL) continue;
        ranked.push({ symbol: s, qv24 });
    }

    ranked.sort((a, b) => b.qv24 - a.qv24);
    const universe = ranked.slice(0, Math.max(20, LIMIT_SYMBOLS));

    const cache = new Map();

    // PASS1
    const p1 = await scanPass({
        passNo: 1,
        universe,
        concurrency: CONCURRENCY,
        minPerSideTarget: MIN_PER_SIDE,
        needLong: true,
        needShort: true,
        cache
    });

    let all = p1.results.slice();
    let longCount = p1.longCount;
    let shortCount = p1.shortCount;

    // PASS2 (부족한 방향만)
    const needLong2 = longCount < MIN_PER_SIDE;
    const needShort2 = shortCount < MIN_PER_SIDE;

    if (needLong2 || needShort2) {
        const p2 = await scanPass({
            passNo: 2,
            universe,
            concurrency: CONCURRENCY,
            minPerSideTarget: MIN_PER_SIDE,
            needLong: needLong2,
            needShort: needShort2,
            cache
        });

        all = all.concat(p2.results);
        longCount += p2.longCount;
        shortCount += p2.shortCount;
    }

    // 최종 정렬: Score ↓
    all.sort((a, b) => b.score - a.score);

    // 메일은 상위 10개만(과다 발송 방지)
    const top = all.slice(0, 10);

    console.log({
        universe: universe.length,
        pass1: p1.results.length,
        longCount,
        shortCount,
        total: all.length,
        mailTop: top.length
    });

    return top;
}

async function main() {
    const rows = await scanOnce();

    if (rows.length > 0) {
        await sendMail(rows);
        console.log("Mail sent:", rows.length);
    } else {
        console.log("No candidates. No mail.");
    }
}

main().catch((e) => {
    console.error(e);
    process.exit(1);
});
