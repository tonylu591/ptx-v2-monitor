# -*- coding: utf-8 -*-
"""
PTX / USDT Telegram Monitor V2.0
BSC + PancakeSwap V2 ONLY

说明：
- 只使用 PancakeSwap V2 PTX/USDT Pair
- 完全移除 Uniswap V4 / PoolManager / StateView / V4 Pool ID
- 价格来源：V2 Swap Event + V2 Pool Reserves
- PTX/USDT 价格统一定义为：1 PTX = X USDT
- 支持 Telegram: /price /market /flow /stats /pool /status /health /help
- 支持 BUY / SELL、5分钟/1小时资金流、大额/巨鲸提醒、价格提醒
- 使用 .env 保存 Telegram 和 WebSocket 配置
"""

import os
import json
import asyncio
import logging
import time
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from collections import deque
from dataclasses import dataclass, field
from decimal import Decimal, getcontext
from typing import Optional

import requests
from dotenv import load_dotenv
from web3 import AsyncWeb3, Web3
from web3.providers import WebSocketProvider

getcontext().prec = 40
load_dotenv()

VERSION = "V2.0"

# ============================================================
# BSC / PancakeSwap V2 配置
# ============================================================
PTX_ADDRESS = os.getenv(
    "PTX_ADDRESS",
    "0x86d4C9E2c3c1eC4BCA0AC458bfCEc8A5f7160F13",
).lower()

USDT_ADDRESS = os.getenv(
    "USDT_ADDRESS",
    "0x55d398326f99059ff775485246999027B3197955",
).lower()

# 已使用并验证过的 PancakeSwap V2 PTX/USDT Pair
PAIR_ADDRESS = os.getenv(
    "PANCAKE_V2_PAIR",
    "0x88cDeEDcD4aE970Cd4DD1Cb3b0F0519F13D59363",
).lower()

PTX_DECIMALS = int(os.getenv("PTX_DECIMALS", "18"))
USDT_DECIMALS = int(os.getenv("USDT_DECIMALS", "18"))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

WS_URLS = [
    os.getenv("BSC_WS_URL", "").strip(),
    "wss://bsc-rpc.publicnode.com",
    "wss://bsc.publicnode.com",
]
WS_URLS = list(dict.fromkeys([u for u in WS_URLS if u]))

LARGE_TRADE_USD = Decimal(os.getenv("LARGE_TRADE_USD", "100"))
WHALE_TRADE_USD = Decimal(os.getenv("WHALE_TRADE_USD", "1000"))

PRICE_ALERT_UP = [
    Decimal(x.strip())
    for x in os.getenv("PRICE_ALERT_UP", "0.02,0.05").split(",")
    if x.strip()
]
PRICE_ALERT_DOWN = [
    Decimal(x.strip())
    for x in os.getenv("PRICE_ALERT_DOWN", "0.01,0.005").split(",")
    if x.strip()
]

FLOW_WINDOW_5M = 300
FLOW_WINDOW_1H = 3600
REPORT_INTERVAL_SECONDS = int(os.getenv("REPORT_INTERVAL_SECONDS", "3600"))
MARKET_REPORT_INTERVAL_SECONDS = int(
    os.getenv("MARKET_REPORT_INTERVAL_SECONDS", "300")
)
COMMAND_POLL_SECONDS = int(os.getenv("COMMAND_POLL_SECONDS", "3"))
TX_DEDUP_SECONDS = int(os.getenv("TX_DEDUP_SECONDS", "7200"))
SEND_ALL_TRADES = os.getenv("SEND_ALL_TRADES", "true").lower() in {
    "1", "true", "yes", "on"
}
COMPACT_TRADES = os.getenv("COMPACT_TRADES", "true").lower() in {
    "1", "true", "yes", "on"
}
SHOW_TX_HASH = os.getenv("SHOW_TX_HASH", "false").lower() in {
    "1", "true", "yes", "on"
}

SWAP_TOPIC = Web3.keccak(
    text="Swap(address,uint256,uint256,uint256,uint256,address)"
).hex().lower()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("PTX-V2")


# ============================================================
# Render Free Web Service health endpoint
# Render Free requires a web service to listen on PORT.
# This endpoint is only for health checks; trading logic remains V2-only.
# ============================================================
class HealthHandler(BaseHTTPRequestHandler):
       def do_GET(self):

# ==============================
# Dashboard
# ==============================
if self.path == "/":

    dashboard_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "dashboard.html"
    )

    try:
        with open(dashboard_path, "rb") as f:
            body = f.read()

        self.send_response(200)
        self.send_header(
            "Content-Type",
            "text/html; charset=utf-8"
        )
        self.send_header(
            "Content-Length",
            str(len(body))
        )
        self.end_headers()

        self.wfile.write(body)
        return

    except Exception as exc:
        logger.exception(
            "Dashboard读取失败: %s",
            exc
        )

        body = b"Dashboard unavailable"

        self.send_response(500)
        self.send_header(
            "Content-Type",
            "text/plain; charset=utf-8"
        )
        self.send_header(
            "Content-Length",
            str(len(body))
        )
        self.end_headers()

        self.wfile.write(body)
        return


# ==============================
# Health check
# ==============================
if self.path == "/health":

    body = b"PTX V2 monitor is running"

    self.send_response(200)
    self.send_header(
        "Content-Type",
        "text/plain; charset=utf-8"
    )
    self.send_header(
        "Content-Length",
        str(len(body))
    )
    self.end_headers()

    self.wfile.write(body)
    return

        # ==============================
        # Dashboard API
        # ==============================
        if self.path == "/api/status":

            flow5_trades, flow5_buy, flow5_sell, flow5_net = STATE.flow(300)

            flow60_trades, flow60_buy, flow60_sell, flow60_net = STATE.flow(3600)

            recent = list(STATE.recent_trades)[-20:]

            trades = []

            for trade in recent:

                trades.append({
                    "timestamp": trade.timestamp,
                    "side": trade.side,
                    "price": float(trade.price),
                    "ptx_amount": float(trade.ptx_amount),
                    "usdt_amount": float(trade.usdt_amount),
                    "tx_hash": trade.tx_hash,
                    "block_number": trade.block_number
                })

            data = {
                "status": "LIVE",

                "price": (
                    float(STATE.latest_price)
                    if STATE.latest_price is not None
                    else 0
                ),

                "pool_ptx": (
                    float(STATE.pool_ptx)
                    if STATE.pool_ptx is not None
                    else 0
                ),

                "pool_usdt": (
                    float(STATE.pool_usdt)
                    if STATE.pool_usdt is not None
                    else 0
                ),

                "last_block": STATE.last_block,

                "flow5": {
                    "buy": float(flow5_buy),
                    "sell": float(flow5_sell),
                    "net": float(flow5_net),
                    "trades": len(flow5_trades)
                },

                "flow60": {
                    "buy": float(flow60_buy),
                    "sell": float(flow60_sell),
                    "net": float(flow60_net),
                    "trades": len(flow60_trades)
                },

                "trades": trades
            }

            body = json.dumps(
                data,
                ensure_ascii=False
            ).encode("utf-8")

            self.send_response(200)

            self.send_header(
                "Content-Type",
                "application/json; charset=utf-8"
            )

            self.send_header(
                "Access-Control-Allow-Origin",
                "*"
            )

            self.send_header(
                "Content-Length",
                str(len(body))
            )

            self.end_headers()

            self.wfile.write(body)
            return

        # ==============================
        # Not Found
        # ==============================

        body = b"Not Found"

        self.send_response(404)

        self.send_header(
            "Content-Type",
            "text/plain; charset=utf-8"
        )

        self.send_header(
            "Content-Length",
            str(len(body))
        )

        self.end_headers()

        self.wfile.write(body) 
   
   


def start_health_server():
    port = int(os.getenv("PORT", "10000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), HealthHandler)
    logger.info("Health HTTP server listening on 0.0.0.0:%s", port)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


@dataclass
class Trade:
    timestamp: float
    side: str
    price: Decimal
    ptx_amount: Decimal
    usdt_amount: Decimal
    tx_hash: str
    block_number: int


@dataclass
class MonitorState:
    latest_price: Optional[Decimal] = None
    latest_trade: Optional[Trade] = None
    started_at: float = field(default_factory=time.time)
    last_block: Optional[int] = None
    last_pool_check: float = 0.0
    pool_ptx: Optional[Decimal] = None
    pool_usdt: Optional[Decimal] = None
    recent_trades: deque = field(default_factory=lambda: deque(maxlen=5000))
    seen_txs: dict = field(default_factory=dict)
    last_price_alert_up: set = field(default_factory=set)
    last_price_alert_down: set = field(default_factory=set)
    websocket_ok: bool = False
    last_error: Optional[str] = None

    def add_trade(self, trade: Trade):
        now = time.time()
        self.recent_trades.append(trade)
        self.latest_price = trade.price
        self.latest_trade = trade
        self.last_block = trade.block_number
        self.seen_txs[trade.tx_hash] = now

        cutoff = now - TX_DEDUP_SECONDS
        stale = [tx for tx, ts in self.seen_txs.items() if ts < cutoff]
        for tx in stale:
            self.seen_txs.pop(tx, None)

    def trades_since(self, seconds: int):
        cutoff = time.time() - seconds
        return [t for t in self.recent_trades if t.timestamp >= cutoff]

    def flow(self, seconds: int):
        trades = self.trades_since(seconds)
        buy = sum(
            (t.usdt_amount for t in trades if t.side == "BUY"),
            Decimal("0"),
        )
        sell = sum(
            (t.usdt_amount for t in trades if t.side == "SELL"),
            Decimal("0"),
        )
        return trades, buy, sell, buy - sell


STATE = MonitorState()


def telegram_configured() -> bool:
    return bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)


def send_telegram(message: str, chat_id: Optional[str] = None) -> bool:
    if not telegram_configured():
        logger.warning("Telegram未配置：请检查 .env")
        return False

    target = chat_id or TELEGRAM_CHAT_ID
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    try:
        response = requests.post(
            url,
            json={
                "chat_id": target,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=15,
        )
        if response.ok:
            logger.info("Telegram message sent.")
            return True

        logger.error(
            "Telegram发送失败 | HTTP %s | %s",
            response.status_code,
            response.text[:500],
        )
    except Exception as exc:
        logger.error("Telegram连接错误: %s", exc)

    return False


async def send_message(message: str, chat_id: Optional[str] = None) -> bool:
    return await asyncio.to_thread(send_telegram, message, chat_id)


async def get_pair_tokens(w3: AsyncWeb3):
    abi = [
        {
            "inputs": [],
            "name": "token0",
            "outputs": [{"name": "", "type": "address"}],
            "stateMutability": "view",
            "type": "function",
        },
        {
            "inputs": [],
            "name": "token1",
            "outputs": [{"name": "", "type": "address"}],
            "stateMutability": "view",
            "type": "function",
        },
    ]

    pair = w3.eth.contract(
        address=Web3.to_checksum_address(PAIR_ADDRESS),
        abi=abi,
    )

    token0 = (await pair.functions.token0().call()).lower()
    token1 = (await pair.functions.token1().call()).lower()

    logger.info("Pair:   %s", PAIR_ADDRESS)
    logger.info("Token0: %s", token0)
    logger.info("Token1: %s", token1)

    if {token0, token1} != {PTX_ADDRESS, USDT_ADDRESS}:
        raise RuntimeError(
            "配置的 Pancake V2 Pair 不是预期的 PTX/USDT Pair"
        )

    return token0, token1


def decode_uint256_words(data_value):
    if isinstance(data_value, (bytes, bytearray)):
        raw = bytes(data_value)
    else:
        text = str(data_value)
        if text.startswith("0x"):
            text = text[2:]
        raw = bytes.fromhex(text)

    if len(raw) < 128:
        raise ValueError(f"Swap data长度异常: {len(raw)} bytes")

    return [
        int.from_bytes(raw[i:i + 32], byteorder="big", signed=False)
        for i in range(0, 128, 32)
    ]


async def get_pool_reserves(w3: AsyncWeb3, token0: str, token1: str):
    abi = [
        {
            "inputs": [],
            "name": "getReserves",
            "outputs": [
                {"name": "_reserve0", "type": "uint112"},
                {"name": "_reserve1", "type": "uint112"},
                {"name": "_blockTimestampLast", "type": "uint32"},
            ],
            "stateMutability": "view",
            "type": "function",
        }
    ]

    pair = w3.eth.contract(
        address=Web3.to_checksum_address(PAIR_ADDRESS),
        abi=abi,
    )
    reserve0, reserve1, _ = await pair.functions.getReserves().call()

    if token0 == PTX_ADDRESS and token1 == USDT_ADDRESS:
        ptx_reserve = Decimal(reserve0) / Decimal(10 ** PTX_DECIMALS)
        usdt_reserve = Decimal(reserve1) / Decimal(10 ** USDT_DECIMALS)
    elif token0 == USDT_ADDRESS and token1 == PTX_ADDRESS:
        usdt_reserve = Decimal(reserve0) / Decimal(10 ** USDT_DECIMALS)
        ptx_reserve = Decimal(reserve1) / Decimal(10 ** PTX_DECIMALS)
    else:
        raise RuntimeError("Pancake V2 token0/token1 不匹配")

    if ptx_reserve <= 0 or usdt_reserve <= 0:
        raise RuntimeError("Pancake V2 Pool 储备异常")

    STATE.pool_ptx = ptx_reserve
    STATE.pool_usdt = usdt_reserve
    STATE.last_pool_check = time.time()
    STATE.latest_price = usdt_reserve / ptx_reserve

    logger.info(
        "V2 Pool | PTX=%s | USDT=%s | Spot=$%s",
        f"{ptx_reserve:,.4f}",
        f"{usdt_reserve:,.4f}",
        f"{STATE.latest_price:.10f}",
    )

    return ptx_reserve, usdt_reserve


def format_flow(seconds: int):
    trades, buy, sell, net = STATE.flow(seconds)
    ratio = buy / sell if sell > 0 else None
    return trades, buy, sell, net, ratio


def build_flow_text(seconds: int, title: str):
    trades, buy, sell, net, ratio = format_flow(seconds)
    ratio_text = f"{ratio:.2f}" if ratio is not None else "∞"
    net_emoji = "🟢" if net > 0 else "🔴" if net < 0 else "🟡"

    return (
        f"<b>{title}</b>\n\n"
        f"🟢 BUY: ${buy:,.2f}\n"
        f"🔴 SELL: ${sell:,.2f}\n"
        f"{net_emoji} NET FLOW: ${net:+,.2f}\n"
        f"⚖️ BUY/SELL: {ratio_text}\n"
        f"📊 Trades: {len(trades)}"
    )


def price_alert_messages(old_price: Optional[Decimal], new_price: Decimal):
    if old_price is None:
        return []

    messages = []

    for level in PRICE_ALERT_UP:
        if old_price < level <= new_price and level not in STATE.last_price_alert_up:
            messages.append(
                "🚀 <b>PTX PRICE BREAKOUT</b>\n\n"
                f"Price crossed <b>${level:.8f}</b>\n"
                f"Current: <b>${new_price:.8f}</b>"
            )
            STATE.last_price_alert_up.add(level)
            STATE.last_price_alert_down.discard(level)

    for level in PRICE_ALERT_DOWN:
        if old_price > level >= new_price and level not in STATE.last_price_alert_down:
            messages.append(
                "⚠️ <b>PTX PRICE BREAKDOWN</b>\n\n"
                f"Price crossed <b>${level:.8f}</b>\n"
                f"Current: <b>${new_price:.8f}</b>"
            )
            STATE.last_price_alert_down.add(level)
            STATE.last_price_alert_up.discard(level)

    for level in PRICE_ALERT_UP:
        if new_price < level:
            STATE.last_price_alert_up.discard(level)

    for level in PRICE_ALERT_DOWN:
        if new_price > level:
            STATE.last_price_alert_down.discard(level)

    return messages


async def process_swap(w3: AsyncWeb3, log, token0: str, token1: str):
    try:
        (
            amount0_in_raw,
            amount1_in_raw,
            amount0_out_raw,
            amount1_out_raw,
        ) = decode_uint256_words(log["data"])

        def scale(raw, token):
            decimals = PTX_DECIMALS if token == PTX_ADDRESS else USDT_DECIMALS
            return Decimal(raw) / Decimal(10 ** decimals)

        amount0_in = scale(amount0_in_raw, token0)
        amount1_in = scale(amount1_in_raw, token1)
        amount0_out = scale(amount0_out_raw, token0)
        amount1_out = scale(amount1_out_raw, token1)

        if token0 == PTX_ADDRESS:
            ptx_in, ptx_out = amount0_in, amount0_out
            usdt_in, usdt_out = amount1_in, amount1_out
        else:
            ptx_in, ptx_out = amount1_in, amount1_out
            usdt_in, usdt_out = amount0_in, amount0_out

        if usdt_in > 0 and ptx_out > 0:
            side = "BUY"
            ptx_amount = ptx_out
            usdt_amount = usdt_in
        elif ptx_in > 0 and usdt_out > 0:
            side = "SELL"
            ptx_amount = ptx_in
            usdt_amount = usdt_out
        else:
            logger.warning("无法判断 V2 Swap 方向")
            return

        if ptx_amount <= 0 or usdt_amount <= 0:
            return

        # 唯一价格定义：1 PTX = X USDT
        price = usdt_amount / ptx_amount

        tx_value = log["transactionHash"]
        tx_hash = tx_value if isinstance(tx_value, str) else bytes(tx_value).hex()
        if not tx_hash.startswith("0x"):
            tx_hash = "0x" + tx_hash
        tx_hash = tx_hash.lower()

        if tx_hash in STATE.seen_txs:
            return

        block_value = log["blockNumber"]
        block_number = (
            int(block_value, 16)
            if isinstance(block_value, str)
            else int(block_value)
        )

        old_price = STATE.latest_price
        trade = Trade(
            timestamp=time.time(),
            side=side,
            price=price,
            ptx_amount=ptx_amount,
            usdt_amount=usdt_amount,
            tx_hash=tx_hash,
            block_number=block_number,
        )
        STATE.add_trade(trade)
        STATE.last_error = None

        emoji = "🟢" if side == "BUY" else "🔴"
        if usdt_amount >= WHALE_TRADE_USD:
            title = f"🐋 <b>PTX WHALE {side}</b>"
            signal = "🔥 <b>Whale activity detected</b>"
        elif usdt_amount >= LARGE_TRADE_USD:
            title = f"💰 <b>PTX LARGE {side}</b>"
            signal = "📌 <b>Large transaction</b>"
        else:
            title = f"{emoji} <b>PTX/USDT {side}</b>"
            signal = ""

        if COMPACT_TRADES and usdt_amount < LARGE_TRADE_USD:
            message = (
                f"{title}\n\n"
                f"💵 <b>${usdt_amount:,.2f}</b>  ·  "
                f"🪙 {ptx_amount:,.2f} PTX\n"
                f"📈 1 PTX = <b>${price:.8f}</b>\n\n"
                f'<a href="https://bscscan.com/tx/{tx_hash}">🔗 View Transaction</a>'
            )
        else:
            message = (
                f"{title}\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"💵 <b>Price</b>    ${price:.8f}\n"
                f"🪙 <b>Amount</b>   {ptx_amount:,.4f} PTX\n"
                f"💰 <b>Value</b>    ${usdt_amount:,.2f}\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"{signal}\n"
                f"⛓ Block       {block_number}\n"
                + (f"TX            <code>{tx_hash}</code>\n" if SHOW_TX_HASH else "")
                + f'<a href="https://bscscan.com/tx/{tx_hash}">🔗 View on BscScan</a>'
            )

        logger.info(
            "V2 Swap | side=%s | price=$%s | PTX=%s | USDT=%s | block=%s | tx=%s",
            side,
            f"{price:.10f}",
            f"{ptx_amount:,.6f}",
            f"{usdt_amount:,.6f}",
            block_number,
            tx_hash,
        )

        if telegram_configured() and (
            SEND_ALL_TRADES or usdt_amount >= LARGE_TRADE_USD
        ):
            await send_message(message)

        for alert in price_alert_messages(old_price, price):
            if telegram_configured():
                await send_message(alert)

    except Exception as exc:
        STATE.last_error = f"V2 Swap处理失败: {exc}"
        logger.exception("V2 Swap处理失败: %s", exc)


def build_market_status_message():
    trades, buy, sell, net, ratio = format_flow(FLOW_WINDOW_5M)
    ratio_text = "—" if ratio is None else f"{ratio:.2f}"

    price = (
        f"${STATE.latest_price:.8f}"
        if STATE.latest_price is not None else "Waiting..."
    )

    if len(trades) == 0:
        pressure = "🟡 WAITING FOR TRADES"
    elif net > 0 and ratio is not None and ratio >= Decimal("1.5"):
        pressure = "🟢 BUY PRESSURE"
    elif net < 0 and ratio is not None and ratio <= Decimal("0.67"):
        pressure = "🔴 SELL PRESSURE"
    else:
        pressure = "🟡 BALANCED"

    return (
        f"📊 <b>PTX MARKET STATUS {VERSION}</b>\n"
        "━━━━━━━━━━━━━━━━\n\n"
        f"🟡 <b>Pancake V2</b>   {price}\n"
        f"📍 <b>Source</b>       BSC PancakeSwap V2\n"
        f"🧩 <b>Pair</b>        <code>{PAIR_ADDRESS}</code>\n\n"
        f"🟢 <b>BUY</b>        ${buy:,.2f}\n"
        f"🔴 <b>SELL</b>       ${sell:,.2f}\n"
        f"📈 <b>NET FLOW</b>   ${net:+,.2f}\n"
        f"⚖️ <b>BUY/SELL</b>   {ratio_text}\n"
        f"🔥 <b>Trades</b>     {len(trades)}\n\n"
        "━━━━━━━━━━━━━━━━\n\n"
        f"<b>{pressure}</b>"
    )


def build_price_message():
    if STATE.latest_price is None:
        return "📊 <b>PTX PRICE</b>\n\n暂无V2价格数据。"

    latest = STATE.latest_trade
    extra = ""
    if latest:
        extra = (
            f"\nLast: {latest.side}\n"
            f"PTX: {latest.ptx_amount:,.6f}\n"
            f"USDT: ${latest.usdt_amount:,.6f}\n"
            f"Block: {latest.block_number}\n"
            f"Source: PancakeSwap V2 Swap"
        )

    return (
        "📈 <b>PTX/USDT V2 PRICE</b>\n\n"
        f"<b>1 PTX = ${STATE.latest_price:.8f} USDT</b>\n"
        f"{extra}"
    )


def build_stats_message():
    trades, buy, sell, net, ratio = format_flow(FLOW_WINDOW_1H)
    ratio_text = f"{ratio:.2f}" if ratio is not None else "∞"
    price_text = (
        f"${STATE.latest_price:.8f}"
        if STATE.latest_price is not None else "N/A"
    )

    return (
        "📊 <b>PTX V2 1H STATS</b>\n\n"
        f"Price: {price_text}\n"
        f"Trades: {len(trades)}\n"
        f"BUY: ${buy:,.2f}\n"
        f"SELL: ${sell:,.2f}\n"
        f"NET: ${net:+,.2f}\n"
        f"BUY/SELL: {ratio_text}"
    )


def build_pool_message():
    if STATE.pool_ptx is None or STATE.pool_usdt is None:
        return "💧 <b>PTX/USDT V2 POOL</b>\n\nPool数据尚未读取。"

    spot = STATE.pool_usdt / STATE.pool_ptx if STATE.pool_ptx else Decimal("0")
    estimated_tvl = STATE.pool_usdt * Decimal("2")

    return (
        "💧 <b>PTX/USDT V2 POOL</b>\n\n"
        f"PTX Reserve: {STATE.pool_ptx:,.4f}\n"
        f"USDT Reserve: ${STATE.pool_usdt:,.4f}\n"
        f"Spot: 1 PTX = ${spot:.8f} USDT\n"
        f"Estimated TVL: ${estimated_tvl:,.2f}\n\n"
        "<i>TVL is an approximate 2×USDT-reserve estimate.</i>"
    )


def build_status_message():
    uptime = int(time.time() - STATE.started_at)
    h, rem = divmod(uptime, 3600)
    m, s = divmod(rem, 60)
    return (
        f"🟢 <b>PTX MONITOR {VERSION} STATUS</b>\n\n"
        f"Network: BSC\n"
        f"DEX: PancakeSwap V2\n"
        f"WebSocket: {'🟢 CONNECTED' if STATE.websocket_ok else '🔴 OFF'}\n"
        f"Telegram: {'🟢 OK' if telegram_configured() else '🔴 NOT CONFIGURED'}\n"
        f"Uptime: {h}h {m}m {s}s\n"
        f"Last block: {STATE.last_block or 'N/A'}\n"
        f"Trades in memory: {len(STATE.recent_trades)}\n"
        f"Price: {('$' + format(STATE.latest_price, '.8f')) if STATE.latest_price is not None else 'Waiting...'}\n"
        f"Last error: {STATE.last_error or 'NONE'}"
    )


def build_health_message():
    age = "—"
    if STATE.latest_trade:
        age = f"{int(time.time() - STATE.latest_trade.timestamp)}s"
    return (
        f"🩺 <b>PTX V2 HEALTH {VERSION}</b>\n\n"
        f"🟢 WebSocket: {'OK' if STATE.websocket_ok else 'OFF'}\n"
        f"🟢 Pair: {PAIR_ADDRESS}\n"
        f"🟢 Price: {'OK' if STATE.latest_price is not None else 'WAITING'}\n"
        f"🟢 Pool: {'OK' if STATE.pool_ptx and STATE.pool_usdt else 'WAITING'}\n"
        f"Last trade age: {age}\n"
        f"Last error: {STATE.last_error or 'NONE'}"
    )


def build_help_message():
    return (
        f"🤖 <b>PTX MONITOR {VERSION}</b>\n\n"
        "📈 /price — 当前PTX/V2价格\n"
        "📊 /market — V2市场状态\n"
        "💰 /flow — 5分钟资金流\n"
        "📋 /stats — 1小时统计\n"
        "💧 /pool — V2池储备\n"
        "🩺 /health — 系统健康\n"
        "🟢 /status — 系统状态\n"
        "❓ /help — 命令说明\n\n"
        "ℹ️ 价格唯一来源：PancakeSwap V2 PTX/USDT"
    )


async def handle_command(command: str, chat_id: str):
    command = command.split("@", 1)[0].lower().strip()

    if command == "/price":
        await send_message(build_price_message(), chat_id)
    elif command == "/market":
        await send_message(build_market_status_message(), chat_id)
    elif command == "/stats":
        await send_message(build_stats_message(), chat_id)
    elif command == "/flow":
        await send_message(build_flow_text(FLOW_WINDOW_5M, "📊 PTX V2 5MIN FLOW"), chat_id)
    elif command == "/pool":
        await send_message(build_pool_message(), chat_id)
    elif command == "/health":
        await send_message(build_health_message(), chat_id)
    elif command == "/status":
        await send_message(build_status_message(), chat_id)
    elif command in ("/help", "/start"):
        await send_message(build_help_message(), chat_id)


async def telegram_command_loop():
    if not telegram_configured():
        logger.warning("Telegram命令监听未启动：缺少Bot Token或Chat ID")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    offset = 0

    while True:
        try:
            response = await asyncio.to_thread(
                requests.get,
                url,
                params={"timeout": 20, "offset": offset},
                timeout=30,
            )

            if not response.ok:
                logger.warning(
                    "Telegram getUpdates失败 | HTTP %s | %s",
                    response.status_code,
                    response.text[:300],
                )
                await asyncio.sleep(COMMAND_POLL_SECONDS)
                continue

            payload = response.json()
            for update in payload.get("result", []):
                offset = max(offset, int(update["update_id"]) + 1)
                message = update.get("message") or update.get("edited_message")
                if not message:
                    continue

                chat_id = str(message.get("chat", {}).get("id", ""))
                if chat_id != TELEGRAM_CHAT_ID:
                    continue

                text = (message.get("text") or "").strip()
                if text.startswith("/"):
                    await handle_command(text.split()[0], chat_id)

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("Telegram命令监听异常: %s", exc)
            await asyncio.sleep(COMMAND_POLL_SECONDS)


async def hourly_report_loop():
    if REPORT_INTERVAL_SECONDS <= 0:
        return

    while True:
        await asyncio.sleep(MARKET_REPORT_INTERVAL_SECONDS)
        try:
            report = (
                f"{build_market_status_message()}\n\n"
                f"{build_flow_text(FLOW_WINDOW_1H, '📋 PTX V2 1H FLOW')}\n\n"
                f"{build_pool_message()}"
            )
            if telegram_configured():
                await send_message(report)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("市场报告生成失败: %s", exc)


async def monitor_once(ws_url: str):
    logger.info("正在连接 BSC WebSocket: %s", ws_url)

    provider = WebSocketProvider(
        ws_url,
        websocket_kwargs={
            "ping_interval": 20,
            "ping_timeout": 20,
        },
    )

    async with AsyncWeb3(provider) as w3:
        chain_id = await w3.eth.chain_id
        if chain_id != 56:
            raise RuntimeError(f"错误网络 Chain ID={chain_id}，需要56")

        logger.info("BSC WebSocket连接成功 | Chain ID: 56")
        token0, token1 = await get_pair_tokens(w3)

        logger.info("=" * 68)
        logger.info("PTX / PancakeSwap V2 BSC MONITOR %s", VERSION)
        logger.info("PTX:       %s", PTX_ADDRESS)
        logger.info("USDT:      %s", USDT_ADDRESS)
        logger.info("PAIR:      %s", PAIR_ADDRESS)
        logger.info("WS:        %s", ws_url)
        logger.info("Large:     $%s", LARGE_TRADE_USD)
        logger.info("Whale:     $%s", WHALE_TRADE_USD)
        logger.info("Price:     ONLY PancakeSwap V2")
        logger.info("=" * 68)

        STATE.websocket_ok = True
        STATE.last_error = None

        try:
            await get_pool_reserves(w3, token0, token1)
        except Exception as exc:
            STATE.last_error = f"Initial pool read failed: {exc}"
            logger.warning("初始V2 Pool读取失败: %s", exc)

        if telegram_configured():
            await send_message(
                f"🟢 <b>PTX/USDT V2 Monitor {VERSION} 已启动</b>\n\n"
                f"Pair:\n<code>{PAIR_ADDRESS}</code>\n\n"
                "BSC WebSocket 实时监听已启动。\n"
                "价格来源：<b>PancakeSwap V2 ONLY</b>\n"
                f"大额阈值: ${LARGE_TRADE_USD:,.0f}\n"
                f"巨鲸阈值: ${WHALE_TRADE_USD:,.0f}"
            )

        subscription_id = await w3.eth.subscribe(
            "logs",
            {
                "address": Web3.to_checksum_address(PAIR_ADDRESS),
                "topics": [SWAP_TOPIC],
            },
        )

        logger.info("Swap订阅成功 | Subscription ID: %s", subscription_id)
        logger.info("正在等待 PancakeSwap V2 PTX/USDT 新成交...")

        async for response in w3.socket.process_subscriptions():
            if response.get("subscription") != subscription_id:
                continue

            log = response.get("result")
            if not log:
                continue
            if log.get("removed"):
                logger.warning("收到reorg日志，忽略")
                continue

            await process_swap(w3, log, token0, token1)


def main():
    health_server = start_health_server()
    async def runner():
        background_tasks = [
            asyncio.create_task(telegram_command_loop()),
            asyncio.create_task(hourly_report_loop()),
        ]

        try:
            while True:
                connected = False
                for ws_url in WS_URLS:
                    try:
                        await monitor_once(ws_url)
                        connected = True
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        STATE.websocket_ok = False
                        STATE.last_error = repr(exc)
                        logger.exception("V2 WebSocket监听失败: %s", exc)
                        logger.info("5秒后尝试下一个WebSocket节点...")
                        await asyncio.sleep(5)

                if not connected:
                    await asyncio.sleep(10)
        finally:
            for task in background_tasks:
                task.cancel()
            await asyncio.gather(*background_tasks, return_exceptions=True)

    try:
        asyncio.run(runner())
    except KeyboardInterrupt:
        logger.info("PTX V2 monitor stopped.")


if __name__ == "__main__":
    main()
