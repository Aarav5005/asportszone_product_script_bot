"""
pip install pytelegrambotapi requests groq schedule python-dotenv
"""

import base64
import json
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus
import re
import schedule
import os
from dotenv import load_dotenv
import logging

import requests
import telebot
from groq import Groq
from telebot.apihelper import ApiTelegramException

# Suppress TeleBot logging noise
logging.getLogger("telebot").setLevel(logging.WARNING)

# Load environment variables from .env file
load_dotenv()

daily_products_added = []
known_chat_ids = set()

# ─── CONFIG (Load from Environment Variables) ─────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ALLOWED_USERNAMES = os.getenv("ALLOWED_USERNAMES", "Aarav5005,Asportszone,asportszone22").split(",")
ALLOWED_USERNAMES = [u.strip() for u in ALLOWED_USERNAMES]

SHOPIFY_STORE = os.getenv("SHOPIFY_STORE")
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
SERPAPI_KEY = os.getenv("SERPAPI_KEY")

GROUP_TIMEOUT_SECONDS = int(os.getenv("GROUP_TIMEOUT_SECONDS", "180"))

# Validate required variables
if not all([TELEGRAM_BOT_TOKEN, SHOPIFY_STORE, SHOPIFY_ACCESS_TOKEN, GROQ_API_KEY, SERPAPI_KEY]):
    print("ERROR: Missing required environment variables!")
    print("Please set: TELEGRAM_BOT_TOKEN, SHOPIFY_STORE, SHOPIFY_ACCESS_TOKEN, GROQ_API_KEY, SERPAPI_KEY")
    exit(1)
# ───────────────────────────────────────────────────────────────────────────────

bot = Bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)
groq_client = Groq(api_key=GROQ_API_KEY)

SHOPIFY_PRODUCTS_URL = f"https://{SHOPIFY_STORE}/admin/api/2024-01/products.json"

buffers_lock = threading.Lock()


def log(symbol: str, message: str) -> None:
    print(f"{datetime.now().isoformat(timespec='seconds')} {symbol} {message}")


# ─── PRODUCT BUFFER ───────────────────────────────────────────────────────────

@dataclass
class ProductBuffer:
    chat_id: int
    username: str
    caption: str = ""
    photo_file_ids: List[str] = field(default_factory=list)
    last_activity: float = field(default_factory=time.time)

    def touch(self):
        self.last_activity = time.time()


product_buffers: Dict[int, ProductBuffer] = {}


def get_or_create_buffer(chat_id: int, username: str) -> ProductBuffer:
    if chat_id not in product_buffers:
        product_buffers[chat_id] = ProductBuffer(chat_id=chat_id, username=username)
    return product_buffers[chat_id]


# ─── TELEGRAM HANDLERS ────────────────────────────────────────────────────────

def is_allowed(message) -> bool:
    username = (message.from_user.username or "").lower()
    return username.lower() in [u.lower() for u in ALLOWED_USERNAMES]


def remember_chat_id(message) -> None:
    if is_allowed(message):
        known_chat_ids.add(message.chat.id)


@bot.message_handler(commands=["start"])
def handle_start(message):
    if not is_allowed(message):
        bot.reply_to(message, "❌ Not authorized.")
        return
    remember_chat_id(message)
    bot.reply_to(message, "✅ Bot ready! Send product caption + images to create Shopify listings.")


@bot.message_handler(commands=["flush"])
def handle_flush(message):
    if not is_allowed(message):
        return
    remember_chat_id(message)
    chat_id = message.chat.id
    with buffers_lock:
        buf = product_buffers.pop(chat_id, None)
    if buf and buf.caption.strip():
        bot.reply_to(message, "⏳ Processing product now...")
        threading.Thread(target=process_buffer, args=(buf,), daemon=True).start()
    else:
        bot.reply_to(message, "No buffered product found.")


@bot.message_handler(commands=["status"])
def handle_status(message):
    if not is_allowed(message):
        return
    remember_chat_id(message)
    chat_id = message.chat.id
    with buffers_lock:
        buf = product_buffers.get(chat_id)
    if buf:
        bot.reply_to(message, 
            f"📦 Buffer status:\n"
            f"Caption: {buf.caption[:60] or '[none]'}\n"
            f"Images: {len(buf.photo_file_ids)}\n"
            f"Last activity: {int(time.time() - buf.last_activity)}s ago"
        )
    else:
        bot.reply_to(message, "No active buffer.")


@bot.message_handler(content_types=["text"])
def handle_text(message):
    if not is_allowed(message):
        return
    remember_chat_id(message)
    chat_id = message.chat.id
    username = message.from_user.username or ""
    text = message.text.strip()

    with buffers_lock:
        buf = get_or_create_buffer(chat_id, username)
        buf.touch()
        if buf.caption:
            buf.caption = f"{buf.caption}\n{text}"
        else:
            buf.caption = text

    log("📝", f"Caption stored: {text[:80]}")
    bot.reply_to(message, f"📝 Caption saved. Now send the product images.")


@bot.message_handler(content_types=["photo"])
def handle_photo(message):
    if not is_allowed(message):
        return
    remember_chat_id(message)
    chat_id = message.chat.id
    username = message.from_user.username or ""

    # Get highest resolution photo
    photo = message.photo[-1]
    file_id = photo.file_id

    # Extract caption from photo if present
    caption = (message.caption or "").strip()

    with buffers_lock:
        buf = get_or_create_buffer(chat_id, username)
        buf.touch()
        buf.photo_file_ids.append(file_id)
        if caption and not buf.caption:
            buf.caption = caption
            log("📝", f"Caption from image: {caption[:80]}")

    img_count = len(buf.photo_file_ids)
    log("🖼", f"Image #{img_count} buffered for chat {chat_id}")
    bot.reply_to(message, f"🖼 Image {img_count} received. Send more or wait 3 min to auto-create listing. Use /flush to create now.")


# ─── PROCESS BUFFER → SHOPIFY ─────────────────────────────────────────────────

def fetch_brand_images(product_title: str) -> list:
    """Fetch product images using SerpApi Google Image Search."""

    log("🔍", f"Searching images for: {product_title}")

    try:
        params = {
            "engine": "google_images",
            "q": product_title + " cricket bat product",
            "api_key": SERPAPI_KEY,
            "num": 5,
            "safe": "active",
        }

        response = requests.get(
            "https://serpapi.com/search.json",
            params=params,
            timeout=15
        )
        response.raise_for_status()
        data = response.json()

        images_results = data.get("images_results", [])
        if not images_results:
            log("✗", "No images found via SerpApi")
            return []

        image_urls = [img["original"] for img in images_results[:5] if "original" in img]
        log("✅", f"Found {len(image_urls)} images via SerpApi")
        return image_urls

    except Exception as e:
        log("✗", f"SerpApi image search error: {e}")
        return []


def download_image_from_url(url: str, idx: int) -> dict:
    """Download image from URL and return base64 payload for Shopify."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    encoded = base64.b64encode(response.content).decode("ascii")
    ext = url.split(".")[-1].split("?")[0] or "jpg"
    return {
        "attachment": encoded,
        "filename": f"product-image-{idx}.{ext}"
    }


def check_duplicate_product(title: str) -> dict:
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json"
    }
    search_title = quote_plus(title[:50])
    response = requests.get(
        f"https://{SHOPIFY_STORE}/admin/api/2024-01/products.json?title={search_title}&limit=5",
        headers=headers,
        timeout=30
    )
    products = response.json().get("products", [])
    for product in products:
        existing_title = product.get("title", "").lower()
        new_title = title.lower()
        existing_words = set(existing_title.split())
        new_words = set(new_title.split())
        if len(existing_words) > 0:
            match_ratio = len(existing_words & new_words) / len(existing_words | new_words)
            if match_ratio > 0.6:
                return {
                    "duplicate": True,
                    "existing_title": product.get("title"),
                    "existing_id": product.get("id"),
                    "url": f"https://{SHOPIFY_STORE}/admin/products/{product.get('id')}"
                }
    return {"duplicate": False}


def process_buffer(buf: ProductBuffer):
    chat_id = buf.chat_id

    if not buf.caption.strip():
        bot.send_message(chat_id, "⚠️ No caption found. Using default product title.")

    log("🤖", f"Processing product for chat {chat_id}")
    log("📝", f"Caption: {buf.caption[:100]}")

    # Step 1: Extract product details with Groq
    try:
        product_data = extract_product_with_groq(buf.caption)
    except Exception as e:
        log("✗", f"Groq error: {e}")
        bot.send_message(chat_id, f"❌ AI extraction failed: {e}")
        return

    dup_check = check_duplicate_product(product_data.get("title", ""))
    if dup_check["duplicate"]:
        bot.send_message(chat_id,
            f"⚠️ Duplicate detected!\n\n"
            f"Similar product already exists:\n"
            f"📦 {dup_check['existing_title']}\n"
            f"🔗 {dup_check['url']}\n\n"
            f"Creating new listing anyway..."
        )

    # Step 2: Download images
    image_payloads = []

    if buf.photo_file_ids:
        # Use images sent via Telegram
        log("🖼", f"Using {len(buf.photo_file_ids)} images from Telegram")
        for idx, file_id in enumerate(buf.photo_file_ids, 1):
            try:
                log("🖼", f"Downloading image {idx}/{len(buf.photo_file_ids)}")
                file_info = bot.get_file(file_id)
                file_url = f"https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{file_info.file_path}"
                response = requests.get(file_url, timeout=45)
                response.raise_for_status()
                encoded = base64.b64encode(response.content).decode("ascii")
                image_payloads.append({
                    "attachment": encoded,
                    "filename": f"product-image-{idx}.jpg"
                })
            except Exception as e:
                log("✗", f"Image download error: {e}")
    else:
        # No images sent — auto fetch from brand website
        log("🔍", f"No images sent. Auto-fetching from brand website...")
        bot.send_message(chat_id, "🔍 No images sent. Searching brand website automatically...")

        title = product_data.get("title", "")
        image_urls = fetch_brand_images(title)

        if image_urls:
            bot.send_message(chat_id, f"🖼 Found {len(image_urls)} images online. Downloading...")
            for idx, url in enumerate(image_urls, 1):
                try:
                    payload = download_image_from_url(url, idx)
                    image_payloads.append(payload)
                    log("✅", f"Downloaded online image {idx}")
                except Exception as e:
                    log("✗", f"Failed to download online image {idx}: {e}")
        else:
            bot.send_message(chat_id, "⚠️ Could not find images online. Creating listing without images.")

    if not image_payloads:
        bot.send_message(chat_id, "❌ Failed to download images.")
        return

    # Step 3: Create Shopify product
    try:
        result = create_shopify_product(product_data, image_payloads)
        product = result.get("product", {})
        product_id = product.get("id")
        title = product.get("title", "Unknown")
        admin_url = f"https://{SHOPIFY_STORE}/admin/products/{product_id}"

        log("✅", f"Product created: {title} (ID: {product_id})")
        daily_products_added.append({
            "title": title,
            "price": product_data.get("price", 0),
            "id": product_id
        })
        bot.send_message(chat_id,
            f"✅ Product created on Shopify!\n\n"
            f"📦 Title: {title}\n"
            f"💰 MRP: ₹{product_data.get('mrp', 0)}\n"
            f"💸 Selling Price: ₹{product_data.get('price', 0)}\n"
            f"📊 Stock: 100\n"
            f"🖼 Images: {len(image_payloads)}\n\n"
            f"🔗 {admin_url}"
        )
    except Exception as e:
        log("✗", f"Shopify error: {e}")
        bot.send_message(chat_id, f"❌ Shopify error: {e}")


# ─── GROQ AI EXTRACTION ───────────────────────────────────────────────────────

def extract_product_with_groq(caption: str) -> Dict[str, Any]:
    if not caption.strip():
        return {"title": "New Product", "description": "", "price": 0, "quantity": 1, "vendor": "Supplier", "sku": ""}

    prompt = (
        "Extract product details from the message below and return ONLY valid JSON. "
        "No markdown, no code fences, no extra text. "
        "Fields required: title, description, price, mrp, quantity, vendor, sku, sizes. "
        "sizes: array of sizes mentioned (e.g. ['4','5','6','SH','Harrow']). "
        "If no sizes mentioned use ['4','5','6','SH'] as default for bats, "
        "['S','M','L','XL'] for clothing, [] for others. "
        "Rules: "
        "- price = the Selling or Selling Price value (number only) "
        "- mrp = the MRP value (number only) "
        "- quantity = always 100, ignore any number in the message "
        "- description = 'MRP: ₹[mrp value] | Selling Price: ₹[price value]' "
        "- vendor = brand name if mentioned, else Supplier "
        "- sku = product code if mentioned, else empty string "
        "price and mrp must be numbers, quantity must be integer 100.\n\n"
        f"Message:\n{caption}"
    )

    log("🤖", "Sending to Groq AI...")
    response = groq_client.chat.completions.create(
        model="llama-3.1-8b-instant",
        temperature=0,
        messages=[
            {"role": "system", "content": "You are a strict JSON extractor. Return JSON only."},
            {"role": "user", "content": prompt}
        ]
    )

    content = response.choices[0].message.content or ""
    content = content.strip().replace("```json", "").replace("```", "").strip()

    start = content.find("{")
    end = content.rfind("}")
    parsed = json.loads(content[start:end+1])

    for f in ["title", "description", "price", "quantity", "vendor", "sku"]:
        if f not in parsed:
            parsed[f] = "" if f not in ["price", "quantity"] else 0

    log("✅", f"Extracted: {parsed.get('title')} — ₹{parsed.get('price')}")
    return parsed


# ─── SHOPIFY PRODUCT CREATION ─────────────────────────────────────────────────

def create_shopify_product(product_data: Dict, image_payloads: List) -> Dict:
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json"
    }

    try:
        price = float(product_data.get("price", 0))
    except:
        price = 0.0

    try:
        quantity = int(float(product_data.get("quantity", 1)))
    except:
        quantity = 1

    product_payload = {
        "product": {
            "title": str(product_data.get("title", "New Product")),
            "body_html": str(product_data.get("description", "")),
            "vendor": str(product_data.get("vendor", "Supplier")),
            "status": "active",
            "images": image_payloads
        }
    }

    sizes = product_data.get("sizes", [])
    if sizes and len(sizes) > 0:
        variants = []
        for size in sizes:
            variants.append({
                "option1": str(size),
                "price": f"{price:.2f}",
                "compare_at_price": f"{float(product_data.get('mrp', 0)):.2f}",
                "sku": f"{str(product_data.get('sku', ''))}-{size}".strip("-"),
                "inventory_management": "shopify",
                "inventory_quantity": 100
            })
        product_payload["product"]["options"] = [{"name": "Size", "values": sizes}]
        product_payload["product"]["variants"] = variants
    else:
        product_payload["product"]["variants"] = [{
            "price": f"{price:.2f}",
            "compare_at_price": f"{float(product_data.get('mrp', 0)):.2f}",
            "sku": str(product_data.get("sku", "")),
            "inventory_management": "shopify",
            "inventory_quantity": 100
        }]

    response = requests.post(SHOPIFY_PRODUCTS_URL, headers=headers, json=product_payload, timeout=120)
    response.raise_for_status()
    return response.json()


def send_daily_report():
    import schedule

    def report_job():
        if not daily_products_added:
            return
        report = f"📊 Daily Report — {datetime.now().strftime('%d %b %Y')}\n\n"
        report += f"✅ Products added today: {len(daily_products_added)}\n\n"
        for i, p in enumerate(daily_products_added, 1):
            report += f"{i}. {p['title']} — ₹{p['price']}\n"
        for chat_id in list(known_chat_ids):
            try:
                bot.send_message(chat_id, report)
            except:
                pass
        daily_products_added.clear()

    schedule.every().day.at("21:00").do(report_job)
    while True:
        schedule.run_pending()
        time.sleep(60)


# ─── BACKGROUND TIMEOUT CHECKER ───────────────────────────────────────────────

def timeout_checker():
    while True:
        time.sleep(30)
        now = time.time()
        to_flush = []
        with buffers_lock:
            for chat_id, buf in list(product_buffers.items()):
                if buf.photo_file_ids and (now - buf.last_activity) >= GROUP_TIMEOUT_SECONDS:
                    to_flush.append(product_buffers.pop(chat_id))

        for buf in to_flush:
            log("⏱", f"Auto-flush for chat {buf.chat_id}")
            threading.Thread(target=process_buffer, args=(buf,), daemon=True).start()


# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log("✅", f"Telegram Bot starting...")
    log("✅", f"Store: {SHOPIFY_STORE}")
    log("✅", f"Allowed users: {ALLOWED_USERNAMES}")
    log("✅", f"Timeout: {GROUP_TIMEOUT_SECONDS}s")

    # Validate bot token early so startup errors are explicit.
    try:
        me = bot.get_me()
        log("✅", f"Telegram bot authenticated: @{me.username}")
    except ApiTelegramException as e:
        desc = getattr(e, "description", str(e))
        code = getattr(e, "error_code", "unknown")
        log("✗", f"Telegram auth failed ({code}): {desc}")
        raise SystemExit(1)

    # Long polling should not use webhooks; clear any webhook from previous setups.
    try:
        bot.delete_webhook(drop_pending_updates=True)
        log("✅", "Webhook cleared for long polling mode")
    except Exception as e:
        log("⚠", f"Could not clear webhook: {e}")

    threading.Thread(target=timeout_checker, daemon=True).start()
    threading.Thread(target=send_daily_report, daemon=True).start()

    log("✅", "Bot is running. Send messages to Telegram...")
    
    # 24/7 operation with auto-reconnection
    retry_count = 0
    while True:
        try:
            log("✅", "Polling started...")
            bot.infinity_polling(timeout=10, long_polling_timeout=5, skip_pending=True)
            retry_count = 0
        except KeyboardInterrupt:
            log("⏹", "Bot stopped by user")
            break
        except ApiTelegramException as e:
            retry_count += 1
            desc = getattr(e, "description", str(e))
            code = getattr(e, "error_code", "unknown")
            log("✗", f"Telegram API error ({code}): {desc}")

            # 409 usually means another instance is polling this same bot token.
            if str(code) == "409" or "terminated by other getUpdates request" in str(desc):
                log("⚠", "Conflict detected: another bot instance is active. Stop other instance and retry.")

            if retry_count > 50:
                log("✗", "Too many retries, sleeping 60s...")
                time.sleep(60)
                retry_count = 0
            else:
                time.sleep(5)
            log("🔄", f"Reconnecting... (attempt {retry_count})")
        except Exception as e:
            retry_count += 1
            log("✗", f"Connection error: {type(e).__name__}: {e}")
            if retry_count > 50:
                log("✗", f"Too many retries, sleeping 60s...")
                time.sleep(60)
                retry_count = 0
            else:
                time.sleep(5)
            log("🔄", f"Reconnecting... (attempt {retry_count})")
