"""
pip install pytelegrambotapi requests groq schedule python-dotenv
"""

import base64
import io
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
from PIL import Image
from rembg import remove

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
daily_report_lock = threading.Lock()  # Protect shared state access

# ─── CONFIG (Load from Environment Variables) ─────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ALLOWED_USERNAMES = os.getenv("ALLOWED_USERNAMES", "Aarav5005,Asportszone,asportszone22").split(",")
ALLOWED_USERNAMES = [u.strip() for u in ALLOWED_USERNAMES]

SHOPIFY_STORE = os.getenv("SHOPIFY_STORE")
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN")

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
TARGET_PUBLICATIONS = [
    p.strip() for p in os.getenv("TARGET_PUBLICATIONS", "Online Store,Asports Zone Headless,Asports Zone Headless 03").split(",") if p.strip()
]

GROUP_TIMEOUT_SECONDS = int(os.getenv("GROUP_TIMEOUT_SECONDS", "180"))

# Validate required variables
if not all([TELEGRAM_BOT_TOKEN, SHOPIFY_STORE, SHOPIFY_ACCESS_TOKEN, GROQ_API_KEY]):
    print("ERROR: Missing required environment variables!")
    print("Please set: TELEGRAM_BOT_TOKEN, SHOPIFY_STORE, SHOPIFY_ACCESS_TOKEN, GROQ_API_KEY")
    exit(1)
# ───────────────────────────────────────────────────────────────────────────────

bot = Bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)
groq_client = Groq(api_key=GROQ_API_KEY)

SHOPIFY_PRODUCTS_URL = f"https://{SHOPIFY_STORE}/admin/api/2024-01/products.json"
SHOPIFY_GRAPHQL_URL = f"https://{SHOPIFY_STORE}/admin/api/2024-01/graphql.json"

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

def process_uploaded_image(image_bytes: bytes, idx: int) -> dict:
    """Remove background, center subject, and return Shopify image payload."""
    if not image_bytes:
        raise ValueError("Empty image bytes")

    try:
        no_bg_bytes = remove(image_bytes)
    except Exception as e:
        log("⚠", f"Background removal failed for image {idx}, using original: {e}")
        no_bg_bytes = image_bytes

    with Image.open(io.BytesIO(no_bg_bytes)) as src:
        rgba = src.convert("RGBA")
        bbox = rgba.getbbox()
        if bbox:
            rgba = rgba.crop(bbox)

        w, h = rgba.size
        side = int(max(w, h) * 1.2)
        side = max(side, 1200)

        canvas = Image.new("RGBA", (side, side), (255, 255, 255, 0))
        x = (side - w) // 2
        y = (side - h) // 2
        canvas.paste(rgba, (x, y), rgba)

        out = io.BytesIO()
        canvas.save(out, format="PNG", optimize=True)

    encoded = base64.b64encode(out.getvalue()).decode("ascii")
    return {
        "attachment": encoded,
        "filename": f"product-image-{idx}.png"
    }


def get_publications() -> List[Dict[str, str]]:
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json"
    }
    query = """
    query GetPublications {
      publications(first: 50) {
        edges {
          node {
            id
            name
          }
        }
      }
    }
    """
    response = requests.post(
        SHOPIFY_GRAPHQL_URL,
        headers=headers,
        json={"query": query},
        timeout=60
    )
    response.raise_for_status()
    body = response.json()

    if body.get("errors"):
        raise RuntimeError(f"GraphQL publication query failed: {body['errors']}")

    edges = body.get("data", {}).get("publications", {}).get("edges", [])
    return [edge.get("node", {}) for edge in edges if edge.get("node")]


def publish_product_to_channels(product_id: int) -> List[str]:
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json"
    }

    publications = get_publications()
    selected_publications = []

    # Match publications by exact name (case-insensitive)
    target_names = {name.lower(): name for name in TARGET_PUBLICATIONS}
    
    for pub in publications:
        pub_name = str(pub.get("name", "")).lower()
        if pub_name in target_names:
            selected_publications.append(pub)

    if not selected_publications:
        log("⚠", f"No matching publications found. Target channels: {TARGET_PUBLICATIONS}")
        return []

    mutation = """
    mutation PublishToChannel($id: ID!, $input: [PublicationInput!]!) {
      publishablePublish(id: $id, input: $input) {
        userErrors {
          field
          message
        }
      }
    }
    """

    publishable_id = f"gid://shopify/Product/{product_id}"
    publication_input = [{"publicationId": pub["id"]} for pub in selected_publications if pub.get("id")]

    response = requests.post(
        SHOPIFY_GRAPHQL_URL,
        headers=headers,
        json={
            "query": mutation,
            "variables": {
                "id": publishable_id,
                "input": publication_input
            }
        },
        timeout=60
    )
    response.raise_for_status()
    body = response.json()

    if body.get("errors"):
        raise RuntimeError(f"GraphQL publish failed: {body['errors']}")

    user_errors = body.get("data", {}).get("publishablePublish", {}).get("userErrors", [])
    if user_errors:
        raise RuntimeError(f"Publish userErrors: {user_errors}")

    return [str(pub.get("name", "")) for pub in selected_publications]


def check_duplicate_product(title: str) -> dict:
    headers = {
        "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
        "Content-Type": "application/json"
    }
    search_title = quote_plus(title[:50])
    try:
        response = requests.get(
            f"https://{SHOPIFY_STORE}/admin/api/2024-01/products.json?title={search_title}&limit=5",
            headers=headers,
            timeout=30
        )
        response.raise_for_status()
        products = response.json().get("products", [])
    except Exception as e:
        log("⚠", f"Duplicate check failed (continuing anyway): {e}")
        return {"duplicate": False}
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
        product_data["raw_caption"] = buf.caption
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

    if not buf.photo_file_ids:
        bot.send_message(chat_id, "❌ No images uploaded. Please send product images, then use /flush.")
        return

    log("🖼", f"Using {len(buf.photo_file_ids)} images from Telegram")
    for idx, file_id in enumerate(buf.photo_file_ids, 1):
        try:
            log("🖼", f"Downloading image {idx}/{len(buf.photo_file_ids)}")
            file_info = bot.get_file(file_id)
            file_url = f"https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{file_info.file_path}"
            response = requests.get(file_url, timeout=45)
            response.raise_for_status()
            payload = process_uploaded_image(response.content, idx)
            image_payloads.append(payload)
            log("✅", f"Image {idx} processed (background removed + centered)")
        except Exception as e:
            log("✗", f"Image processing error for #{idx}: {e}")

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

        published_channels = []
        if product_id:
            try:
                published_channels = publish_product_to_channels(int(product_id))
                if published_channels:
                    log("✅", f"Published to channels: {', '.join(published_channels)}")
            except Exception as publish_error:
                log("⚠", f"Product created but publish step failed: {publish_error}")

        log("✅", f"Product created: {title} (ID: {product_id})")
        with daily_report_lock:
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
            f"📡 Published To: {', '.join(published_channels) if published_channels else 'Default channel only'}\n\n"
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
        "User may only send title + MRP + selling price, so infer missing non-critical fields safely. "
        "sizes: array of sizes mentioned (e.g. ['4','5','6','SH','Harrow']). "
        "If no sizes mentioned use ['4','5','6','SH'] as default for bats, "
        "['S','M','L','XL'] for clothing, [] for others. "
        "Rules: "
        "- price = the Selling or Selling Price value (number only) "
        "- mrp = the MRP value (number only) "
        "- quantity = always 100, ignore any number in the message "
        "- description must be 3-5 short sales lines in plain text for Shopify, "
        "  including product highlights and use case, then a final price line like "
        "  'MRP: ₹[mrp] | Selling Price: ₹[price]' "
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

    for f in ["title", "description", "price", "mrp", "quantity", "vendor", "sku", "sizes"]:
        if f not in parsed:
            if f in ["price", "mrp", "quantity"]:
                parsed[f] = 0
            elif f == "sizes":
                parsed[f] = []
            else:
                parsed[f] = ""

    # Safety fallback: ensure there is always a useful auto-generated description.
    description_text = str(parsed.get("description", "")).strip()
    if len(description_text) < 25:
        title = str(parsed.get("title", "This product")).strip() or "This product"
        vendor = str(parsed.get("vendor", "Supplier")).strip() or "Supplier"
        mrp = int(float(parsed.get("mrp", 0) or 0))
        price = int(float(parsed.get("price", 0) or 0))
        parsed["description"] = (
            f"{title} by {vendor} is built for reliable daily performance.\n"
            f"Designed for comfort, control, and long-lasting use.\n"
            f"A strong choice for training sessions and match play.\n"
            f"MRP: ₹{mrp} | Selling Price: ₹{price}"
        )

    log("✅", f"Extracted: {parsed.get('title')} — ₹{parsed.get('price')}")
    return parsed


def normalize_sizes(raw_sizes: Any, title: str, caption: str) -> List[str]:
    """Normalize size values from AI/user text to clean Shopify variant values."""
    normalized: List[str] = []

    if isinstance(raw_sizes, list):
        candidates = [str(s).strip() for s in raw_sizes if str(s).strip()]
    elif isinstance(raw_sizes, str):
        candidates = [s.strip() for s in re.split(r"[,/|]", raw_sizes) if s.strip()]
    else:
        candidates = []

    text = f"{title} {caption}".lower()

    # Strong signal: explicit user input like "size 7" should take priority.
    explicit_sizes_from_text = re.findall(r"\bsize\s*([0-9]{1,2})\b", text)
    explicit_named_sizes = []
    if re.search(r"\bfull\s*size\b|\bshort\s*handle\b|\bsh\b", text):
        explicit_named_sizes.append("SH")
    if re.search(r"\bharrow\b", text):
        explicit_named_sizes.append("Harrow")
    if explicit_sizes_from_text or explicit_named_sizes:
        candidates = [*explicit_sizes_from_text, *explicit_named_sizes]

    # Common cricket bat size aliases.
    alias_map = {
        "full size": "SH",
        "full": "SH",
        "short handle": "SH",
        "short-handle": "SH",
        "size 7": "7",
        "size 6": "6",
        "size 5": "5",
        "size 4": "4",
        "harrow": "Harrow",
    }

    for c in candidates:
        key = c.lower().strip()
        normalized.append(alias_map.get(key, c.strip()))

    # Fallback detect from caption/title when model misses sizes.
    if not normalized:
        if re.search(r"\bsize\s*7\b", text):
            normalized.append("7")
        if re.search(r"\bfull\s*size\b|\bshort\s*handle\b|\bsh\b", text):
            normalized.append("SH")
        if re.search(r"\bharrow\b", text):
            normalized.append("Harrow")
        if re.search(r"\bsize\s*6\b|\b6\b", text):
            normalized.append("6")
        if re.search(r"\bsize\s*5\b|\b5\b", text):
            normalized.append("5")
        if re.search(r"\bsize\s*4\b|\b4\b", text):
            normalized.append("4")

    # De-duplicate while preserving order.
    deduped: List[str] = []
    seen = set()
    for size in normalized:
        size_clean = str(size).strip()
        if not size_clean:
            continue
        size_key = size_clean.lower()
        if size_key in seen:
            continue
        seen.add(size_key)
        deduped.append(size_clean)

    # Keep existing defaults if still nothing found.
    if deduped:
        return deduped

    if any(k in text for k in ["bat", "cricket"]):
        return ["4", "5", "6", "SH"]
    if any(k in text for k in ["tshirt", "t-shirt", "jersey", "hoodie", "track pant", "clothing"]):
        return ["S", "M", "L", "XL"]
    return []


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

    sizes = normalize_sizes(
        product_data.get("sizes", []),
        str(product_data.get("title", "")),
        str(product_data.get("raw_caption", ""))
    )
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
        with daily_report_lock:
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

    # Pre-download rembg model to cache it (avoids timeout on first image processing)
    try:
        log("📦", "Pre-downloading rembg AI model (this may take 1-2 min on first run)...")
        from rembg import new_session
        session = new_session()
        log("✅", "rembg model cached successfully")
    except Exception as e:
        log("⚠", f"rembg pre-download warning (will retry on first use): {e}")

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
