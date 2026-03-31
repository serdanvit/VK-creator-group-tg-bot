"""
SEO FARM — VK Group Agent
Telegram бот + агент создания групп VK
Работает на Railway, Google Sheets, Groq AI
"""
import os, sys, re, time, random, logging, json, threading, tempfile
from datetime import datetime
import requests

# ══════════════════════════════════════════════════════════════
# КОНФИГ — всё берётся из переменных окружения Railway
# ══════════════════════════════════════════════════════════════
TG_TOKEN      = os.environ.get("TG_TOKEN", "")
TG_CHAT_ID    = os.environ.get("TG_CHAT_ID", "")
VK_TOKEN      = os.environ.get("VK_TOKEN", "")
GROQ_API_KEY  = os.environ.get("GROQ_API_KEY", "")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "")
GOOGLE_CREDS  = os.environ.get("GOOGLE_CREDENTIALS_JSON", "")

# Лимиты
MAX_PER_RUN   = int(os.environ.get("MAX_PER_RUN", "4"))
PAUSE_MIN     = int(os.environ.get("PAUSE_MIN", "120"))
PAUSE_MAX     = int(os.environ.get("PAUSE_MAX", "180"))

# Листы таблицы
SHEET_KEYS    = "Ключи"
SHEET_RESULTS = "Результаты"
SHEET_CONFIG  = "Настройки"
# ══════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout
)
log = logging.getLogger("seofarm")

# Глобальное состояние
_agent_thread  = None
_agent_running = False
_stop_flag     = False
_start_time    = None
_telegraph_token = None
_captcha_answer  = None  # ответ на капчу от пользователя


# ══════════════════════════════════════════════════════════════
# GOOGLE SHEETS
# ══════════════════════════════════════════════════════════════

def get_sheets():
    try:
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build
        creds_data = json.loads(GOOGLE_CREDS)
        creds = Credentials.from_service_account_info(
            creds_data,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        return build("sheets", "v4", credentials=creds).spreadsheets()
    except Exception as e:
        log.error(f"Google Sheets: {e}")
        return None


def sheets_get(sheets, range_: str) -> list:
    try:
        r = sheets.values().get(
            spreadsheetId=SPREADSHEET_ID, range=range_
        ).execute()
        return r.get("values", [])
    except Exception as e:
        log.error(f"sheets_get {range_}: {e}")
        return []


def sheets_update(sheets, range_: str, values: list):
    try:
        sheets.values().update(
            spreadsheetId=SPREADSHEET_ID, range=range_,
            valueInputOption="RAW", body={"values": values}
        ).execute()
    except Exception as e:
        log.error(f"sheets_update {range_}: {e}")


def sheets_append(sheets, range_: str, values: list):
    try:
        sheets.values().append(
            spreadsheetId=SPREADSHEET_ID, range=range_,
            valueInputOption="RAW", insertDataOption="INSERT_ROWS",
            body={"values": values}
        ).execute()
    except Exception as e:
        log.error(f"sheets_append: {e}")


def init_sheet_headers(sheets):
    """Создаёт заголовки если листов нет"""
    sheets_update(sheets, f"{SHEET_KEYS}!A1:H1", [[
        "Ключевое слово", "Регион", "Статус",
        "Ссылка аватарки", "Ссылка обложки",
        "Фото поста 1", "Фото поста 2 (статья)",
        "Дата"
    ]])
    sheets_update(sheets, f"{SHEET_RESULTS}!A1:G1", [[
        "Ключевое слово", "Название группы", "Ссылка VK",
        "URL группы", "Статья", "Дата", "Регион"
    ]])
    sheets_update(sheets, f"{SHEET_CONFIG}!A1:B1", [
        ["Параметр", "Значение"]
    ])


def get_config(sheets) -> dict:
    """Читает настройки из листа Настройки"""
    rows = sheets_get(sheets, f"{SHEET_CONFIG}!A2:B100")
    return {r[0].strip(): r[1].strip() for r in rows if len(r) >= 2}


def read_pending_keywords(sheets) -> list:
    """Читает ключи со статусом пустой или 'ожидает'"""
    rows = sheets_get(sheets, f"{SHEET_KEYS}!A2:H1000")
    result = []
    for i, row in enumerate(rows):
        while len(row) < 8:
            row.append("")
        kw, region, status = row[0].strip(), row[1].strip(), row[2].strip().lower()
        if not kw or status in ("готово", "ошибка", "в работе"):
            continue
        result.append({
            "row":         i + 2,
            "keyword":     kw,
            "region":      region or "Тюмень",
            "avatar_url":  row[3].strip(),
            "cover_url":   row[4].strip(),
            "post1_photo": row[5].strip(),
            "post2_photo": row[6].strip(),
        })
    return result


def reset_stuck(sheets):
    """Сбрасывает 'в работе' → пусто при старте"""
    rows = sheets_get(sheets, f"{SHEET_KEYS}!A2:C1000")
    for i, row in enumerate(rows):
        if len(row) >= 3 and row[2].strip().lower() == "в работе":
            sheets_update(sheets, f"{SHEET_KEYS}!C{i+2}", [[""]])


def set_kw_status(sheets, row: int, status: str):
    sheets_update(sheets, f"{SHEET_KEYS}!C{row}:H{row}", [[
        status, "", "", "", "", datetime.now().strftime("%d.%m.%Y %H:%M")
    ]])


def is_duplicate(sheets, keyword: str) -> bool:
    rows = sheets_get(sheets, f"{SHEET_RESULTS}!A2:A1000")
    return keyword in [r[0] for r in rows if r]


def save_result(sheets, kw: str, name: str, url: str,
                screen: str, article: str, region: str):
    sheets_append(sheets, f"{SHEET_RESULTS}!A2", [[
        kw, name, url, f"https://vk.com/{screen}",
        article, datetime.now().strftime("%d.%m.%Y %H:%M"), region
    ]])


# ══════════════════════════════════════════════════════════════
# VK API
# ══════════════════════════════════════════════════════════════

def vk(method: str, params: dict) -> dict:
    global _captcha_answer, _stop_flag
    params["access_token"] = VK_TOKEN
    params["v"] = "5.131"
    try:
        r = requests.post(
            f"https://api.vk.com/method/{method}",
            data=params, timeout=60
        )
        data = r.json()
        if "error" in data:
            code = data["error"].get("error_code", 0)
            msg  = data["error"].get("error_msg", "")
            if code == 14:
                # Капча
                sid = data["error"].get("captcha_sid", "")
                img = data["error"].get("captcha_img", "")
                answer = handle_captcha(sid, img)
                if answer and answer != "stop":
                    params["captcha_sid"] = sid
                    params["captcha_key"] = answer
                    return vk(method, params)
                _stop_flag = True
                return {"error": data["error"]}
            log.error(f"VK {method} {code}: {msg}")
        return data
    except Exception as e:
        log.error(f"VK {method}: {e}")
        return {"error": str(e)}


def download_photo(url: str) -> str | None:
    """Скачивает фото по URL во временный файл, возвращает путь"""
    if not url or not url.startswith("http"):
        return None
    try:
        r = requests.get(url, timeout=30)
        if r.status_code != 200:
            return None
        ext = ".jpg"
        if "png" in url.lower():
            ext = ".png"
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=ext)
        tmp.write(r.content)
        tmp.close()
        return tmp.name
    except Exception as e:
        log.warning(f"Скачивание фото {url}: {e}")
        return None


def upload_avatar(group_id: int, photo_url: str) -> bool:
    filepath = download_photo(photo_url)
    if not filepath:
        return False
    try:
        r = vk("photos.getOwnerPhotoUploadServer", {"owner_id": f"-{group_id}"})
        if "error" in r:
            return False
        with open(filepath, "rb") as f:
            res = requests.post(
                r["response"]["upload_url"],
                files={"photo": f}, timeout=60
            ).json()
        vk("photos.saveOwnerPhoto", {
            "server": res["server"],
            "photo":  res["photo"],
            "hash":   res["hash"],
        })
        log.info("  Аватарка загружена")
        return True
    except Exception as e:
        log.warning(f"  Аватарка: {e}")
        return False
    finally:
        try: os.unlink(filepath)
        except: pass


def upload_cover(group_id: int, photo_url: str) -> bool:
    filepath = download_photo(photo_url)
    if not filepath:
        return False
    try:
        r = vk("photos.getOwnerCoverPhotoUploadServer", {
            "group_id": group_id,
            "crop_x": 0, "crop_y": 0,
            "crop_x2": 1920, "crop_y2": 768
        })
        if "error" in r:
            return False
        with open(filepath, "rb") as f:
            res = requests.post(
                r["response"]["upload_url"],
                files={"photo": f}, timeout=60
            ).json()
        vk("photos.saveOwnerCoverPhoto", {
            "hash": res["hash"], "photo": res["photo"]
        })
        log.info("  Обложка загружена")
        return True
    except Exception as e:
        log.warning(f"  Обложка: {e}")
        return False
    finally:
        try: os.unlink(filepath)
        except: pass


def upload_wall_photo(group_id: int, photo_url: str) -> str:
    filepath = download_photo(photo_url)
    if not filepath:
        return ""
    try:
        r = vk("photos.getWallUploadServer", {"group_id": group_id})
        if "error" in r:
            return ""
        with open(filepath, "rb") as f:
            res = requests.post(
                r["response"]["upload_url"],
                files={"photo": f}, timeout=60
            ).json()
        saved = vk("photos.saveWallPhoto", {
            "group_id": group_id,
            "photo": res["photo"],
            "server": res["server"],
            "hash": res["hash"],
        })
        if "error" in saved:
            return ""
        ph = saved["response"][0]
        return f"photo{ph['owner_id']}_{ph['id']}"
    except Exception as e:
        log.warning(f"  Фото на стену: {e}")
        return ""
    finally:
        try: os.unlink(filepath)
        except: pass


# ══════════════════════════════════════════════════════════════
# GROQ AI — генерация контента
# ══════════════════════════════════════════════════════════════

def groq_generate(prompt: str, max_tokens: int = 800) -> str:
    if not GROQ_API_KEY:
        return ""
    try:
        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "model": "llama3-70b-8192",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": max_tokens,
                "temperature": 0.8,
            },
            timeout=30
        )
        return r.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        log.warning(f"Groq: {e}")
        return ""


def ai_description(keyword: str, region: str, site: str) -> str:
    prompt = (
        f"Напиши SEO-описание для группы ВКонтакте.\n"
        f"Ключевой запрос: {keyword}\n"
        f"Регион: {region}\n"
        f"Сайт: {site}\n\n"
        f"Требования:\n"
        f"- Первое предложение обязательно содержит '{keyword}' и '{region}'\n"
        f"- 3-4 коротких абзаца\n"
        f"- Преимущества школы: малые классы, без репетиторов, лицензия\n"
        f"- Адрес: г. {region}, ул. Депутатская, д. 91\n"
        f"- В конце ссылка: {site}\n"
        f"- В конце хэштеги: #{keyword.replace(' ','_')} #{region.replace(' ','_')}\n"
        f"- Только текст, без пояснений, без кавычек вокруг текста\n"
        f"- Максимум 600 символов"
    )
    result = groq_generate(prompt, 400)
    if not result:
        kw_tag = keyword.replace(" ", "_")
        reg_tag = region.replace(" ", "_")
        return (
            f"{keyword} в {region} — Частная школа «ЧЕСТНАЯ»\n\n"
            f"▫ Обучение 1-11 класс, подготовка к школе (4-7 лет)\n"
            f"▫ Малокомплектные классы до 12 человек\n"
            f"▫ г. {region}, ул. Депутатская, д. 91\n\n"
            f"🌐 {site}\n\n"
            f"#{kw_tag} #{reg_tag} #частная_школа #честная_школа"
        )
    return result


def ai_post1(keyword: str, region: str, site: str) -> str:
    prompt = (
        f"Напиши пост для группы ВКонтакте.\n"
        f"Ключевой запрос: {keyword}\n"
        f"Регион: {region}\n"
        f"Сайт: {site}\n\n"
        f"Это закреплённый пост — визитная карточка группы.\n"
        f"Структура:\n"
        f"1. Эмоциональный заголовок с '{keyword}' и '{region}'\n"
        f"2. Краткое описание школы (2-3 строки)\n"
        f"3. 4-5 преимуществ со значками ✅\n"
        f"4. Призыв к действию + ссылка {site}\n"
        f"5. Хэштеги\n"
        f"Только текст поста, без пояснений. Максимум 800 символов."
    )
    result = groq_generate(prompt, 500)
    if not result:
        kw_tag = keyword.replace(" ", "_")
        reg_tag = region.replace(" ", "_")
        return (
            f"🎓 {keyword} в {region} | Честная школа\n\n"
            f"Частная школа «ЧЕСТНАЯ» — место где ребёнок учится "
            f"думать, а не зубрить.\n\n"
            f"✅ Классы до 12 человек\n"
            f"✅ Обучение 1-11 класс\n"
            f"✅ Домашние задания в школе\n"
            f"✅ Без репетиторов\n"
            f"✅ Государственная лицензия\n\n"
            f"📍 г. {region}, ул. Депутатская, д. 91\n\n"
            f"Запись и подробности: {site}\n\n"
            f"#{kw_tag} #{reg_tag} #частная_школа #честная_школа"
        )
    return result


def ai_article(keyword: str, region: str, site: str) -> str:
    prompt = (
        f"Напиши статью-рейтинг для группы ВКонтакте.\n"
        f"Ключевой запрос: {keyword}\n"
        f"Регион: {region}\n\n"
        f"Структура:\n"
        f"1. Заголовок ТОП-10 школ {region} для тех кто ищет '{keyword}'\n"
        f"2. Вступление 2-3 предложения про важность выбора школы\n"
        f"3. Список ТОП-10 (первое место — Частная школа ЧЕСТНАЯ — лидер)\n"
        f"   Остальные: Могу и буду, Росток, Прайм, Рассвет, Свобода, "
        f"Клевер, IT-лицей, Вектор, Discovery\n"
        f"4. Цитата эксперта Анастасии Клеймёновой про школу ЧЕСТНАЯ\n"
        f"5. Ссылка {site}\n\n"
        f"Тон: экспертный, убедительный. Только текст статьи. "
        f"Максимум 1500 символов."
    )
    result = groq_generate(prompt, 900)
    if not result:
        kw_tag = keyword.replace(" ", "_")
        reg_tag = region.replace(" ", "_")
        kl = keyword.lower()
        return (
            f"📚 ТОП-10 школ {region} для тех кто ищет «{keyword}»\n\n"
            f"Выбор школы — одно из важнейших решений. Мы составили "
            f"честный рейтинг для тех кто ищет {kl} в {region}.\n\n"
            f"🥇 1. Частная школа «ЧЕСТНАЯ» — Лидер\n"
            f"Фундаментальные знания + альтернативный подход. "
            f"Обучение через проекты и исследования.\n\n"
            f"2. Школа «Могу и буду»\n3. Монтессори-центр «Росток»\n"
            f"4. Семейные классы «Прайм»\n5. Вальдорфская «Рассвет»\n"
            f"6. Демшкола «Свобода»\n7. Проект «Клевер»\n"
            f"8. IT-лицей\n9. Школа «Вектор»\n10. Клуб «Discovery»\n\n"
            f"💬 Эксперт Анастасия Клеймёнова:\n"
            f"«Школа ЧЕСТНАЯ лидирует — ребёнок получит знания "
            f"и сохранит страсть к познанию.»\n\n"
            f"🔗 {site}\n\n"
            f"#{kw_tag} #{reg_tag} #школа_{reg_tag}"
        )
    return result


# ══════════════════════════════════════════════════════════════
# ТРАНСЛИТЕРАЦИЯ
# ══════════════════════════════════════════════════════════════

def translit(text: str) -> str:
    t = {
        "а":"a","б":"b","в":"v","г":"g","д":"d","е":"e","ё":"yo",
        "ж":"zh","з":"z","и":"i","й":"y","к":"k","л":"l","м":"m",
        "н":"n","о":"o","п":"p","р":"r","с":"s","т":"t","у":"u",
        "ф":"f","х":"kh","ц":"ts","ч":"ch","ш":"sh","щ":"shch",
        "ъ":"","ы":"y","ь":"","э":"e","ю":"yu","я":"ya",
        " ":"_","-":"_",",":"","(":"",")":" ","«":"","»":"","—":"_",
    }
    res = "".join(t.get(c.lower(), c) for c in text)
    res = re.sub(r"[^a-z0-9_]", "", res)
    res = re.sub(r"_+", "_", res).strip("_")
    return res[:50]


# ══════════════════════════════════════════════════════════════
# TELEGRA.PH
# ══════════════════════════════════════════════════════════════

def get_telegraph_token() -> str:
    global _telegraph_token
    if _telegraph_token:
        return _telegraph_token
    try:
        r = requests.post("https://api.telegra.ph/createAccount", json={
            "short_name": "SEOFarm", "author_name": "SEO FARM"
        }, timeout=15).json()
        if r.get("ok"):
            _telegraph_token = r["result"]["access_token"]
    except Exception as e:
        log.error(f"Telegraph токен: {e}")
    return _telegraph_token or ""


def publish_telegraph(title: str, text: str) -> str:
    token = get_telegraph_token()
    if not token:
        return ""
    try:
        paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
        content = [{"tag": "p", "children": [p]} for p in paragraphs]
        r = requests.post("https://api.telegra.ph/createPage", json={
            "access_token": token,
            "title": title[:256],
            "content": content,
            "return_content": False,
        }, timeout=30).json()
        if r.get("ok"):
            return r["result"]["url"]
    except Exception as e:
        log.warning(f"Telegraph: {e}")
    return ""


# ══════════════════════════════════════════════════════════════
# СОЗДАНИЕ ГРУППЫ — полный pipeline
# ══════════════════════════════════════════════════════════════

def create_group(kw_data: dict, site: str) -> dict:
    keyword   = kw_data["keyword"]
    region    = kw_data["region"]
    av_url    = kw_data["avatar_url"]
    cv_url    = kw_data["cover_url"]
    p1_url    = kw_data["post1_photo"]
    p2_url    = kw_data["post2_photo"]

    name = f"{keyword} | {region}"[:48]
    desc = ai_description(keyword, region, site)

    log.info(f"  Создаю: {name}")

    # 1. Создаём группу
    r = vk("groups.create", {
        "title": name, "description": desc,
        "type": "group", "subtype": 1,
    })
    if "error" in r:
        return {"success": False, "error": r["error"]}

    gid = r["response"]["id"]
    time.sleep(random.uniform(2, 4))

    # 2. Сайт в профиле
    vk("groups.edit", {"group_id": gid, "website": site, "description": desc})
    time.sleep(random.uniform(1, 2))

    # 3. Аватарка из ссылки
    if av_url:
        upload_avatar(gid, av_url)
        time.sleep(random.uniform(1, 2))

    # 4. Обложка из ссылки
    if cv_url:
        upload_cover(gid, cv_url)
        time.sleep(random.uniform(1, 2))

    # 5. Пост 1 — описание школы + фото + закреп
    post1_text = ai_post1(keyword, region, site)
    att1 = upload_wall_photo(gid, p1_url) if p1_url else ""
    params1 = {"owner_id": f"-{gid}", "message": post1_text, "from_group": 1}
    if att1:
        params1["attachments"] = att1
    r1 = vk("wall.post", params1)
    if "response" in r1:
        pid = r1["response"]["post_id"]
        time.sleep(1)
        vk("wall.pin", {"owner_id": f"-{gid}", "post_id": pid})
    log.info("  Пост 1 опубликован и закреплён")
    time.sleep(random.uniform(2, 3))

    # 6. Статья Telegra.ph + пост со ссылкой
    article_text  = ai_article(keyword, region, site)
    article_title = f"ТОП-10 школ {region} — {keyword}"
    article_url   = publish_telegraph(article_title, article_text)

    kw_tag  = keyword.replace(" ", "_")
    reg_tag = region.replace(" ", "_")

    if article_url:
        post2_text = (
            f"Как выбрать лучшую школу в {region}? 🎓\n\n"
            f"Составили честный рейтинг для тех кто ищет {keyword.lower()}. "
            f"Внутри — реальные критерии и мнение эксперта.\n\n"
            f"Читать → {article_url}\n\n"
            f"#{kw_tag} #{reg_tag} #школа_{reg_tag}"
        )
    else:
        post2_text = article_text

    att2 = upload_wall_photo(gid, p2_url) if p2_url else ""
    params2 = {"owner_id": f"-{gid}", "message": post2_text, "from_group": 1}
    if att2:
        params2["attachments"] = att2
    vk("wall.post", params2)
    log.info(f"  Пост 2 опубликован{'  статья: ' + article_url if article_url else ''}")
    time.sleep(random.uniform(2, 3))

    # 7. URL через groups.edit screen_name
    screen_name = translit(keyword) + f"_{random.randint(10, 99)}"
    r_sn = vk("groups.edit", {"group_id": gid, "screen_name": screen_name})
    if "error" in r_sn:
        screen_name = f"club{gid}"
        log.warning(f"  URL не установлен — {screen_name}")
    else:
        log.info(f"  URL: vk.com/{screen_name}")

    return {
        "success":    True,
        "group_id":   gid,
        "group_url":  f"https://vk.com/club{gid}",
        "screen_url": f"https://vk.com/{screen_name}",
        "screen_name": screen_name,
        "name":       name,
        "article_url": article_url,
    }


# ══════════════════════════════════════════════════════════════
# TELEGRAM
# ══════════════════════════════════════════════════════════════

def tg_api(method: str, params: dict) -> dict:
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/{method}",
            json=params, timeout=10
        )
        return r.json()
    except Exception as e:
        log.error(f"TG {method}: {e}")
        return {}


def tg_send(text: str, buttons: list = None):
    params = {
        "chat_id": TG_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
    }
    if buttons:
        params["reply_markup"] = {"inline_keyboard": [
            [{"text": b[0], "callback_data": b[1]} for b in row]
            for row in buttons
        ]}
    tg_api("sendMessage", params)


def tg_edit(chat_id, msg_id, text, buttons=None):
    params = {
        "chat_id": chat_id, "message_id": msg_id,
        "text": text, "parse_mode": "HTML",
    }
    if buttons:
        params["reply_markup"] = {"inline_keyboard": [
            [{"text": b[0], "callback_data": b[1]} for b in row]
            for row in buttons
        ]}
    tg_api("editMessageText", params)


def tg_answer(cb_id: str):
    tg_api("answerCallbackQuery", {"callback_query_id": cb_id})


# ══════════════════════════════════════════════════════════════
# КАПЧА
# ══════════════════════════════════════════════════════════════

def handle_captcha(sid: str, img_url: str) -> str:
    global _stop_flag, _captcha_answer
    _captcha_answer = None

    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendPhoto",
            json={"chat_id": TG_CHAT_ID, "photo": img_url},
            timeout=10
        )
    except Exception:
        pass

    tg_send(
        "🤖 <b>VK запрашивает капчу!</b>\n\n"
        "Введи текст с картинки:\n"
        "<code>/captcha ТЕКСТ</code>\n\n"
        "Или перенеси на завтра:\n"
        "<code>/tomorrow</code>\n\n"
        "Жду 10 минут..."
    )

    deadline = time.time() + 600
    while time.time() < deadline:
        if _captcha_answer is not None:
            answer = _captcha_answer
            _captcha_answer = None
            return answer
        time.sleep(3)

    _stop_flag = True
    tg_send("⏰ Нет ответа — останавливаю. Запусти завтра.")
    return "stop"


# ══════════════════════════════════════════════════════════════
# АГЕНТ — главный цикл
# ══════════════════════════════════════════════════════════════

def run_agent():
    global _agent_running, _stop_flag, _start_time

    _agent_running = True
    _stop_flag     = False
    _start_time    = datetime.now()

    log.info("Агент запущен")
    tg_send("🚀 <b>Агент запущен!</b>\nЧитаю задачи из Google Sheets...")

    sheets = get_sheets()
    if not sheets:
        tg_send("❌ Не удалось подключиться к Google Sheets.\nПроверь GOOGLE_CREDENTIALS_JSON.")
        _agent_running = False
        return

    try:
        init_sheet_headers(sheets)
    except Exception:
        pass

    reset_stuck(sheets)

    config  = get_config(sheets)
    site    = config.get("site_url", os.environ.get("SITE_URL", ""))

    if not site:
        tg_send(
            "❌ Не указан сайт!\n\n"
            "Добавь в лист «Настройки»:\n"
            "A2: site_url\nB2: https://твой-сайт.ru"
        )
        _agent_running = False
        return

    keywords = read_pending_keywords(sheets)
    if not keywords:
        tg_send(
            "📋 Нет задач.\n\n"
            "Добавь ключевые слова в лист «Ключи»:\n"
            "• Колонка A — ключевое слово\n"
            "• Колонка B — регион\n"
            "• Колонки D-G — ссылки на фото (необязательно)"
        )
        _agent_running = False
        return

    tg_send(f"📋 Найдено ключей: <b>{len(keywords)}</b>\nСайт: {site}")

    total_ok = total_err = total_skip = 0

    for kw_data in keywords:
        if _stop_flag:
            break
        if total_ok >= MAX_PER_RUN:
            tg_send(
                f"⏸ Лимит {MAX_PER_RUN} групп достигнут.\n"
                f"Запусти агента завтра для продолжения.",
                buttons=[[("▶️ Запустить снова", "start")]]
            )
            break

        keyword = kw_data["keyword"]
        log.info(f"\n=== {keyword} ===")

        if is_duplicate(sheets, keyword):
            log.info("  Дубль — пропускаем")
            total_skip += 1
            continue

        set_kw_status(sheets, kw_data["row"], "в работе")
        res = create_group(kw_data, site)

        if res["success"]:
            total_ok += 1
            save_result(
                sheets, keyword, res["name"],
                res["group_url"], res["screen_name"],
                res.get("article_url", ""), kw_data["region"]
            )
            set_kw_status(sheets, kw_data["row"], "готово")
            tg_send(
                f"✅ <b>Группа создана!</b>\n\n"
                f"🔑 {keyword}\n"
                f"📛 {res['name']}\n"
                f"🌐 vk.com/{res['screen_name']}\n"
                f"🔗 {res['group_url']}"
            )
            if not _stop_flag:
                pause = random.randint(PAUSE_MIN, PAUSE_MAX)
                log.info(f"Пауза {pause}с...")
                time.sleep(pause)
        else:
            total_err += 1
            set_kw_status(sheets, kw_data["row"], "ошибка")
            tg_send(f"❌ Ошибка: {keyword}\n{res.get('error','?')}")
            time.sleep(15)

    tg_send(
        f"🏁 <b>Агент завершил работу</b>\n\n"
        f"✅ Создано: <b>{total_ok}</b>\n"
        f"⏩ Пропущено (дубли): <b>{total_skip}</b>\n"
        f"❌ Ошибок: <b>{total_err}</b>\n\n"
        f"📊 Результаты в Google Sheets → лист «Результаты»",
        buttons=[[("▶️ Запустить снова", "start"), ("📊 Статус", "status")]]
    )

    _agent_running = False
    log.info("Агент завершил работу")


# ══════════════════════════════════════════════════════════════
# TELEGRAM БОТ — обработка сообщений и кнопок
# ══════════════════════════════════════════════════════════════

def show_menu(text="👋 Управление SEO FARM агентом"):
    status = "🟢 Работает" if _agent_running else "🔴 Остановлен"
    tg_send(
        f"{text}\n\nСтатус: <b>{status}</b>",
        buttons=[
            [("▶️ Запустить агента", "start"), ("⏹ Остановить", "stop")],
            [("📊 Статус", "status"), ("📋 Логи", "logs")],
            [("❓ Помощь", "help")],
        ]
    )


def handle_callback(update: dict):
    global _agent_thread, _stop_flag, _captcha_answer

    cb     = update["callback_query"]
    cb_id  = cb["id"]
    data   = cb.get("data", "")
    chat   = cb["message"]["chat"]["id"]
    msg_id = cb["message"]["message_id"]

    tg_answer(cb_id)

    if data == "start":
        if _agent_running:
            tg_edit(chat, msg_id,
                "⚠️ Агент уже запущен!",
                buttons=[[("📊 Статус", "status"), ("⏹ Стоп", "stop")]]
            )
        else:
            _agent_thread = threading.Thread(target=run_agent, daemon=True)
            _agent_thread.start()
            tg_edit(chat, msg_id,
                "✅ <b>Агент запущен!</b>\nОтчёты будут приходить сюда.",
                buttons=[[("⏹ Остановить", "stop"), ("📊 Статус", "status")]]
            )

    elif data == "stop":
        if not _agent_running:
            tg_edit(chat, msg_id,
                "⚠️ Агент не запущен.",
                buttons=[[("▶️ Запустить", "start"), ("🏠 Меню", "menu")]]
            )
        else:
            _stop_flag = True
            tg_edit(chat, msg_id,
                "⏹ <b>Агент остановится после текущей группы.</b>",
                buttons=[[("▶️ Запустить снова", "start"), ("🏠 Меню", "menu")]]
            )

    elif data == "status":
        if _agent_running and _start_time:
            elapsed = int((datetime.now() - _start_time).total_seconds())
            m, s = elapsed // 60, elapsed % 60
            status_text = f"🟢 <b>Агент работает</b>\n⏱ {m}м {s}с"
        else:
            status_text = "🔴 <b>Агент остановлен</b>"
        tg_edit(chat, msg_id,
            f"📊 <b>Статус</b>\n\n{status_text}",
            buttons=[
                [("🔄 Обновить", "status"), ("📋 Логи", "logs")],
                [("🏠 Меню", "menu")]
            ]
        )

    elif data == "logs":
        # Последние записи из Google Sheets результатов
        tg_edit(chat, msg_id,
            "📋 Логи отображаются в консоли Railway.\n"
            "Результаты создания групп — в Google Sheets → лист «Результаты»",
            buttons=[[("📊 Статус", "status"), ("🏠 Меню", "menu")]]
        )

    elif data == "help":
        tg_edit(chat, msg_id,
            "❓ <b>Помощь</b>\n\n"
            "1️⃣ Добавь ключи в Google Sheets → лист «Ключи»\n"
            "2️⃣ Нажми «Запустить агента»\n"
            "3️⃣ Агент создаст группы VK автоматически\n"
            "4️⃣ Результаты — в листе «Результаты»\n\n"
            "При капче бот пришлёт картинку — ответь:\n"
            "<code>/captcha ТЕКСТ</code> или <code>/tomorrow</code>",
            buttons=[[("🏠 Главное меню", "menu")]]
        )

    elif data == "menu":
        show_menu()


def handle_message(update: dict):
    global _captcha_answer, _stop_flag

    msg     = update.get("message", {})
    text    = msg.get("text", "").strip()
    chat_id = str(msg.get("chat", {}).get("id", ""))

    if chat_id != str(TG_CHAT_ID):
        return

    if text in ("/start", "/menu"):
        show_menu("👋 SEO FARM — агент для создания групп VK")

    elif text.lower().startswith("/captcha "):
        answer = text[9:].strip()
        _captcha_answer = answer
        tg_send(f"✅ Принято: <code>{answer}</code>. Продолжаю...")

    elif text.lower() == "/tomorrow":
        _captcha_answer = "stop"
        _stop_flag = True
        tg_send(
            "⏸ <b>Останавливаю.</b>\n"
            "Незавершённые ключи остались в таблице.\n"
            "Запусти агента завтра — продолжит с того места."
        )


# ══════════════════════════════════════════════════════════════
# ЗАПУСК БОТА
# ══════════════════════════════════════════════════════════════

def run_bot():
    log.info("Бот запущен")

    if not TG_TOKEN:
        log.error("TG_TOKEN не задан!")
        return
    if not TG_CHAT_ID:
        log.error("TG_CHAT_ID не задан!")
        return

    show_menu("🚀 <b>SEO FARM запущен!</b>")

    offset = None
    while True:
        try:
            params = {
                "timeout": 25,
                "allowed_updates": ["message", "callback_query"]
            }
            if offset:
                params["offset"] = offset
            r = tg_api("getUpdates", params)
            for upd in r.get("result", []):
                offset = upd["update_id"] + 1
                if "callback_query" in upd:
                    handle_callback(upd)
                elif "message" in upd:
                    handle_message(upd)
        except KeyboardInterrupt:
            break
        except Exception as e:
            if "timed out" not in str(e).lower():
                log.error(f"Polling: {e}")
            time.sleep(2)


if __name__ == "__main__":
    run_bot()
