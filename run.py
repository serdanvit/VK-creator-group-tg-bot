"""
run.py — SEO FARM без интерфейса
Читает задачи из Google Sheets → создаёт группы VK → пишет результат обратно → отчёт в Telegram
"""
import os, sys, re, time, random, logging, json
from datetime import datetime

# ══════════════════════════════════════════════════════════════
# НАСТРОЙКИ — заполни этот блок
# ══════════════════════════════════════════════════════════════
SPREADSHEET_ID     = "1jgHfdox5z6IAgHUXqFl36yQaiqlS_tFR31-O19RkD34"
SHEET_TASKS        = "Задачи"
SHEET_RESULTS      = "Результаты"
CREDENTIALS_FILE   = "credentials.json"

TELEGRAM_TOKEN     = "8009097004:AAFF28Ef_QiiTCoAn4Koe-RXtoxXrElAusM"
TELEGRAM_CHAT_ID   = "8002970207"

VK_TOKEN           = "vk1.a.1GV8ghj7aVmvK39gTVt9swGFEkX2LdPSf7l253aeTd5AmXyWg_ycJsX8PzFnLefjH5FNJ9xyNndslSXtK2dbVeYtPuMYswAfIKq1HaCvjY7nvbU2dFP-83Nzil7yrKyBBcqRS0aSQdJ1MzxepeYpCw0w2DMTNdQZEjQ8dqRcswXw99pCN0vokYbTxgmVeZKb1PuupMDGH6pFdXwVz_qXww"

PAUSE_MIN          = 120
PAUSE_MAX          = 180
MAX_GROUPS_PER_RUN = 4   # максимум групп за запуск

AVATARS_DIR        = "avatars"    # аватарки групп (квадрат JPG/PNG)
COVERS_DIR         = "covers"    # обложки групп (1920x768 JPG/PNG)
POST1_PHOTOS_DIR    = "post1photos"  # фото к посту 1
POST2_PHOTOS_DIR    = "post2photos"  # фото к статье ТОП-10
# ══════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("run.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger("seofarm")


# ── Транслитерация ───────────────────────────────────────────
def translit(text: str) -> str:
    table = {
        "а":"a","б":"b","в":"v","г":"g","д":"d","е":"e","ё":"yo",
        "ж":"zh","з":"z","и":"i","й":"y","к":"k","л":"l","м":"m",
        "н":"n","о":"o","п":"p","р":"r","с":"s","т":"t","у":"u",
        "ф":"f","х":"kh","ц":"ts","ч":"ch","ш":"sh","щ":"shch",
        "ъ":"","ы":"y","ь":"","э":"e","ю":"yu","я":"ya",
        " ":"_","-":"_",",":"","(":"",")":" ","«":"","»":"","—":"_",
    }
    res = "".join(table.get(c.lower(), c) for c in text)
    res = re.sub(r"[^a-z0-9_]", "", res)
    res = re.sub(r"_+", "_", res).strip("_")
    return res[:50]


# ── Случайный файл из папки ──────────────────────────────────
def random_file(folder: str):
    if not os.path.isdir(folder):
        return None
    files = [
        os.path.join(folder, f) for f in os.listdir(folder)
        if f.lower().endswith((".jpg", ".jpeg", ".png"))
    ]
    return random.choice(files) if files else None


# ── Контент — посты и описание ───────────────────────────────

def make_group_name(keyword: str, region: str) -> str:
    """Название = ключ + регион, до 48 символов"""
    name = f"{keyword} | {region}"
    if len(name) > 48:
        name = f"{keyword[:44]} {region[:3]}"
    return name[:48]


def make_description(keyword: str, region: str, site: str) -> str:
    """SEO-описание под конкретный ключ"""
    kw_tag  = keyword.replace(" ", "_")
    reg_tag = region.replace(" ", "_")
    return (
        f"{keyword} в {region} — Честная школа\n\n"
        f"▫ Обучение 1-11 класс, подготовка к школе (4-7 лет)\n"
        f"▫ Малокомплектные классы, индивидуальный подход\n"
        f"▫ Город {region}, ул. Депутатская, д. 91\n\n"
        f"Записывайтесь на экскурсию и бесплатную пробную неделю!\n\n"
        f"🌐 {site}\n\n"
        f"#{kw_tag} #{reg_tag} #частная_школа #честная_школа"
    )


def make_post1(keyword: str, region: str, site: str) -> str:
    """Пост 1 — описание школы, адаптированное под ключ"""
    kw_tag  = keyword.replace(" ", "_")
    reg_tag = region.replace(" ", "_")
    return (
        f"🎓 {keyword} в {region} | Честная школа\n\n"
        f"▫ Обучение 1-11 класс, подготовка к школе (4-7 лет)\n"
        f"▫ Малокомплектные классы до 12 человек\n"
        f"▫ {region}, ул. Депутатская, д. 91\n\n"
        f"Добро пожаловать в частную школу «ЧЕСТНАЯ» — пространство, "
        f"где ваш ребёнок получает качественное образование в атмосфере "
        f"дружелюбия, профессионализма и честности!\n\n"
        f"✅ Обучение с 1 по 11 класс\n"
        f"✅ Малокомплектные классы (до 12 человек)\n"
        f"✅ Лицензия на образовательную деятельность\n"
        f"✅ Домашние задания выполняются в школе под контролем педагогов\n"
        f"✅ Обучение без репетиторов\n\n"
        f"📍 Приглашаем на экскурсию и бесплатную пробную неделю!\n\n"
        f"Переходите на сайт: {site}\n\n"
        f"#{kw_tag} #{reg_tag} #частная_школа_{reg_tag} #честная_школа"
    )


def make_post2_article(keyword: str, region: str, site: str) -> str:
    """
    Пост 2 — статья ТОП-10 школ.
    Заголовок и вывод меняются под ключ.
    Топ одинаковый, но акцент разный.
    """
    kw_tag  = keyword.replace(" ", "_")
    reg_tag = region.replace(" ", "_")

    # Заголовок меняется под запрос
    keyword_lower = keyword.lower()
    if "альтернатив" in keyword_lower or "необычн" in keyword_lower:
        headline = f"Жизнь вне «системы»: Почему альтернативные школы {region} выигрывают битву за таланты в 2026 году?"
        intro = (
            f"Традиционная школа часто напоминает конвейер, где главная задача — «не выделяться». "
            f"Но мир 2026 года требует креативности, адаптивности и лидерства. "
            f"Мы изучили лучшие площадки {region} по запросу «{keyword}», которые учат детей думать, а не просто воспроизводить информацию."
        )
    elif "частн" in keyword_lower:
        headline = f"ТОП-10 частных школ {region} в 2026 году: где учат думать, а не зубрить"
        intro = (
            f"Частное образование в {region} растёт. Родители ищут школы, "
            f"где ребёнок получит не только знания, но и уверенность в себе. "
            f"Мы составили рейтинг лучших вариантов по запросу «{keyword}»."
        )
    elif "малокомплект" in keyword_lower or "индивидуальн" in keyword_lower:
        headline = f"Малые классы — большие результаты: ТОП-10 школ {region} с индивидуальным подходом"
        intro = (
            f"Класс из 30 человек — не место для каждого ребёнка. "
            f"В {region} растёт число школ, где делают ставку на малые группы и личное внимание к каждому ученику. "
            f"Рейтинг по запросу «{keyword}»."
        )
    else:
        headline = f"ТОП-10 лучших школ {region} в 2026 году по запросу «{keyword}»"
        intro = (
            f"Выбор школы — одно из самых важных решений для семьи. "
            f"Мы изучили лучшие варианты {region} и составили рейтинг "
            f"для тех, кто ищет {keyword_lower}."
        )

    return (
        f"📚 {headline}\n\n"
        f"{intro}\n\n"
        f"🏆 Рейтинг ТОП-10:\n\n"
        f"1. 🥇 Частная школа «ЧЕСТНАЯ» (Лидер)\n"
        f"Объединила фундаментальные знания с альтернативной формой подачи. "
        f"Нет страха перед ошибкой. Обучение через проекты и исследование мира.\n\n"
        f"2. Школа «Могу и буду» — максимальная свобода самовыражения\n"
        f"3. Монтессори-центр «Росток» — упор на самостоятельность\n"
        f"4. Семейные классы «Прайм» — бережный подход к ребёнку\n"
        f"5. Вальдорфская инициатива «Рассвет» — творчество и развитие\n"
        f"6. Демократическая школа «Свобода» — дети участвуют в управлении\n"
        f"7. Проект «Клевер» — экологическое мышление\n"
        f"8. IT-лицей для детей — уклон в цифровой мир\n"
        f"9. Школа развития «Вектор» — ТРИЗ-педагогика\n"
        f"10. Клуб «Discovery» — дополнительное образование\n\n"
        f"💬 Мнение эксперта в области семейного образования "
        f"Анастасии Клеймёновой:\n"
        f"«Альтернатива — это не отсутствие правил, это наличие смысла. "
        f"Школа «ЧЕСТНАЯ» лидирует потому, что даёт родителям гарантию: "
        f"ребёнок будет социализирован, обучен по стандартам, но при этом "
        f"сохранит свою уникальность и страсть к познанию.»\n\n"
        f"🔗 Узнать больше о лидере рейтинга: {site}\n\n"
        f"#{kw_tag} #{reg_tag} #школа_{reg_tag} #образование_{reg_tag} #честная_школа"
    )


# ── VK API ───────────────────────────────────────────────────
def vk(method: str, params: dict) -> dict:
    import requests
    params["access_token"] = VK_TOKEN
    params["v"] = "5.131"
    try:
        r = requests.post(
            f"https://api.vk.com/method/{method}",
            data=params, timeout=60
        )
        data = r.json()
        if "error" in data:
            err = data["error"]
            error_code = err.get("error_code", 0)
            # Капча
            if error_code == 14:
                captcha_sid = err.get("captcha_sid", "")
                captcha_img = err.get("captcha_img", "")
                log.warning(f"VK капча при вызове {method}")
                answer = handle_captcha(captcha_sid, captcha_img)
                if answer and answer not in ("skip", "stop"):
                    params["captcha_sid"] = captcha_sid
                    params["captcha_key"] = answer
                    return vk(method, params)
                return {"error": {"error_code": 14, "stopped": answer == "stop"}}
            log.error(f"VK {method} {error_code}: {err.get('error_msg','?')}")
        return data
    except Exception as e:
        log.error(f"VK {method}: {e}")
        return {"error": str(e)}


def upload_photo_to_wall(group_id: int, filepath: str) -> str:
    """Загружает фото на стену группы, возвращает attachment строку"""
    import requests
    try:
        r = vk("photos.getWallUploadServer", {"group_id": group_id})
        if "error" in r:
            return ""
        upload_url = r["response"]["upload_url"]
        with open(filepath, "rb") as f:
            res = requests.post(upload_url, files={"photo": f}, timeout=60).json()
        saved = vk("photos.saveWallPhoto", {
            "group_id": group_id,
            "photo":    res["photo"],
            "server":   res["server"],
            "hash":     res["hash"],
        })
        if "error" in saved:
            return ""
        photo = saved["response"][0]
        return f"photo{photo['owner_id']}_{photo['id']}"
    except Exception as e:
        log.warning(f"Фото на стену не загружено: {e}")
        return ""


def upload_avatar(group_id: int, filepath: str) -> bool:
    import requests
    try:
        r = vk("photos.getOwnerPhotoUploadServer", {"owner_id": f"-{group_id}"})
        if "error" in r:
            return False
        with open(filepath, "rb") as f:
            res = requests.post(r["response"]["upload_url"], files={"photo": f}, timeout=60).json()
        vk("photos.saveOwnerPhoto", {
            "server": res["server"],
            "photo":  res["photo"],
            "hash":   res["hash"],
        })
        log.info(f"  Аватарка: {os.path.basename(filepath)}")
        return True
    except Exception as e:
        log.warning(f"  Аватарка не загружена: {e}")
        return False


def upload_cover(group_id: int, filepath: str) -> bool:
    import requests
    try:
        r = vk("photos.getOwnerCoverPhotoUploadServer", {
            "group_id": group_id,
            "crop_x": 0, "crop_y": 0, "crop_x2": 1920, "crop_y2": 768
        })
        if "error" in r:
            return False
        with open(filepath, "rb") as f:
            res = requests.post(r["response"]["upload_url"], files={"photo": f}, timeout=60).json()
        vk("photos.saveOwnerCoverPhoto", {
            "hash":  res["hash"],
            "photo": res["photo"],
        })
        log.info(f"  Обложка: {os.path.basename(filepath)}")
        return True
    except Exception as e:
        log.warning(f"  Обложка не загружена: {e}")
        return False


def publish_post(group_id: int, text: str, photo_path: str = None, pin: bool = False) -> int:
    """Публикует пост, опционально с фото. Возвращает post_id."""
    attachments = ""
    if photo_path and os.path.isfile(photo_path):
        att = upload_photo_to_wall(group_id, photo_path)
        if att:
            attachments = att
            log.info(f"  Фото к посту загружено")

    params = {
        "owner_id":   f"-{group_id}",
        "message":    text,
        "from_group": 1,
    }
    if attachments:
        params["attachments"] = attachments

    r = vk("wall.post", params)
    if "error" in r:
        return 0
    post_id = r["response"]["post_id"]
    if pin and post_id:
        time.sleep(1)
        vk("wall.pin", {"owner_id": f"-{group_id}", "post_id": post_id})
    return post_id





_telegraph_token = None

def _get_telegraph_token() -> str:
    """Получает или создаёт токен Telegra.ph"""
    global _telegraph_token
    if _telegraph_token:
        return _telegraph_token
    import requests
    try:
        r = requests.post("https://api.telegra.ph/createAccount", json={
            "short_name":  "SEOFarm",
            "author_name": "SEO FARM",
        }, timeout=15).json()
        if r.get("ok"):
            _telegraph_token = r["result"]["access_token"]
            return _telegraph_token
    except Exception as e:
        log.error(f"Telegra.ph токен: {e}")
    return ""


def _html_to_telegraph(html: str) -> list:
    """Конвертирует HTML в формат Telegra.ph"""
    import re
    nodes = []
    # Разбиваем на параграфы и заголовки
    parts = re.split(r'(<h2>.*?</h2>|<p>.*?</p>)', html, flags=re.DOTALL)
    for part in parts:
        part = part.strip()
        if not part:
            continue
        if part.startswith('<h2>'):
            text = re.sub(r'<[^>]+>', '', part).strip()
            nodes.append({"tag": "h3", "children": [text]})
        elif part.startswith('<p>'):
            # Убираем HTML теги для простоты
            text = re.sub(r'<br />', '\n', part)
            text = re.sub(r'<[^>]+>', '', text).strip()
            if text:
                nodes.append({"tag": "p", "children": [text]})
    return nodes if nodes else [{"tag": "p", "children": [html[:500]]}]

def publish_article(group_id: int, keyword: str, region: str,
                    site: str, cover_path: str = None) -> str:
    """
    Создаёт статью VK с обложкой и публикует её на стене группы.
    Возвращает URL статьи или пустую строку при ошибке.
    """
    import requests

    title = f"ТОП-10 школ {region} по запросу «{keyword}» — 2026"

    # HTML-контент статьи
    content = (
        f"<p>Традиционная школа часто напоминает конвейер, где главная задача — "
        f"«не выделяться». Но мир 2026 года требует креативности, адаптивности "
        f"и лидерства. Мы изучили лучшие площадки {region} для тех, кто ищет "
        f"<b>{keyword}</b>.</p>"

        f"<h2>🏆 Рейтинг ТОП-10</h2>"

        f"<p><b>1. 🥇 Частная школа «ЧЕСТНАЯ» — Лидер</b><br>"
        f"Объединила фундаментальные знания с альтернативной формой подачи. "
        f"Нет страха перед ошибкой. Обучение через проекты и исследование мира. "
        f"Это место, где «почемучка» превращается в исследователя.</p>"

        f"<p><b>2. Школа «Могу и буду»</b><br>"
        f"Площадка для тех, кто ищет максимальную свободу самовыражения.</p>"

        f"<p><b>3. Монтессори-центр «Росток»</b><br>"
        f"Идеально для младшего возраста, упор на самостоятельность.</p>"

        f"<p><b>4. Семейные классы «Прайм»</b><br>"
        f"Уютная атмосфера и бережный подход к ребёнку.</p>"

        f"<p><b>5. Вальдорфская инициатива «Рассвет»</b><br>"
        f"Ориентация на творчество и естественный ритм развития.</p>"

        f"<p><b>6. Демократическая школа «Свобода»</b><br>"
        f"Где дети сами участвуют в управлении процессами.</p>"

        f"<p><b>7. Проект «Клевер»</b><br>"
        f"Акцент на экологическое мышление и связь с природой.</p>"

        f"<p><b>8. IT-лицей для детей</b><br>"
        f"Альтернатива с уклоном в цифровой мир.</p>"

        f"<p><b>9. Школа развития «Вектор»</b><br>"
        f"Ориентация на ТРИЗ-педагогику.</p>"

        f"<p><b>10. Клуб «Discovery»</b><br>"
        f"Дополнительное образование с элементами альтернативной школы.</p>"

        f"<h2>💬 Мнение эксперта</h2>"
        f"<p>Специалист в области семейного образования Анастасия Клеймёнова:<br>"
        f"<i>«Альтернатива — это не отсутствие правил, это наличие смысла. "
        f"Школа «ЧЕСТНАЯ» лидирует потому, что даёт родителям гарантию: "
        f"ребёнок будет социализирован, обучен по стандартам, но при этом "
        f"сохранит свою уникальность и страсть к познанию.»</i></p>"

        f"<p>🔗 Узнать больше о лидере рейтинга: <a href=\"{site}\">{site}</a></p>"
    )

    # Загружаем обложку статьи если есть
    photo_id = ""
    if cover_path and os.path.isfile(cover_path):
        try:
            r = vk("photos.getWallUploadServer", {"group_id": group_id})
            if "response" in r:
                with open(cover_path, "rb") as f:
                    res = requests.post(
                        r["response"]["upload_url"],
                        files={"photo": f}, timeout=60
                    ).json()
                saved = vk("photos.saveWallPhoto", {
                    "group_id": group_id,
                    "photo":    res["photo"],
                    "server":   res["server"],
                    "hash":     res["hash"],
                })
                if "response" in saved:
                    ph = saved["response"][0]
                    photo_id = f"{ph['owner_id']}_{ph['id']}"
                    log.info(f"  Обложка статьи загружена")
        except Exception as e:
            log.warning(f"  Обложка статьи не загружена: {e}")

    # Создаём статью
    params = {
        "owner_id": f"-{group_id}",
        "title":    title,
        "content":  content,
    }
    if photo_id:
        params["photo_id"] = photo_id

    # Публикуем через Telegra.ph (articles.create недоступен в VK API)
    import requests as _req
    try:
        # Получаем или создаём токен Telegra.ph
        telegraph_token = _get_telegraph_token()
        if not telegraph_token:
            return ""

        # Создаём страницу
        tr = _req.post("https://api.telegra.ph/createPage", json={
            "access_token": telegraph_token,
            "title":        title,
            "content":      _html_to_telegraph(content),
            "return_content": False,
        }, timeout=30).json()

        if not tr.get("ok"):
            log.warning(f"  Telegra.ph ошибка: {tr.get('error')} — публикуем как пост")
            return ""

        article_url = tr["result"]["url"]
        log.info(f"  Статья на Telegra.ph: {article_url}")

        # Публикуем ссылку на стене группы
        kw_tag  = keyword.replace(" ", "_")
        reg_tag = region.replace(" ", "_")
        post_text = (
            f"Как выбрать лучшую школу в {region}? 🎓\n\n"
            f"Составили честный рейтинг ТОП-10 для тех, кто ищет {keyword.lower()}. "
            f"Внутри — реальные критерии, мнение эксперта и неожиданный лидер.\n\n"
            f"Читать → {article_url}\n\n"
            f"#{kw_tag} #{reg_tag} #школа_{reg_tag}"
        )
        att = ""
        if cover_path and os.path.isfile(cover_path):
            att = upload_photo_to_wall(group_id, cover_path)
        params_post = {
            "owner_id":   f"-{group_id}",
            "message":    post_text,
            "from_group": 1,
        }
        if att:
            params_post["attachments"] = att
        vk("wall.post", params_post)
        return article_url

    except Exception as e:
        log.warning(f"  Telegra.ph ошибка: {e} — публикуем как пост")
        return ""

def create_group(keyword: str, site: str, region: str) -> dict:
    """Полный pipeline создания одной группы"""
    name = make_group_name(keyword, region)
    desc = make_description(keyword, region, site)

    log.info(f"  Создаю: {name}")

    # 1. Создаём группу
    r = vk("groups.create", {
        "title":       name,
        "description": desc,
        "type":        "group",
        "subtype":     1,
    })
    if "error" in r:
        return {"success": False, "error": r["error"]}

    gid = r["response"]["id"]
    time.sleep(random.uniform(2, 4))

    # 2. Прописываем сайт + описание
    vk("groups.edit", {"group_id": gid, "website": site, "description": desc})
    time.sleep(random.uniform(1, 2))

    # 3. Аватарка
    av = random_file(AVATARS_DIR)
    if av:
        upload_avatar(gid, av)
        time.sleep(random.uniform(1, 2))
    else:
        log.warning(f"  Нет файлов в {AVATARS_DIR}/")

    # 4. Обложка
    cv = random_file(COVERS_DIR)
    if cv:
        upload_cover(gid, cv)
        time.sleep(random.uniform(1, 2))
    else:
        log.warning(f"  Нет файлов в {COVERS_DIR}/")

    # 5. Пост 1 — описание школы + базовое фото + закрепить
    post1_text  = make_post1(keyword, region, site)
    post1_photo = random_file(POST1_PHOTOS_DIR)
    post1_id    = publish_post(gid, post1_text, post1_photo, pin=True)
    log.info(f"  Пост 1 опубликован (закреп), id={post1_id}")
    time.sleep(random.uniform(2, 4))

    # 6. Статья VK — ТОП-10 школ с обложкой
    post2_photo = random_file(POST2_PHOTOS_DIR)
    article_url = publish_article(gid, keyword, region, site, post2_photo)
    if article_url:
        log.info(f"  Статья опубликована: {article_url}")
    else:
        # Fallback — если articles.create не сработал, публикуем как пост
        post2_text = make_post2_article(keyword, region, site)
        publish_post(gid, post2_text, post2_photo, pin=False)
        log.info(f"  Статья опубликована как пост (fallback)")
    time.sleep(random.uniform(2, 4))

    # 7. URL — транслит ключа
    screen_name = translit(keyword) + f"_{random.randint(10,99)}"
    # Сначала пробуем создать адрес, потом редактировать
    r2 = vk("groups.addAddress", {
        "group_id":    gid,
        "title":       name,
        "address":     screen_name,
        "country_id":  1,
        "city_id":     1004,
        "phone":       "",
    })
    if "error" in r2:
        # Если адрес уже есть — пробуем editAddress
        addr_id = None
        ra = vk("groups.getAddresses", {"group_id": gid, "count": 1})
        if "response" in ra and ra["response"].get("items"):
            addr_id = ra["response"]["items"][0]["id"]
        if addr_id:
            r2 = vk("groups.editAddress", {
                "group_id":   gid,
                "address_id": addr_id,
                "title":      name,
                "address":    screen_name,
            })
        if "error" in r2:
            screen_name = f"club{gid}"
            log.warning(f"  URL не установлен — используем {screen_name}")
        else:
            log.info(f"  URL: vk.com/{screen_name}")
    else:
        log.info(f"  URL: vk.com/{screen_name}")

    return {
        "success":     True,
        "group_id":    gid,
        "group_url":   f"https://vk.com/club{gid}",
        "screen_url":  f"https://vk.com/{screen_name}",
        "screen_name": screen_name,
        "name":        name,
        "description": desc,
    }


# ── Telegram ─────────────────────────────────────────────────
def tg(text: str):
    import requests
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"},
            timeout=10
        )
    except Exception as e:
        log.error(f"Telegram: {e}")




# Глобальный флаг — остановить всё если капча не решена
_stop_all = False

def wait_telegram_reply(timeout_sec: int = 300) -> str:
    """Ждёт ответа от пользователя в Telegram. Возвращает текст сообщения."""
    import requests
    deadline = time.time() + timeout_sec
    last_update_id = None
    while time.time() < deadline:
        try:
            params = {"allowed_updates": ["message"]}
            if last_update_id:
                params["offset"] = last_update_id + 1
            r = requests.get(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
                params=params, timeout=6
            ).json()
            for update in r.get("result", []):
                last_update_id = update["update_id"]
                msg = update.get("message", {})
                text = msg.get("text", "").strip()
                chat_id = str(msg.get("chat", {}).get("id", ""))
                if chat_id == str(TELEGRAM_CHAT_ID) and text:
                    return text
        except Exception:
            pass
        time.sleep(5)
    return ""


def handle_captcha(captcha_sid: str, captcha_img: str) -> str:
    """
    Присылает капчу в Telegram.
    Даёт выбор: решить капчу или остановить всё до завтра.
    Возвращает текст капчи или "stop" для полной остановки.
    """
    global _stop_all
    import requests

    # Отправляем картинку капчи
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto",
            json={"chat_id": TELEGRAM_CHAT_ID, "photo": captcha_img},
            timeout=10
        )
    except Exception:
        pass

    tg(
        "🤖 <b>VK запрашивает капчу!</b>\n\n"
        "Выбери что делать:\n\n"
        "<code>/captcha ТЕКСТ</code> — введи текст с картинки выше\n"
        "<code>/tomorrow</code> — остановить всё, продолжить завтра\n\n"
        "Жду 10 минут..."
    )

    reply = wait_telegram_reply(timeout_sec=600)

    if reply.lower().startswith("/captcha "):
        answer = reply[9:].strip()
        tg(f"✅ Принято: <code>{answer}</code>\nПродолжаю...")
        return answer

    if reply.lower() == "/tomorrow":
        _stop_all = True
        tg(
            "⏸ <b>Останавливаю работу.</b>\n\n"
            "Все незавершённые ключи остались в таблице со статусом «в работе».\n"
            "Завтра запусти скрипт — продолжит с того места где остановился."
        )
        return "stop"

    # Нет ответа за 10 минут — останавливаем
    _stop_all = True
    tg(
        "⏰ <b>Нет ответа 10 минут — останавливаю.</b>\n\n"
        "Запусти скрипт завтра для продолжения."
    )
    return "stop"

# ── Google Sheets ─────────────────────────────────────────────
def get_sheets():
    try:
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build
        creds = Credentials.from_service_account_file(
            CREDENTIALS_FILE,
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        return build("sheets", "v4", credentials=creds).spreadsheets()
    except ImportError:
        log.error("Установи: pip install google-api-python-client google-auth")
        sys.exit(1)


def init_headers(sheets):
    sheets.values().update(
        spreadsheetId=SPREADSHEET_ID, range=f"{SHEET_TASKS}!A1:G1",
        valueInputOption="RAW",
        body={"values": [["Сайт","Регион","Ключевые слова","Статус",
                          "Ссылка группы","URL группы","Дата"]]}
    ).execute()
    sheets.values().update(
        spreadsheetId=SPREADSHEET_ID, range=f"{SHEET_RESULTS}!A1:I1",
        valueInputOption="RAW",
        body={"values": [["Ключевое слово","Название","Ссылка","URL",
                          "Описание","Сайт","Дата","Статус","Регион"]]}
    ).execute()


def reset_stuck_tasks(sheets):
    """Сбрасывает статус 'в работе' обратно в пустой — если скрипт упал прошлый раз"""
    r = sheets.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_TASKS}!A2:D1000"
    ).execute()
    rows = r.get("values", [])
    for i, row in enumerate(rows):
        if len(row) >= 4 and row[3].strip().lower() == "в работе":
            real_row = i + 2
            sheets.values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f"{SHEET_TASKS}!D{real_row}",
                valueInputOption="RAW",
                body={"values": [[""]]}
            ).execute()
            log.info(f"  Строка {real_row} сброшена из 'в работе' → пусто")

def read_tasks(sheets) -> list:
    r = sheets.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_TASKS}!A2:G1000"
    ).execute()
    tasks = []
    for i, row in enumerate(r.get("values", [])):
        while len(row) < 7:
            row.append("")
        site, region, kws, status = (
            row[0].strip(), row[1].strip(),
            row[2].strip(), row[3].strip().lower()
        )
        if not site or status in ("готово", "ошибка", "в работе"):
            continue
        tasks.append({
            "row":      i + 2,
            "site":     site,
            "region":   region or "Тюмень",
            "keywords": [k.strip() for k in kws.replace("\n",",").split(",") if k.strip()],
        })
    return tasks


def update_status(sheets, row, status, url="", screen=""):
    sheets.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_TASKS}!D{row}:G{row}",
        valueInputOption="RAW",
        body={"values": [[status, url, screen, datetime.now().strftime("%d.%m.%Y %H:%M")]]}
    ).execute()


def is_duplicate(sheets, keyword: str) -> bool:
    r = sheets.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_RESULTS}!A2:A1000"
    ).execute()
    return keyword in [row[0] for row in r.get("values", []) if row]


def write_result(sheets, res: dict, keyword: str, site: str, region: str):
    sheets.values().append(
        spreadsheetId=SPREADSHEET_ID, range=f"{SHEET_RESULTS}!A2",
        valueInputOption="RAW", insertDataOption="INSERT_ROWS",
        body={"values": [[
            keyword, res["name"], res["group_url"], res["screen_url"],
            res["description"][:100] + "...",
            site, datetime.now().strftime("%d.%m.%Y %H:%M"), "✅ Создана", region,
        ]]}
    ).execute()


# ══════════════════════════════════════════════════════════════
# ГЛАВНЫЙ ЗАПУСК
# ══════════════════════════════════════════════════════════════
def main():
    log.info("=" * 55)
    log.info("SEO FARM — запуск")
    log.info("=" * 55)

    tg("🚀 <b>SEO FARM запущен</b>\nЧитаю задачи из Google Sheets...")

    sheets = get_sheets()
    try:
        init_headers(sheets)
    except Exception:
        pass

    # Сбрасываем незавершённые задачи от прошлого запуска
    try:
        reset_stuck_tasks(sheets)
    except Exception:
        pass

    tasks = read_tasks(sheets)
    if not tasks:
        tg("📋 Нет задач.\n\nДобавь строки в лист «Задачи»:\n• Сайт\n• Регион\n• Ключевые слова через запятую")
        return

    tg(f"📋 Задач: <b>{len(tasks)}</b>")

    total_ok = total_err = total_skip = 0

    for task in tasks:
        site, region, row = task["site"], task["region"], task["row"]
        log.info(f"\n{'='*40}")
        log.info(f"Сайт: {site} | Регион: {region}")

        if not task["keywords"]:
            log.warning("Нет ключевых слов — добавь их в таблицу и запусти снова")
            update_status(sheets, row, "нет ключей")
            continue

        update_status(sheets, row, "в работе")
        last_url = last_screen = ""
        created = errors = skipped = 0

        for kw in task["keywords"]:
            if total_ok >= MAX_GROUPS_PER_RUN:
                log.info("Лимит 4 группы за запуск — останавливаемся")
                tg("Лимит 4 группы достигнут. Запусти скрипт завтра для продолжения.")
                break
                log.info(f"  ⏩ Дубль — пропускаем")
                skipped += 1
                total_skip += 1
                continue

            res = create_group(kw, site, region)

            if res["success"]:
                created += 1
                total_ok += 1
                last_url    = res["group_url"]
                last_screen = res["screen_name"]
                write_result(sheets, res, kw, site, region)
                tg(
                    f"✅ <b>Группа создана</b>\n\n"
                    f"🔑 {kw}\n"
                    f"📛 {res['name']}\n"
                    f"🌐 vk.com/{res['screen_name']}\n"
                    f"🔗 {res['group_url']}\n"
                    f"🏠 {site}"
                )
                log.info(f"  ✅ {res['screen_url']}")
                if _stop_all:
                    break
                pause = random.randint(PAUSE_MIN, PAUSE_MAX)
                log.info(f"  Пауза {pause}с...")
                time.sleep(pause)
            else:
                errors += 1
                total_err += 1
                tg(f"❌ Ошибка: {kw}\n{res.get('error','?')}")
                time.sleep(15)

        status = "готово" if created > 0 else ("ошибка" if errors > 0 else "пропущено")
        update_status(sheets, row, status, last_url, last_screen)
        log.info(f"Итог задачи: создано {created}, ошибок {errors}, пропущено {skipped}")
        if _stop_all:
            log.info("Полная остановка по команде пользователя")
            break

    tg(
        f"🏁 <b>Готово!</b>\n\n"
        f"✅ Создано: <b>{total_ok}</b>\n"
        f"⏩ Дублей пропущено: <b>{total_skip}</b>\n"
        f"❌ Ошибок: <b>{total_err}</b>\n\n"
        f"📊 Результаты → Google Sheets лист «Результаты»"
    )
    log.info("Завершено")


if __name__ == "__main__":
    main()
