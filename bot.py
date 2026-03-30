"""
bot.py — Telegram бот для управления SEO FARM агентом
Запускай отдельно: python bot.py
Агент запускается/останавливается прямо из Telegram через кнопки
"""
import os, sys, time, logging, threading, subprocess
from datetime import datetime

# ══════════════════════════════════════════════════════════════
# НАСТРОЙКИ — те же что в run.py
# ══════════════════════════════════════════════════════════════
TELEGRAM_TOKEN     = "8009097004:AAFF28Ef_QiiTCoAn4Koe-RXtoxXrElAusM"
TELEGRAM_CHAT_ID   = "8002970207"
RUN_SCRIPT       = "run.py"      # путь к скрипту агента
# ══════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("bot.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger("bot")

# Состояние агента
_agent_process = None
_agent_running = False
_start_time    = None


def api(method: str, params: dict) -> dict:
    import requests
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}",
            json=params, timeout=10
        )
        return r.json()
    except Exception as e:
        log.error(f"API {method}: {e}")
        return {}


def send(text: str, buttons: list = None):
    """
    Отправляет сообщение с inline-кнопками.
    buttons = [[("Текст кнопки", "callback_data"), ...], ...]
    """
    params = {
        "chat_id":    TELEGRAM_CHAT_ID,
        "text":       text,
        "parse_mode": "HTML",
    }
    if buttons:
        keyboard = []
        for row in buttons:
            keyboard.append([
                {"text": btn[0], "callback_data": btn[1]}
                for btn in row
            ])
        params["reply_markup"] = {"inline_keyboard": keyboard}
    api("sendMessage", params)


def answer_callback(callback_id: str, text: str = ""):
    """Убирает часики на кнопке после нажатия"""
    api("answerCallbackQuery", {
        "callback_query_id": callback_id,
        "text": text,
        "show_alert": False,
    })


def edit_message(chat_id, message_id, text, buttons=None):
    """Обновляет существующее сообщение"""
    params = {
        "chat_id":    chat_id,
        "message_id": message_id,
        "text":       text,
        "parse_mode": "HTML",
    }
    if buttons:
        keyboard = []
        for row in buttons:
            keyboard.append([
                {"text": btn[0], "callback_data": btn[1]}
                for btn in row
            ])
        params["reply_markup"] = {"inline_keyboard": keyboard}
    api("editMessageText", params)


# ── Главное меню ─────────────────────────────────────────────
def show_main_menu(text="👋 Управление SEO FARM агентом"):
    global _agent_running
    status = "🟢 Агент работает" if _agent_running else "🔴 Агент остановлен"
    send(
        f"{text}\n\n"
        f"Статус: <b>{status}</b>",
        buttons=[
            [("▶️ Запустить агента", "start"), ("⏹ Остановить", "stop")],
            [("📊 Статус", "status"), ("📋 Логи", "logs")],
            [("⚙️ Настройки", "settings")],
        ]
    )


# ── Запуск агента ─────────────────────────────────────────────
def start_agent():
    global _agent_process, _agent_running, _start_time
    if _agent_running:
        return False

    try:
        # Запускаем из той же папки где лежит bot.py
        script_dir = os.path.dirname(os.path.abspath(__file__))
        run_path = os.path.join(script_dir, RUN_SCRIPT)
        _agent_process = subprocess.Popen(
            [sys.executable, run_path],
            cwd=script_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="ignore",
        )
        _agent_running = True
        _start_time    = datetime.now()
        log.info("Агент запущен")

        # Мониторим завершение в фоне
        def monitor():
            global _agent_running
            _agent_process.wait()
            _agent_running = False
            log.info("Агент завершил работу")
            send(
                "🏁 <b>Агент завершил работу</b>",
                buttons=[[("🔄 Запустить снова", "start"), ("📊 Статус", "status")]]
            )

        threading.Thread(target=monitor, daemon=True).start()
        return True
    except Exception as e:
        log.error(f"Ошибка запуска: {e}")
        return False


def stop_agent():
    global _agent_process, _agent_running
    if not _agent_running or not _agent_process:
        return False
    try:
        _agent_process.terminate()
        _agent_running = False
        log.info("Агент остановлен")
        return True
    except Exception as e:
        log.error(f"Ошибка остановки: {e}")
        return False


def get_status_text() -> str:
    if _agent_running and _start_time:
        elapsed = int((datetime.now() - _start_time).total_seconds())
        mins = elapsed // 60
        secs = elapsed % 60
        return (
            f"🟢 <b>Агент работает</b>\n"
            f"⏱ Время работы: {mins}м {secs}с"
        )
    return "🔴 <b>Агент остановлен</b>"


def get_last_logs(lines=10) -> str:
    """Последние строки из run.log"""
    try:
        with open("run.log", encoding="utf-8", errors="ignore") as f:
            all_lines = f.readlines()
        last = [l.strip() for l in all_lines[-lines:] if l.strip()]
        return "\n".join(last) if last else "Логов нет"
    except FileNotFoundError:
        return "Файл run.log не найден"


# ── Обработка нажатий на кнопки ──────────────────────────────
def handle_callback(update: dict):
    cb      = update["callback_query"]
    cb_id   = cb["id"]
    data    = cb.get("data", "")
    chat_id = cb["message"]["chat"]["id"]
    msg_id  = cb["message"]["message_id"]

    answer_callback(cb_id)

    if data == "start":
        if _agent_running:
            edit_message(chat_id, msg_id,
                "⚠️ Агент уже запущен!",
                buttons=[[("📊 Статус", "status"), ("⏹ Остановить", "stop")]]
            )
        else:
            ok = start_agent()
            if ok:
                edit_message(chat_id, msg_id,
                    "✅ <b>Агент запущен!</b>\n\nОтчёты будут приходить сюда.",
                    buttons=[
                        [("⏹ Остановить", "stop"), ("📊 Статус", "status")],
                        [("🏠 Главное меню", "menu")],
                    ]
                )
            else:
                edit_message(chat_id, msg_id,
                    "❌ Не удалось запустить агента.\nПроверь что run.py находится рядом с bot.py",
                    buttons=[[("🔄 Попробовать снова", "start"), ("🏠 Меню", "menu")]]
                )

    elif data == "stop":
        if not _agent_running:
            edit_message(chat_id, msg_id,
                "⚠️ Агент не запущен.",
                buttons=[[("▶️ Запустить", "start"), ("🏠 Меню", "menu")]]
            )
        else:
            stop_agent()
            edit_message(chat_id, msg_id,
                "⏹ <b>Агент остановлен.</b>",
                buttons=[
                    [("▶️ Запустить снова", "start"), ("📊 Статус", "status")],
                    [("🏠 Главное меню", "menu")],
                ]
            )

    elif data == "status":
        edit_message(chat_id, msg_id,
            f"📊 <b>Статус агента</b>\n\n{get_status_text()}",
            buttons=[
                [("🔄 Обновить", "status"), ("📋 Логи", "logs")],
                [("🏠 Главное меню", "menu")],
            ]
        )

    elif data == "logs":
        logs = get_last_logs(15)
        edit_message(chat_id, msg_id,
            f"📋 <b>Последние логи:</b>\n\n<code>{logs[:3000]}</code>",
            buttons=[
                [("🔄 Обновить", "logs"), ("📊 Статус", "status")],
                [("🏠 Главное меню", "menu")],
            ]
        )

    elif data == "settings":
        edit_message(chat_id, msg_id,
            "⚙️ <b>Настройки</b>\n\n"
            "Все настройки задаются в файле <code>run.py</code> в блоке НАСТРОЙКИ:\n\n"
            "• <code>SPREADSHEET_ID</code> — ID Google таблицы\n"
            "• <code>VK_TOKEN</code> — токен VK аккаунта\n"
            "• <code>MAX_GROUPS_PER_RUN</code> — макс. групп за запуск\n"
            "• <code>PAUSE_MIN/MAX</code> — паузы между группами (сек)",
            buttons=[[("🏠 Главное меню", "menu")]]
        )

    elif data == "menu":
        show_main_menu()


# ── Обработка текстовых сообщений ───────────────────────────
def handle_message(update: dict):
    msg     = update.get("message", {})
    text    = msg.get("text", "").strip()
    chat_id = str(msg.get("chat", {}).get("id", ""))

    if chat_id != str(TELEGRAM_CHAT_ID):
        return

    if text in ("/start", "/menu"):
        show_main_menu()

    # Капча — обрабатывается в run.py через getUpdates
    # Здесь просто подтверждаем получение
    elif text.lower().startswith("/captcha "):
        pass  # обрабатывается в run.py

    elif text.lower() == "/tomorrow":
        pass  # обрабатывается в run.py


# ── Главный цикл polling ─────────────────────────────────────
def run_bot():
    log.info("Бот запущен. Жди сообщений...")
    send(
        "🚀 <b>SEO FARM бот запущен!</b>\n\nНажми кнопку чтобы начать:",
        buttons=[
            [("▶️ Запустить агента", "start"), ("📊 Статус", "status")],
            [("📋 Логи", "logs"), ("⚙️ Настройки", "settings")],
        ]
    )

    offset = None
    while True:
        try:
            params = {"timeout": 25, "allowed_updates": ["message", "callback_query"]}
            if offset:
                params["offset"] = offset
            r = api("getUpdates", params)

            for update in r.get("result", []):
                offset = update["update_id"] + 1
                if "callback_query" in update:
                    handle_callback(update)
                elif "message" in update:
                    handle_message(update)

        except KeyboardInterrupt:
            log.info("Бот остановлен")
            break
        except Exception as e:
            if 'timed out' not in str(e).lower():
                log.error(f"Polling ошибка: {e}")
            time.sleep(2)


if __name__ == "__main__":
    run_bot()
