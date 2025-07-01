import os

import requests
from dotenv import load_dotenv

class TelegramBot:
    # Load environment variables
    load_dotenv()
    TOKEN = os.getenv("BOT_TOKEN")
    CHAT_ID = os.getenv("CHAT_ID")

    @staticmethod
    def remove_duplicates(events):
        seen = set()
        unique_events = []

        for event in events:
            if event not in seen:
                seen.add(event)
                unique_events.append(event)

        return unique_events

    @staticmethod
    def send_message(message: str) -> bool:
        if not TelegramBot.TOKEN or not TelegramBot.CHAT_ID:
            print("Error: BOT_TOKEN or CHAT_ID not found in environment variables.")
            return False

        url = f"https://api.telegram.org/bot{TelegramBot.TOKEN}/sendMessage"
        payload = {
            "chat_id": TelegramBot.CHAT_ID,
            "text": message,
            "parse_mode": "Markdown"  # <-- mensajes mas lindos
        }

        try:
            response = requests.post(url, json=payload)
            if response.status_code == 200:
                return True
            else:
                print(f"Failed to send message. Status code: {response.status_code}")
                print(response.text)
                return False
        except Exception as e:
            print(f"Error sending message: {e}")
            return False

    @staticmethod
    def notify_analysis_result(result):
        print("[TELEGRAM BOT - SE NOTIFICARA EL RESULTADO DEL ULTIMO ANALISIS]")

        sentiment_map = {
            1:  "📈 Bullish",
            -1: "📉 Bearish",
            0:  "🔷 Neutral"
        }
        currency2 = result[0][0]
        currency1 = result[1][0]
        pair_name = result[2][0]
        prediction_currency2, coef_currency2 = result[0][1]  # CURRENCY 2
        prediction_currency1, coef_currency1 = result[1][1]  # CURRENCY 1
        prediction_currency_pair, coef_currency_pair = result[2][1]  ##CURRENCY1CURRENCY2
        text = f"""
📊 *Nuevo Análisis Disponible*

🔍 *Par analizado:* `{pair_name}`

━━━━━━━━━━━━━━━━━━━━
💱 *{currency1}*
Sentimiento: *{sentiment_map[prediction_currency1]}*
Confianza: *{int(float(coef_currency1)*100)}%*
━━━━━━━━━━━━━━━━━━━━
💱 *{currency2}*
Sentimiento: *{sentiment_map[prediction_currency2]}*
Confianza: *{int(float(coef_currency2)*100)}%*
━━━━━━━━━━━━━━━━━━━━
🔗 *Par {pair_name}*
Sentimiento: *{sentiment_map[prediction_currency_pair]}*
Confianza: *{int(float(coef_currency_pair)*100)}%*
━━━━━━━━━━━━━━━━━━━━

🧠 _Este análisis fue generado automáticamente por el sistema de predicción de abby!!._
"""
        TelegramBot.send_message(text)

    @staticmethod
    def post(events):
        print("[TELEGRAM BOT - SE ENVIARAN SIN FILTRAR]")
        print(events)
        unique_events = TelegramBot.remove_duplicates(events)
        print("[TELEGRAM BOT - FILTRADO]")
        print(unique_events)

        for (title, country, actual) in unique_events:
            text = f"""
    📢 *Nuevo Evento Económico Detectado*

    🌍 *País:* `{country}`
    📌 *Evento:* _{title}_
    📈 *Valor actual:* *{actual}*

    🧠 _Notificación enviada automáticamente por el sistema._
    """
            TelegramBot.send_message(text)
