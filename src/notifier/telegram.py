import requests

API_BASE = "https://api.telegram.org"


class TelegramNotifier:
    def __init__(self, bot_token: str, chat_id: str, api_base: str = API_BASE):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.api_base = api_base

    def _url(self, method: str) -> str:
        return f"{self.api_base}/bot{self.bot_token}/{method}"

    def send_job_alert(self, job_id: int, title: str, company: str, location: str,
                        employment_type: str, source: str, url: str) -> dict:
        text = f"{title} at {company}\n{location} | {employment_type} | via {source}\n{url}"
        keyboard = {
            "inline_keyboard": [[
                {"text": "Snooze 24h", "callback_data": f"snooze:{job_id}"},
                {"text": "Skip", "callback_data": f"skip:{job_id}"},
            ]]
        }
        response = requests.post(
            self._url("sendMessage"),
            json={"chat_id": self.chat_id, "text": text, "reply_markup": keyboard},
            timeout=10,
        )
        response.raise_for_status()
        return response.json()

    def send_health_alert(self, source: str, last_error: str) -> dict:
        text = f"Source {source} disabled after repeated failures.\nLast error: {last_error}"
        response = requests.post(
            self._url("sendMessage"),
            json={"chat_id": self.chat_id, "text": text},
            timeout=10,
        )
        response.raise_for_status()
        return response.json()

    def get_callback_updates(self, offset: int | None = None) -> list[dict]:
        params = {"timeout": 0}
        if offset is not None:
            params["offset"] = offset
        response = requests.get(self._url("getUpdates"), params=params, timeout=10)
        response.raise_for_status()
        return response.json().get("result", [])
