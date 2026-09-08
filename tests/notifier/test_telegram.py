import responses
from src.notifier.telegram import TelegramNotifier

API_BASE = "https://api.telegram.org"


@responses.activate
def test_send_job_alert_posts_expected_payload():
    responses.add(
        responses.POST, f"{API_BASE}/bottoken123/sendMessage",
        json={"ok": True, "result": {"message_id": 1}}, status=200,
    )
    notifier = TelegramNotifier(bot_token="token123", chat_id="chat1", api_base=API_BASE)
    notifier.send_job_alert(
        job_id=42, title="Backend Engineer", company="Acme", location="Remote",
        employment_type="full_time", source="remoteok", url="http://x/42",
    )
    request = responses.calls[0].request
    import json
    payload = json.loads(request.body)
    assert payload["chat_id"] == "chat1"
    assert "Backend Engineer" in payload["text"]
    assert "Acme" in payload["text"]
    buttons = payload["reply_markup"]["inline_keyboard"][0]
    assert buttons[0]["callback_data"] == "snooze:42"
    assert buttons[1]["callback_data"] == "skip:42"


@responses.activate
def test_send_health_alert_posts_source_and_error():
    responses.add(
        responses.POST, f"{API_BASE}/bottoken123/sendMessage",
        json={"ok": True, "result": {"message_id": 2}}, status=200,
    )
    notifier = TelegramNotifier(bot_token="token123", chat_id="chat1", api_base=API_BASE)
    notifier.send_health_alert(source="linkedin", last_error="Timeout")
    import json
    payload = json.loads(responses.calls[0].request.body)
    assert "linkedin" in payload["text"]
    assert "Timeout" in payload["text"]


@responses.activate
def test_get_callback_updates_passes_offset_and_parses_result():
    responses.add(
        responses.GET, f"{API_BASE}/bottoken123/getUpdates",
        json={"ok": True, "result": [{"update_id": 5, "callback_query": {"data": "skip:1"}}]},
        status=200,
    )
    notifier = TelegramNotifier(bot_token="token123", chat_id="chat1", api_base=API_BASE)
    updates = notifier.get_callback_updates(offset=6)
    assert updates == [{"update_id": 5, "callback_query": {"data": "skip:1"}}]
    assert responses.calls[0].request.params["offset"] == "6"
