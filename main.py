import json
import random
import time
import os

from dotenv import load_dotenv

from src.db import get_connection, init_db, DB_PATH
from src.ingestion.health import record_success, record_failure, is_disabled, should_poll
from src.ingestion.we_work_remotely import WeWorkRemotelyAdapter
from src.ingestion.remoteok import RemoteOKAdapter
from src.ingestion.remotive import RemotiveAdapter
from src.ingestion.nodesk import NoDeskAdapter
from src.ingestion.flexjobs import FlexJobsAdapter
from src.ingestion.linkedin import LinkedInAdapter
from src.notifier.telegram import TelegramNotifier
from src.notifier.callbacks import apply_callback
from src.orchestrator import process_job, resurface_snoozed

DEFAULT_ADAPTER_CLASSES = {
    "we_work_remotely": WeWorkRemotelyAdapter,
    "remoteok": RemoteOKAdapter,
    "remotive": RemotiveAdapter,
    "nodesk": NoDeskAdapter,
    "flexjobs": FlexJobsAdapter,
    "linkedin": LinkedInAdapter,
}


def load_json(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def run_cycle(conn, sources_config: dict, blacklist: dict, profile: dict, notifier,
              adapter_classes: dict = DEFAULT_ADAPTER_CLASSES) -> dict:
    results = {}
    for source, cfg in sources_config.items():
        if not cfg.get("enabled", False):
            continue
        if is_disabled(conn, source):
            continue
        if not should_poll(conn, source, cfg.get("poll_interval_minutes", 30)):
            continue

        low, high = cfg.get("delay_range_seconds", [1, 3])
        if high > 0:
            time.sleep(random.uniform(low, high))

        adapter = adapter_classes[source]()
        try:
            postings = adapter.fetch()
        except Exception as exc:
            just_disabled = record_failure(conn, source)
            if just_disabled:
                notifier.send_health_alert(source, str(exc))
            continue

        record_success(conn, source)
        results[source] = [
            process_job(conn, job, blacklist, profile, notifier) for job in postings
        ]

    resurface_snoozed(conn, notifier)

    offset = None
    for update in notifier.get_callback_updates(offset):
        callback = update.get("callback_query")
        if callback:
            apply_callback(conn, callback["data"])
            offset = update["update_id"] + 1

    return results


def main():
    load_dotenv()
    conn = get_connection(DB_PATH)
    init_db(conn)
    sources_config = load_json("config/sources.json")
    blacklist = load_json("config/blacklist.json")
    profile = load_json("data/master_profile.json")
    notifier = TelegramNotifier(
        bot_token=os.environ["TELEGRAM_BOT_TOKEN"],
        chat_id=os.environ["TELEGRAM_CHAT_ID"],
    )

    while True:
        run_cycle(conn, sources_config, blacklist, profile, notifier)
        time.sleep(60)


if __name__ == "__main__":
    main()
