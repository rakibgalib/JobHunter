import json
import logging
import random
import sys
import time
import os

from dotenv import load_dotenv

from src.db import get_connection, init_db, DB_PATH
from src.ingestion.health import record_success, record_failure, is_disabled, should_poll, clear_disabled
from src.ingestion.we_work_remotely import WeWorkRemotelyAdapter
from src.ingestion.remoteok import RemoteOKAdapter
from src.ingestion.remotive import RemotiveAdapter
from src.ingestion.nodesk import NoDeskAdapter
from src.ingestion.flexjobs import FlexJobsAdapter
from src.ingestion.linkedin import LinkedInAdapter
from src.notifier.telegram import TelegramNotifier
from src.notifier.callbacks import apply_callback
from src.orchestrator import process_job, resurface_snoozed, retry_unalerted

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
              adapter_classes: dict = DEFAULT_ADAPTER_CLASSES, callback_offset=None) -> tuple:
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
            logging.error(f"fetch failed for {source}: {exc}")
            just_disabled = record_failure(conn, source)
            if just_disabled:
                try:
                    notifier.send_health_alert(source, str(exc))
                except Exception as alert_exc:
                    logging.error(f"send_health_alert failed for {source}: {alert_exc}")
            continue

        if not postings:
            just_disabled = record_failure(conn, source)
            if just_disabled:
                try:
                    notifier.send_health_alert(source, "fetch returned no results")
                except Exception as alert_exc:
                    logging.error(f"send_health_alert failed for {source}: {alert_exc}")
            results[source] = []
            continue

        record_success(conn, source)
        outcomes = []
        for job in postings:
            try:
                outcomes.append(process_job(conn, job, blacklist, profile, notifier))
            except Exception as exc:
                logging.error(f"process_job failed for {source} job {job.source_job_id}: {exc}")
        results[source] = outcomes

    try:
        resurface_snoozed(conn, notifier)
    except Exception as exc:
        logging.error(f"resurface_snoozed failed: {exc}")

    try:
        retry_unalerted(conn, notifier)
    except Exception as exc:
        logging.error(f"retry_unalerted failed: {exc}")

    offset = callback_offset
    for update in notifier.get_callback_updates(offset):
        offset = update["update_id"] + 1
        callback = update.get("callback_query")
        if callback:
            try:
                apply_callback(conn, callback["data"])
            except Exception as exc:
                logging.error(f"apply_callback failed for {callback['data']}: {exc}")

    return results, offset


def enable_source(source: str) -> None:
    conn = get_connection(DB_PATH)
    init_db(conn)
    row = conn.execute(
        "SELECT 1 FROM source_health WHERE source = ?", (source,)
    ).fetchone()
    if row is None:
        print(f"Unknown source '{source}': no source_health record found.")
        sys.exit(1)
    clear_disabled(conn, source)
    print(f"Source '{source}' re-enabled.")


def main():
    logging.basicConfig(
        filename="agent.log",
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
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

    offset = None
    while True:
        _, offset = run_cycle(conn, sources_config, blacklist, profile, notifier, callback_offset=offset)
        time.sleep(60)


if __name__ == "__main__":
    if len(sys.argv) >= 3 and sys.argv[1] == "--enable":
        enable_source(sys.argv[2])
    else:
        main()
