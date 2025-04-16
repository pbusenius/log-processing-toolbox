import re
import polars as pl

from cysystemd.reader import JournalReader

events_log_regex = re.compile(
    r"(?P<datetime>\d{2}/\d{2}\s\d{2}:\d{2}:\d{2}\sUTC)\s(?P<unknown>\*\*\*)\s(?P<name>\S*)\s\((?P<ip>\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3})\)\s(?P<command>\S*)"
)


def cast_columns(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("ts").str.to_datetime("%Y %b  %d %H:%M:%S"),
    )

def create_empty_log_collection():
    return {
        "ts": [],
        "id.orig_h": [],
        "name": [],
        "command": []
    }


def add_event_entry(log_collection, x, year: str):
    log_collection["id.orig_h"].append(x["ip"])
    log_collection["ts"].append(f"{year} " + x["datetime"])
    log_collection["name"].append(x["name"])
    log_collection["command"].append(x["command"])

    return log_collection


def open_events_log(files) -> pl.DataFrame:
    log_collection = create_empty_log_collection()

    if isinstance(files, str):
        files = [files]

    for f in files:
        with open(f, "r") as file:
            lines = file.readlines()
            for line in lines:
                x = events_log_regex.match(line)
                if x is not None:
                    add_event_entry(log_collection, x, "2024")

    df = pl.DataFrame(log_collection)

    return df
