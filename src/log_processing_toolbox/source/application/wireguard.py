import re
import polars as pl

from cysystemd.reader import JournalReader, Rule


openvpn_log_regex = re.compile(r"")

rules = Rule("SYSLOG_IDENTIFIER", "wireguard")


def cast_columns(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("ts").str.to_datetime("%Y-%m-%d %H:%M:%S"),
    )


def create_empty_log_collection():
    return {"ts": [], "id.orig_h": [], "id.orig_p": [], "client": []}


def add_entry(log_collection, x):
    log_collection["id.orig_h"].append(x["ip"])
    log_collection["id.orig_p"].append(x["port"])
    log_collection["ts"].append(x["date"])
    log_collection["client"].append(x["client"])


def constuct_log_string(record) -> str:
    return f"{record.date.strftime('%Y-%m-%d %H:%M:%S')} {record.data['_HOSTNAME']} {record.data['SYSLOG_IDENTIFIER']}[{record.data['_PID']}]: {record.data['MESSAGE']}"


def open_journal_log(directory: str) -> pl.DataFrame:
    log_collection = create_empty_log_collection()

    journal_reader = JournalReader()
    journal_reader.open_directory(directory)

    journal_reader.add_filter(rules)

    for record in journal_reader:
        try:
            try:
                log_string = constuct_log_string(record)
                x = openvpn_log_regex.match(log_string)
                if x is not None:
                    add_entry(log_collection, x)
            except TypeError:
                pass
        except KeyError:
            pass

    df = pl.DataFrame(log_collection)

    df = cast_columns(df)

    return df
