import re
import polars as pl

from cysystemd.reader import JournalReader, Rule


openvpn_log_regex = re.compile(
    r"(?P<date>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\svpn\sopenvpn\S* \S*\/(?P<ip>\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}):(?P<port>\d*).*"
)   

rules = (
  Rule("SYSLOG_IDENTIFIER", "openvpn")
)

def cast_columns(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("ts").str.to_datetime("%Y-%m-%d %H:%M:%S"),
    )

def create_empty_log_collection():
    return {
        "ts": [],
        "id.orig_h": [],
        "id.orig_p": [],
    }


def add_entry(log_collection, x):
    log_collection["id.orig_h"].append(x["ip"])
    log_collection["id.orig_p"].append(x["port"])
    log_collection["ts"].append(x["date"])


def constuct_log_string(record) -> str:
    print(record.date.tzinfo)
    return f"{record.date.strftime('%Y-%m-%d %H:%M:%S')} {record.data['_HOSTNAME']} {record.data['SYSLOG_IDENTIFIER']}[{record.data['_PID']}]: {record.data['MESSAGE']}"


def open_journal_log(directory: str) -> pl.DataFrame:
    log_collection = create_empty_log_collection()

    journal_reader = JournalReader()
    journal_reader.open_directory(directory)

    journal_reader.add_filter(rules)

    for record in journal_reader:
        try:
            try:
                x = openvpn_log_regex.match(constuct_log_string(record))
                if x is not None:
                    add_entry(log_collection, x)
            except TypeError:
                pass
        except KeyError:
            pass

    df = pl.DataFrame(log_collection)

    df = cast_columns(df)

    return df
