import re
import polars as pl

from cysystemd.reader import JournalReader, Rule

sftp_regex = re.compile(
    r"(?P<date>\S*\s*\d*\s\d{2}:\d{2}:\d{2})\s(?P<server_ip>\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3})\s(?P<user>\S*)\sStarting\ssession.\ssubsystem\s\'sftp\'\sfor\s\S*\sfrom\s(?P<from_ip>\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3})\sport\s(?P<from_port>\d*)\sid\s\d*"
)

rules = Rule("SYSLOG_IDENTIFIER", "sshd")


def cast_columns(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("ts").str.to_datetime("%Y-%m-%d %H:%M:%S"),
    )


def create_empty_log_collection():
    return {"ts": [], "id.orig_h": [], "id.orig_p": [], "id.resp_h": [], "user": []}


def add_entry(log_collection, x):
    log_collection["id.resp_h"].append(x["server_ip"])
    log_collection["id.orig_h"].append(x["from_ip"])
    log_collection["id.orig_p"].append(x["from_port"])
    log_collection["ts"].append(x["date"])
    log_collection["user"].append(x["user"])


def open_log(file: str) -> pl.DataFrame:
    log_collection = create_empty_log_collection()

    with open(file, "r") as file:
        lines = file.readlines()
        for line in lines:
            x = sftp_regex.match(line)
            if x is not None:
                add_entry(log_collection, x)

    df = pl.DataFrame(log_collection)

    # df = cast_columns(df)

    return df
