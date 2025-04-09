import re
import polars as pl

from cysystemd.reader import JournalReader


def create_empty_log_collection():
    return {
        "ts": [],
        "message": []
    }


def constuct_log_string(record) -> str:
    return f"{record.data['SYSLOG_TIMESTAMP']}{record.data['_HOSTNAME']} {record.data['SYSLOG_IDENTIFIER']}[{record.data['_PID']}]: {record.data['MESSAGE']}"


def open_journal_log(directory: str) -> pl.DataFrame:
    journal_reader = JournalReader()
    journal_reader.open_directory(directory)

    for record in journal_reader:
        try:
            if record.data["SYSLOG_IDENTIFIER"] == "Tor":
                try:
                    print(constuct_log_string(record))
                except TypeError:
                    pass
        except KeyError:
            pass
