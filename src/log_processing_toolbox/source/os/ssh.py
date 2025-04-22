import re
import polars as pl

from cysystemd.reader import JournalReader, Rule

auth_log_regex = re.compile(
    r"(?P<date>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s(?P<user>\S*)\s(?P<service>\S*\s)(?P<type>Disconnected from invalid user (?P<disconnected_remote_user>\S*) |Invalid user (?P<invalid_remote_user>\S*) from |Received disconnect from |Accepted publickey for (?P<accepted_public_key_remote_user>\S*) from |Failed password for invalid user (?P<invalid_user_failed_password>\S*) from |Failed password for (?P<failed_password_user>\S*) from |Accepted password for (?P<accepted_password_remote_user>\S*) from )(?P<ip>\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}) port (?P<port>\d*)( ssh2(: RSA (?P<rsa>SHA256:\S*))?)?"
)

rules = Rule("SYSLOG_IDENTIFIER", "sshd")


def cast_columns(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("ts").str.to_datetime("%Y-%m-%d %H:%M:%S"),
    )


def extract_user_name(x) -> str:
    user_name = ""

    if x["disconnected_remote_user"] != None:
        user_name = x["disconnected_remote_user"]
    elif x["invalid_remote_user"] != None:
        user_name = x["invalid_remote_user"]
    elif x["accepted_public_key_remote_user"] != None:
        user_name = x["accepted_public_key_remote_user"]
    elif x["invalid_user_failed_password"] != None:
        user_name = x["invalid_user_failed_password"]
    elif x["failed_password_user"] != None:
        user_name = x["failed_password_user"]

    return user_name


def create_empty_log_collection():
    return {
        "ts": [],
        "id.orig_h": [],
        "id.orig_p": [],
        "id.resp_h": [],
        "id.resp_p": [],
        "user_name": [],
        "auth_success": [],
        "client": [],
        "server": [],
        "cipher_alg": [],
        "mac_alg": [],
        "compression_alg": [],
        "kex_alg": [],
        "host_key_alg": [],
        "host_key": [],
    }


def add_entry(log_collection, x):
    if "Accepted" in x["type"]:
        log_collection["auth_success"].append("T")
    else:
        log_collection["auth_success"].append("F")

    log_collection["user_name"].append(extract_user_name(x))

    log_collection["id.resp_h"].append(x["user"])
    log_collection["id.orig_h"].append(x["ip"])
    log_collection["id.orig_p"].append(x["port"])
    log_collection["ts"].append(x["date"])
    log_collection["id.resp_p"].append("XXXX")
    log_collection["client"].append("XXXX")
    log_collection["server"].append("XXXX")
    log_collection["cipher_alg"].append("XXXX")
    log_collection["mac_alg"].append("XXXX")
    log_collection["compression_alg"].append("XXXX")
    log_collection["kex_alg"].append("XXXX")
    log_collection["host_key_alg"].append("XXXX")
    log_collection["host_key"].append("XXXX")


def open_log(file: str) -> pl.DataFrame:
    log_collection = create_empty_log_collection()

    with open(file, "r") as file:
        lines = file.readlines()
        for line in lines:
            x = auth_log_regex.match(line)
            if x is not None:
                add_entry(log_collection, x)

    df = pl.DataFrame(log_collection)

    df = cast_columns(df)

    return df


def constuct_log_string(record) -> str:
    return f"{record.date.strftime('%Y-%m-%d %H:%M:%S')} {record.data['_HOSTNAME']} {record.data['SYSLOG_IDENTIFIER']}[{record.data['_PID']}]: {record.data['MESSAGE']}"


def open_journal_log(directory: str) -> pl.DataFrame:
    log_collection = create_empty_log_collection()

    journal_reader = JournalReader()
    journal_reader.open_directory(directory)

    journal_reader.add_filter(rules)

    for record in journal_reader:
        try:
            x = auth_log_regex.match(constuct_log_string(record))
            if x is not None:
                add_entry(log_collection, x)
        except (TypeError, KeyError):
            pass

    df = pl.DataFrame(log_collection)

    df = cast_columns(df)

    return df
