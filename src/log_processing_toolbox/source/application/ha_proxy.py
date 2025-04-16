import re
import polars as pl

auth_log_regex = re.compile(
    r"(?P<date>\S*\s*\d*\s\d{2}:\d{2}:\d{2})\s(?P<user>\S*)\s(?P<service>\S*)\sConnect\sfrom\s(?P<from_ip>\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}):(?P<from_port>\d*)\sto\s(?P<to_ip>\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}):(?P<to_port>\d*)"
)


def cast_columns(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("ts").str.to_datetime("%Y %b  %d %H:%M:%S"),
    )


def open_log(file: str) -> pl.DataFrame:
    data = {
        "ts": [],
        "id.orig_h": [],
        "id.orig_p": [],
        "id.resp_h": [],
        "id.resp_p": [],
    }

    with open(file, "r") as file:
        lines = file.readlines()
        for line in lines:
            x = auth_log_regex.match(line)
            if x is not None:
                data["id.resp_h"].append(x["to_ip"])
                data["id.resp_p"].append(x["to_port"])
                data["id.orig_h"].append(x["from_ip"])
                data["id.orig_p"].append(x["from_port"])
                data["ts"].append("2024" + x["date"])

    df = pl.DataFrame(data)

    df = cast_columns(df)

    return df
