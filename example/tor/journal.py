import polars as pl
from log_processing_toolbox.source.os import tor as tor_source


def main():
    tor_df = tor_source.open_journal_log("data/journal")


if __name__ == "__main__":
    main()
