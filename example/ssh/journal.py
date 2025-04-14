import polars as pl
from log_processing_toolbox.source.os import ssh as ssh_source


def main():
    ssh_df = ssh_source.open_journal_log("data/journal")

    # export
    ssh_df.write_csv("ip_ssh.csv")

    # filter
    ssh_df = ssh_df.filter(pl.col("auth_success") == "T")


if __name__ == "__main__":
    main()
