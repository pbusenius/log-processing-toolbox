import polars as pl
from log_processing_toolbox.source.os import ssh as ssh_source
from log_processing_toolbox.analysis import ssh as ssh_analysis


def main():
    ssh_df = ssh_source.open_journal_log("data/journal")

    ssh_df.write_csv("146.70.125.123_ssh.csv")

    # filter
    ssh_df = ssh_df.filter(
        pl.col("auth_success") == "T"
    )

    ssh_df = ssh_analysis.impossible_travel(ssh_df, "root")

    print(ssh_df)


if __name__ == "__main__":
    main()