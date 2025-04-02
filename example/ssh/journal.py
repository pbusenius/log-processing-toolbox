import polars as pl
from log_processing_toolbox.source.os import ssh as ssh_source


def main():
    ssh_df = ssh_source.open_journal_log("/media/pbusenius/One Touch/logs/146.70.101.118/journal")

    # filter
    ssh_df = ssh_df.filter(
        pl.col("auth_success") == "T"
    )

    print(ssh_df)



if __name__ == "__main__":
    main()