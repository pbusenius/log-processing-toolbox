import polars as pl
from log_processing_toolbox.source.os import ssh as ssh_source


def main():
    df = ssh_source.open_journal_log("/media/pbusenius/One Touch/logs/146.70.101.118/journal")

    print(df)



if __name__ == "__main__":
    main()