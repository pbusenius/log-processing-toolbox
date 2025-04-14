import polars as pl

from log_processing_toolbox.enrichment import ip
from log_processing_toolbox.source.os import ssh as ssh_source
from log_processing_toolbox.analysis import ssh as ssh_analysis


def main():
    ssh_df = ssh_source.open_log("data/access_success.log")

    # enrichment
    ssh_df = ip.location_information(ssh_df)

    # filter
    ssh_df = ssh_df.filter(
        pl.col("auth_success") == "T"
    )

    ssh_df = ssh_analysis.impossible_travel(ssh_df, "user_name")

    print(ssh_df)


if __name__ == "__main__":
    main()