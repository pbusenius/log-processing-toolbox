import polars as pl

from log_processing_toolbox.enrichment import ip
from log_processing_toolbox.source.os import ssh as ssh_os_source


def main():
    # source
    ssh_df = ssh_os_source.open_log("data/auth.log")

    print(ssh_df)

    # enrichment
    ssh_df = ip.city_information(ssh_df)
    ssh_df = ip.country_information(ssh_df)
    ssh_df = ip.asn_information(ssh_df)

    # filter
    ssh_df = ssh_df.filter(
        pl.col("auth_success") == "T"
    )

    print(ssh_df)


if __name__ == "__main__":
    main()


