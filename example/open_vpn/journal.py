import polars as pl
from log_processing_toolbox.enrichment import ip

from log_processing_toolbox.source.application import open_vpn as open_vpn_source


def main():
    vpn_df = open_vpn_source.open_journal_log(
        "/media/pbusenius/ShadowCape/tmp/openvpn_logs/log/journal"
    )

    # enrichment
    vpn_df = ip.city_information(vpn_df)
    vpn_df = ip.country_information(vpn_df)
    vpn_df = ip.asn_information(vpn_df)

    vpn_df.write_csv("open_vpn.csv")

    vpn_df = vpn_df.group_by(["id.orig_h", "client"]).agg(
        pl.col("ts").count().alias("number_of_log_entries"),
        pl.col("ts").first().alias("first_timestamp"),
        pl.col("ts").last().alias("last_timestamp"),
        pl.col("city_information").first(),
        pl.col("country_information").first(),
        pl.col("asn_information").first(),
    )

    # export
    vpn_df.write_csv("openvpn_ips.csv")


if __name__ == "__main__":
    main()
