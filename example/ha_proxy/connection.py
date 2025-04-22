import polars as pl

from log_processing_toolbox.enrichment import ip
from log_processing_toolbox.source.application import ha_proxy


def main():
    # source
    connection_df = ha_proxy.open_log("data/haproxy.log")

    # enrichment
    connection_df = ip.city_information(connection_df)
    connection_df = ip.country_information(connection_df)
    connection_df = ip.asn_information(connection_df)

    print(connection_df)


if __name__ == "__main__":
    main()
