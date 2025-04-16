import glob
import polars as pl

from log_processing_toolbox.enrichment import ip
from log_processing_toolbox.source.application import cobalt_strike_team


def main():
    files = glob.glob("data/logs/*/events.log")
    print(files)

    # source
    df = cobalt_strike_team.open_events_log(files)

    print(df)

    # enrichment
    df = ip.city_information(df)
    df = ip.country_information(df)
    df = ip.asn_information(df)
    df = ip.is_public(df)
    df = ip.is_global(df)

    print(df)


if __name__ == "__main__":
    main()
