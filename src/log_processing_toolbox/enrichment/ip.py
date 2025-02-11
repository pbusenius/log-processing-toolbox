import polars as pl
from polars_maxminddb import (
    ip_lookup_city,
    ip_lookup_country,
    ip_lookup_asn,
    ip_lookup_latitude,
    ip_lookup_longitude,
)


def city_information(df: pl.DataFrame, ip_column: str = "id.orig_h", location_file: str = "/home/pbusenius/github.com/pbusenius/log-processing-toolbox/data/GeoLite2-City.mmdb) -> pl.DataFrame"):
    return df.with_columns(
        ip_lookup_city(df[ip_column], location_file).alias(
            "city_information"
        )
    )


def location_information(
    df: pl.DataFrame, ip_column: str = "id.orig_h", location_file: str = "/home/pbusenius/github.com/pbusenius/log-processing-toolbox/data/GeoLite2-City.mmdb"
) -> pl.DataFrame:
    return df.with_columns(
        ip_lookup_longitude(df[ip_column], location_file).alias(
            "longitude"
        ),
        ip_lookup_latitude(df[ip_column], location_file).alias("latitude"),
    )


def country_information(df: pl.DataFrame, ip_column: str = "id.orig_h", location_file: str = "/home/pbusenius/github.com/pbusenius/log-processing-toolbox/data/GeoLite2-City.mmdb") -> pl.DataFrame:
    return df.with_columns(
        ip_lookup_country(df[ip_column], location_file).alias(
            "country_information"
        )
    )


def asn_information(df: pl.DataFrame, ip_column: str = "id.orig_h", location_file: str = "/home/pbusenius/github.com/pbusenius/log-processing-toolbox/data/GeoLite2-City.mmdb") -> pl.DataFrame:
    return df.with_columns(
        ip_lookup_asn(df[ip_column], location_file).alias("asn_information")
    )
