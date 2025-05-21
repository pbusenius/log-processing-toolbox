import os
import requests
import ipaddress
import polars as pl
from polars_maxminddb import (
    ip_lookup_city,
    ip_lookup_country,
    ip_lookup_asn,
    ip_lookup_latitude,
    ip_lookup_longitude,
)
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")

def city_information(
    df: pl.DataFrame,
    ip_column: str = "id.orig_h",
    location_file: str = "data/GeoLite2-City.mmdb",
) -> pl.DataFrame:
    return df.with_columns(
        ip_lookup_city(df[ip_column], location_file).alias("city_information")
    )


def location_information(
    df: pl.DataFrame,
    ip_column: str = "id.orig_h",
    location_file: str = "data/GeoLite2-City.mmdb",
) -> pl.DataFrame:
    return df.with_columns(
        ip_lookup_longitude(df[ip_column], location_file).alias("longitude"),
        ip_lookup_latitude(df[ip_column], location_file).alias("latitude"),
    )


def country_information(
    df: pl.DataFrame,
    ip_column: str = "id.orig_h",
    location_file: str = "data/GeoLite2-Country.mmdb",
) -> pl.DataFrame:
    return df.with_columns(
        ip_lookup_country(df[ip_column], location_file).alias("country_information")
    )


def asn_information(
    df: pl.DataFrame,
    ip_column: str = "id.orig_h",
    location_file: str = "data/GeoLite2-ASN.mmdb",
) -> pl.DataFrame:
    return df.with_columns(
        ip_lookup_asn(df[ip_column], location_file).alias("asn_information")
    )


def is_public(df: pl.DataFrame, ip_column: str = "id.orig_h"):
    return df.with_columns(
        pl.col(ip_column)
        .map_elements(
            lambda x: ipaddress.ip_address(x).is_private, return_dtype=pl.Boolean
        )
        .alias("is_private")
    )


def is_global(df: pl.DataFrame, ip_column: str = "id.orig_h"):
    return df.with_columns(
        pl.col(ip_column)
        .map_elements(
            lambda x: ipaddress.ip_address(x).is_global, return_dtype=pl.Boolean
        )
        .alias("is_global")
    )


def vpn_api_call(ip: str, key: str) -> bool:
    response = requests.get(f"https://vpnapi.io/api/{ip}?key={API_KEY}")

    if response.status_code == 200:
        body = response.json()

        return body["security"][key]
    
    return False


def is_vpn(df: pl.DataFrame, ip_column: str = "id.orig_h"):
    return df.with_columns(
        pl.col(ip_column)
        .map_elements(
            lambda x: vpn_api_call(x, "vpn"), return_dtype=pl.Boolean
        )
        .alias("is_vpn")
    )
    pass


def is_tor(df: pl.DataFrame, ip_column: str = "id.orig_h"):
    return df.with_columns(
        pl.col(ip_column)
        .map_elements(
            lambda x: vpn_api_call(x, "tor"), return_dtype=pl.Boolean
        )
        .alias("is_tor")
    )
    pass
