import polars as pl
from polars_geodesic_distance import distance


THEORETICAL_SPEED_REQUIRED_THRESHOLD= 100


def impossible_travel(df: pl.DataFrame, ip_column: str = "id.orig_h"):
    # TODO:

    # groupby user and shift to get login a and login b and time a and time b

    # compute distance from login a ip to login b ip
    df = df.with_columns(distance=distance("latitude_a", "longitude_a", "latitude_b", "longitude_b"))

    df = df.with_columns(
        (pl.col("timestamp_b") - pl.col("timestamp_a")).alias("time_between_logins")
    )

    # compute theoretical speed required to travel from point a to point b    
    
    # apply threshold to get impossible travel logins
    df = df.filter(
        pl.col("theoretical_speed") <= THEORETICAL_SPEED_REQUIRED_THRESHOLD
    )

    return df


def brute_force_detection(
    df: pl.DataFrame, timeout: int = 30, limit: int = 30
) -> pl.DataFrame:
    df_brute_force = (
        df.group_by_dynamic("ts", group_by="id.orig_h", every=f"{timeout}m")
        .agg(
            pl.col("auth_success").count().alias("number_of_attempts"),
            pl.col(
                "id.resp_h",
                "id.resp_p",
                "client",
                "server",
                "cipher_alg",
                "mac_alg",
                "compression_alg",
                "kex_alg",
                "host_key_alg",
                "host_key",
            ).first(),
        )
        .filter(pl.col("number_of_attempts") >= limit)
        .group_by("id.orig_h")
        .agg(
            pl.col("number_of_attempts").sum(),
            pl.col(
                "id.resp_h",
                "id.resp_p",
                "client",
                "server",
                "cipher_alg",
                "mac_alg",
                "compression_alg",
                "kex_alg",
                "host_key_alg",
                "host_key",
            ).first(),
        )
    )

    return df_brute_force
