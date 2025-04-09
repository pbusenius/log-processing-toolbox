import polars as pl


THEORETICAL_SPEED_REQUIRED_THRESHOLD= 100


def impossible_travel(df: pl.DataFrame, ip_column: str = "id.orig_h"):
    # TODO:
    # compute distance from login a ip to login b ip
    # compute theoretical speed required to travel from point a to point b
    # apply threshold to get impossible travel logins

    df = df.filter(
        pl.col("theoretical_speed") <= THEORETICAL_SPEED_REQUIRED_THRESHOLD
    )

    return df
