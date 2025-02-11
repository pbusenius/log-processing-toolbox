import matplotlib
import polars as pl
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

matplotlib.rc("font", size=6)
plt.style.use("bmh")


BYTE_TO_GIGABYTE_MODIFIER = 1e-9


def plot_data_transfer_over_time(df: pl.DataFrame, out_file: str, y_lim):
    df = df.with_columns(
        pl.col("timestamp").dt.to_string("%d.%m-%H:%M").alias("timestamp_string")
    )

    ax = sns.lineplot(
        df,
        x="timestamp_string",
        y="tx",
    )

    tickMultiplier = round(len(df) / 7)

    ax.xaxis.set_major_locator(ticker.MultipleLocator(tickMultiplier))

    ax.set(ylabel="Datentransfer GB/h", xlabel="Zeitstempel")

    ax.set_ylim(y_lim)

    plt.savefig(out_file, dpi=1000)

    plt.clf()


def plot_conn_transfer_over_time(df: pl.DataFrame, out_file: str):
    # TODO: add two plot for tx and rx and link them
    df = (
        df.group_by_dynamic("ts", every="1m")
        .agg(
            pl.col("orig_ip_bytes").sum() / BYTE_TO_GIGABYTE_MODIFIER,
            pl.col("resp_ip_bytes").sum() / BYTE_TO_GIGABYTE_MODIFIER,
        )
        .with_columns(
            pl.col("ts").dt.to_string("%d.%m-%H:%M").alias("timestamp_string")
        )
    )

    df = df["timestamp_string", "orig_ip_bytes", "resp_ip_bytes"]

    # add orig_ip line
    ax = sns.lineplot(
        df,
        x="timestamp_string",
        y="orig_ip_bytes",
    )

    # add resp_ip line
    ax = sns.lineplot(
        df,
        x="timestamp_string",
        y="resp_ip_bytes",
    )

    tickMultiplier = round(len(df) / 7)

    ax.xaxis.set_major_locator(ticker.MultipleLocator(tickMultiplier))

    ax.set(ylabel="Datentransfer GB/h", xlabel="Zeitstempel")

    plt.show()
