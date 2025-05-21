from log_processing_toolbox.enrichment import ip
from log_processing_toolbox.source.os import sftp as sftp_source


def main():
    df = sftp_source.open_log("data/sftp.log")

    # enrichment
    df = ip.location_information(df)

    print(df)


if __name__ == "__main__":
    main()
