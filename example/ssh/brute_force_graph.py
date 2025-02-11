from log_processing_toolbox.analysis import ssh
from log_processing_toolbox.source.os import ssh as ssh_zeek_source
from log_processing_toolbox.enrichment import ip


def main():
    # source
    zeek_df = ssh_zeek_source.open_log("data/auth.log")

    # analysis
    df_brute_force = ssh.brute_force_detection(zeek_df)
    print(df_brute_force)

    # enrichment
    df_brute_force = ip.city_information(df_brute_force, ip_column="id.resp_h")
    df_brute_force = ip.country_information(df_brute_force, ip_column="id.resp_h")
    df_brute_force = ip.asn_information(df_brute_force, ip_column="id.resp_h")
    df_brute_force = ip.location_information(df_brute_force, ip_column="id.resp_h")

    # visualization#
    print(df_brute_force)


if __name__ == "__main__":
    main()
