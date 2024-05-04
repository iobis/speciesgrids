import pandas as pd


def read_worms() -> pd.DataFrame:
    df = pd.read_csv("worms/WoRMS_OBIS/taxon.txt", sep="\t")
    df = df[["taxonID", "acceptedNameUsageID", "scientificName", "acceptedNameUsage", "taxonRank"]]
    df["taxonRank"] = df["taxonRank"].str.lower()
    df = df[df["taxonRank"] == "species"]
    return df


def read_worms_matching() -> pd.DataFrame:
    df = pd.read_csv("worms/match-dataset-2011.tsv", sep="\t", dtype={"ID": "Int64"})
    df = df[df["rank"] == "species"]
    df = df[["inputID", "inputName", "ID", "scientificName", "matchType"]]
    return df


def read_profiles() -> pd.DataFrame:
    df = pd.read_csv("worms/WoRMS_OBIS/speciesprofile.txt", sep="\t")
    df["marine"] = df["isMarine"].eq(1)
    df = df[["taxonID", "marine"]]
    return df

def worms_to_parquet():

    worms = read_worms()
    worms_matching = read_worms_matching()
    profiles = read_profiles()

    df = worms_matching[["ID", "inputID"]]
    df = df.merge(worms[["taxonID", "acceptedNameUsageID"]], left_on="inputID", right_on="taxonID")[["ID", "acceptedNameUsageID"]]
    df = df.merge(worms[["taxonID", "taxonRank", "scientificName"]], left_on="acceptedNameUsageID", right_on="taxonID")[["ID", "acceptedNameUsageID", "scientificName", "taxonRank"]]
    df = df[df["taxonRank"] == "species"]
    df = df.merge(profiles, left_on="acceptedNameUsageID", right_on="taxonID")[["ID", "acceptedNameUsageID", "scientificName", "marine"]]
    df["AphiaID"] = df.acceptedNameUsageID.str.extract("(\d+)")
    df = df[df["marine"]][["ID", "AphiaID", "scientificName"]]
    df.to_parquet("worms/worms_mapping.parquet", index=False)
