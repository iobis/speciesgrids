import pandas as pd
import logging


class WormsBuilder:

    def read_worms_species(self) -> pd.DataFrame:
        logging.info(f"Reading WoRMS taxon file from {self.worms_taxon_path}")
        df = pd.read_csv(self.worms_taxon_path, sep="\t")
        df = df[["taxonID", "acceptedNameUsageID", "scientificName", "acceptedNameUsage", "taxonRank"]]
        df["taxonRank"] = df["taxonRank"].str.lower()
        df = df[df["taxonRank"] == "species"]
        return df

    def read_worms_taxonomy(self) -> pd.DataFrame:
        logging.info(f"Reading WoRMS taxon file from {self.worms_taxon_path}")
        df = pd.read_csv(self.worms_taxon_path, sep="\t")
        df = df[df["scientificNameID"] == df["acceptedNameUsageID"]]
        df["AphiaID"] = df.acceptedNameUsageID.str.extract("(\\d+)")
        df["AphiaID"] = df["AphiaID"].astype("Int64")
        df["taxonRank"] = df["taxonRank"].str.lower()
        df.loc[df["taxonRank"] == "species", "species"] = df["scientificName"]
        df = df[["AphiaID", "kingdom", "phylum", "class", "order", "family", "genus", "species"]]
        return df

    def read_worms_matching(self) -> pd.DataFrame:
        logging.info(f"Reading WoRMS matching file from {self.worms_matching_path}")
        df = pd.read_csv(self.worms_matching_path, sep="\t", dtype={"ID": "Int64"})
        df = df[df["rank"] == "species"]
        df = df[["inputID", "inputName", "ID", "scientificName", "matchType"]]
        return df

    def read_profiles(self) -> pd.DataFrame:
        logging.info(f"Reading WoRMS species profile file from {self.worms_profile_path}")
        df = pd.read_csv(self.worms_profile_path, sep="\t")
        df["marine"] = df["isMarine"].eq(1)
        df = df[["taxonID", "marine"]]
        return df

    def read_redlist(self) -> pd.DataFrame:
        logging.info(f"Reading Red List file from {self.worms_redlist_path}")
        df = pd.read_csv(self.worms_redlist_path, sep="\t")
        return df

    def worms_to_parquet(self):

        # GBIF to WoRMS mapping

        worms = self.read_worms_species()
        worms_matching = self.read_worms_matching()
        profiles = self.read_profiles()

        df = worms_matching[["ID", "inputID"]]
        df = df.merge(worms[["taxonID", "acceptedNameUsageID"]], left_on="inputID", right_on="taxonID")[["ID", "acceptedNameUsageID"]]
        df = df.merge(worms[["taxonID", "taxonRank", "scientificName"]], left_on="acceptedNameUsageID", right_on="taxonID")[["ID", "acceptedNameUsageID", "scientificName", "taxonRank"]]
        df = df[df["taxonRank"] == "species"]
        df = df.merge(profiles, left_on="acceptedNameUsageID", right_on="taxonID")[["ID", "acceptedNameUsageID", "scientificName", "marine"]]
        df["AphiaID"] = df.acceptedNameUsageID.str.extract("(\\d+)")
        df = df[df["marine"]][["ID", "AphiaID", "scientificName"]]
        logging.info(f"Writing WoRMS mapping for {len(df)} species to {self.worms_mapping_output_path}")
        df.to_parquet(self.worms_mapping_output_path, index=False)

        # accepted taxonomy

        taxonomy = self.read_worms_taxonomy()
        taxonomy.to_parquet(self.worms_taxonomy_output_path, index=False)
