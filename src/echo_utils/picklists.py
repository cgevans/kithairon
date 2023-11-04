import polars as pl


class PickList:
    df: pl.DataFrame

    def __init__(self, df: pl.DataFrame):
        self.df = df

    def __repr__(self):
        return repr(self.df)

    def __str__(self):
        return self.df.to_string()

    def from_csv(path: str):
        return PickList(pl.read_csv(path))

    def _totvols(self):
        return self.df.group_by(["Destination Plate Name", "Destination Well"]).agg(
            pl.col("Transfer Volume").sum().alias("total_volume")
        )

    def get_contents(
        self,
        plate: str | None = None,
        well: str | None = None,
        sample: str | None = None,
    ):
        if (plate is not None) and (well is None):
            if sample is not None:
                raise ValueError("Both plate and sample cannot be specified")
            else:
                sample = plate
                plate = None
        if (plate is not None) and (well is not None):
            transfers_to = self.df.filter(
                (pl.col("Destination Plate Name") == plate)
                & (pl.col("Destination Well") == well)
            )
        elif (plate is None) and (well is None) and (sample is not None):
            transfers_to = self.df.filter(pl.col("Destination Sample Name") == sample)
        else:
            raise ValueError("Invalid combination of arguments")

        totvols = self._totvols().lazy()

        # If transfers_to does not have a "Source Concentration" column, add one filled with nulls
        if "Source Concentration" not in transfers_to.columns:
            transfers_to = transfers_to.with_columns(
                pl.lit(None).cast(pl.Float32).alias("Source Concentration")
            )
            
        # Lazily add a Source Concentration column to self.df if there isn't one
        if "Source Concentration" not in self.df.columns:
            selfdf = self.df.with_columns(
                pl.lit(None).cast(pl.Float32).alias("Source Concentration")
            ).lazy()
        else:
            selfdf = self.df.lazy()

        transfers_to = transfers_to.lazy().join(
            totvols.lazy(),
            left_on=["Destination Plate Name", "Destination Well"],
            right_on=["Destination Plate Name", "Destination Well"],
            how="left",
        ).with_columns(
            (pl.col("Transfer Volume") / pl.col("total_volume")).alias("transfer_ratio"),
        ).with_columns(
            (pl.col("transfer_ratio") * pl.col("Source Concentration")).alias("Destination Concentration")
        )

        maybe_intermediates = True
        while maybe_intermediates:
            any_transfers = transfers_to.join(
                selfdf,
                left_on=["Source Plate Name", "Source Well"],
                right_on=["Destination Plate Name", "Destination Well"],
                how="inner",
                suffix=" int",
            ).collect()
            if len(any_transfers) == 0:
                break
            
            transfers_to = transfers_to.join(
                selfdf,
                left_on=["Source Plate Name", "Source Well"],
                right_on=["Destination Plate Name", "Destination Well"],
                how="left",
                suffix="_int",
            ).join(
                totvols,
                left_on=["Source Plate Name", "Source Well"],
                right_on=["Destination Plate Name", "Destination Well"],
                how="left",
            ).with_columns(
                pl.when(pl.col("Source Well_int").is_not_null()).then((pl.col("transfer_ratio") * pl.col("Transfer Volume_int") / pl.col("total_volume_right"))).otherwise(pl.col("transfer_ratio")).alias("transfer_ratio"),
                pl.when(pl.col("Source Well_int").is_not_null()).then(pl.col("Source Concentration_int")).otherwise(pl.col("Source Concentration")).alias("Source Concentration"),
                pl.when(pl.col("Source Well_int").is_not_null()).then(pl.col("Sample Name_int")).otherwise(pl.col("Sample Name")).alias("Sample Name"),
                pl.when(pl.col("Source Well_int").is_not_null()).then(pl.col("Source Plate Name_int")).otherwise(pl.col("Source Plate Name")).alias("Source Plate Name"),
            ).with_columns(
                pl.when(pl.col("Source Well_int").is_not_null()).then(pl.col("transfer_ratio") * pl.col("Source Concentration")).otherwise(pl.col("Destination Concentration")).alias("Destination Concentration"),
                pl.when(pl.col("Source Well_int").is_not_null()).then(pl.col("Source Well_int")).otherwise(pl.col("Source Well")).alias("Source Well"),
            ).drop_nulls("Source Well").select(
                [
                    "Sample Name",
                    "Source Plate Name",
                    "Source Well",
                    "Source Concentration",
                    "Destination Concentration",
                    "transfer_ratio"
                ]
            )
            

        return transfers_to.select(
            [
                "Sample Name",
                "Source Plate Name",
                "Source Well",
                "Source Concentration",
                "Destination Concentration",
                "transfer_ratio"
            ]
        ).collect()