from typing import Sequence

import polars as pl

from kithairon.labware import Labware
from kithairon.surveys import Survey


class PickList:
    df: pl.DataFrame

    def __init__(self, df: pl.DataFrame):
        self.df = df

    def __repr__(self):
        return repr(self.df)

    def __str__(self):
        return str(self.df)

    def _repr_html_(self):
        return self.df._repr_html_()

    @classmethod
    def from_csv(cls, path: str):
        return cls(pl.read_csv(path))

    def to_csv(self, path: str):
        self.df.write_csv(path)

    def _totvols(self):
        return self.df.group_by(["Destination Plate Name", "Destination Well"]).agg(
            pl.col("Transfer Volume").sum().alias("total_volume")
        )

    def validate(
        self,
        labware: Labware | None = None,
        surveys: Sequence[Survey] | None = None,
        raise_on_error: bool = True,
    ) -> Sequence[str]:
        errors = []

        # Check that every appearance of a Plate Name has the same Plate Type
        dest_plate_types_per_name = (
            self.df.lazy()
            .group_by("Destination Plate Name")
            .agg(pl.col("Destination Plate Type").unique().alias("plate_types"))
            .with_columns(pl.col("plate_types").list.lengths().alias("n_plate_types"))
            .filter(pl.col("n_plate_types") > 1)
            .select("Source Plate Name", "plate_types")
            .collect()
        )

        if len(dest_plate_types_per_name) > 0:
            print("Plate Name appears with multiple Plate Types:")
            print(dest_plate_types_per_name)

        src_plate_types_per_name = (
            self.df.lazy()
            .group_by("Source Plate Name")
            .agg(pl.col("Source Plate Type").unique().alias("plate_types"))
            .with_columns(pl.col("plate_types").list.lengths().alias("n_plate_types"))
            .filter(pl.col("n_plate_types") > 1)
            .select("Source Plate Name", "plate_types")
            .collect()
        )

        if len(src_plate_types_per_name) > 0:
            print("Plate Name appears with multiple Plate Types:")
            print(src_plate_types_per_name)

        if labware is not None:
            labware_df = labware.to_polars()

            p_with_lb = (
                self.df.lazy()
                .join(
                    labware_df.lazy(),
                    left_on="Source Plate Name",
                    right_on="platetype",
                    how="left",
                )
                .join(
                    labware_df.lazy(),
                    left_on="Destination Plate Name",
                    right_on="platetype",
                    how="left",
                    suffix="_dest",
                )
            )

            wrongvolume = (
                p_with_lb.with_columns(
                    tx_mod=(pl.col("Transfer Volume") % pl.col("dropvolume"))
                )
                .filter(pl.col("tx_mod") != 0)
                .collect()
            )

            if len(wrongvolume) > 0:
                print("Transfer volumes are not multiples of drop volume:")
                print(wrongvolume)
                errors.append("Transfer volumes are not multiples of drop volume")

    def get_contents(
        self,
        plate: str | None = None,
        well: str | None = None,
        name: str | None = None,
    ):
        if (plate is not None) and (well is None):
            if name is not None:
                raise ValueError("Both plate and name cannot be specified")
            else:
                name = plate
                plate = None
        if (plate is not None) and (well is not None):
            transfers_to = self.df.filter(
                (pl.col("Destination Plate Name") == plate)
                & (pl.col("Destination Well") == well)
            )
        elif (plate is None) and (well is None) and (name is not None):
            transfers_to = self.df.filter(pl.col("Destination Sample Name") == name)
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

        transfers_to = (
            transfers_to.lazy()
            .join(
                totvols.lazy(),
                left_on=["Destination Plate Name", "Destination Well"],
                right_on=["Destination Plate Name", "Destination Well"],
                how="left",
            )
            .with_columns(
                (pl.col("Transfer Volume") / pl.col("total_volume")).alias(
                    "transfer_ratio"
                ),
            )
            .with_columns(
                (pl.col("transfer_ratio") * pl.col("Source Concentration")).alias(
                    "Destination Concentration"
                )
            )
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

            transfers_to = (
                transfers_to.join(
                    selfdf,
                    left_on=["Source Plate Name", "Source Well"],
                    right_on=["Destination Plate Name", "Destination Well"],
                    how="left",
                    suffix="_int",
                )
                .join(
                    totvols,
                    left_on=["Source Plate Name", "Source Well"],
                    right_on=["Destination Plate Name", "Destination Well"],
                    how="left",
                )
                .with_columns(
                    pl.when(pl.col("Source Well_int").is_not_null())
                    .then(
                        (
                            pl.col("transfer_ratio")
                            * pl.col("Transfer Volume_int")
                            / pl.col("total_volume_right")
                        )
                    )
                    .otherwise(pl.col("transfer_ratio"))
                    .alias("transfer_ratio"),
                    pl.when(pl.col("Source Well_int").is_not_null())
                    .then(pl.col("Source Concentration_int"))
                    .otherwise(pl.col("Source Concentration"))
                    .alias("Source Concentration"),
                    pl.when(pl.col("Source Well_int").is_not_null())
                    .then(pl.col("Sample Name_int"))
                    .otherwise(pl.col("Sample Name"))
                    .alias("Sample Name"),
                    pl.when(pl.col("Source Well_int").is_not_null())
                    .then(pl.col("Source Plate Name_int"))
                    .otherwise(pl.col("Source Plate Name"))
                    .alias("Source Plate Name"),
                )
                .with_columns(
                    pl.when(pl.col("Source Well_int").is_not_null())
                    .then(pl.col("transfer_ratio") * pl.col("Source Concentration"))
                    .otherwise(pl.col("Destination Concentration"))
                    .alias("Destination Concentration"),
                    pl.when(pl.col("Source Well_int").is_not_null())
                    .then(pl.col("Source Well_int"))
                    .otherwise(pl.col("Source Well"))
                    .alias("Source Well"),
                )
                .drop_nulls("Source Well")
                .select(
                    [
                        "Sample Name",
                        "Source Plate Name",
                        "Source Well",
                        "Source Concentration",
                        "Destination Concentration",
                        "transfer_ratio",
                    ]
                )
            )

        return transfers_to.select(
            [
                "Sample Name",
                "Source Plate Name",
                "Source Well",
                "Source Concentration",
                "Destination Concentration",
                "transfer_ratio",
            ]
        ).collect()
