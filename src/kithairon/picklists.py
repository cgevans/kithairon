"""Echo PickList support (Kithairon-extended)."""

from collections.abc import Sequence
from typing import TYPE_CHECKING, Literal

import polars as pl
from loguru import logger

from .labware import Labware, _CONSISTENT_COLS, get_default_labware

# from kithairon.surveys import SurveyData

if TYPE_CHECKING:  # pragma: no cover
    from networkx import DiGraph, MultiDiGraph


class PickList:
    """A PickList in Echo-software-compatible format."""

    data: pl.DataFrame

    def __init__(self, df: pl.DataFrame):
        self.data = df

    @classmethod
    def concat(cls, picklists: Sequence["PickList"]) -> "PickList":
        return cls(pl.concat(p.data for p in picklists))

    def __repr__(self):
        return repr(self.data)

    def __str__(self):
        return str(self.data)

    def _repr_html_(self):
        return self.data._repr_html_()

    @classmethod
    def from_csv(cls, path: str):
        return cls(pl.read_csv(path))

    def to_csv(self, path: str):
        self.data.write_csv(path)

    def _totvols(self):
        return self.data.group_by(["Destination Plate Name", "Destination Well"]).agg(
            pl.col("Transfer Volume").sum().alias("total_volume")
        )

    def plate_transfer_graph(self) -> "DiGraph":
        """Generate graph of plate usage (source plate -> destination plate)."""
        from networkx import DiGraph, is_directed_acyclic_graph

        plate_txs = (
            self.data.lazy()
            .group_by(
                "Source Plate Name", "Destination Plate Name", maintain_order=True
            )
            .agg(
                pl.col("Transfer Volume").sum(),
                pl.col("Transfer Volume").count().alias("n_txs"),
            )
            .unique(maintain_order=True)
        ).collect()

        G = DiGraph()
        for sn, dn, txv, txn in plate_txs.iter_rows():
            G.add_edge(sn, dn, tot_vol=txv, n_txs=txn)

        if not is_directed_acyclic_graph(G):
            logger.warning("Plate transfer graph is not a DAG")

        return G

    def well_transfer_multigraph(self) -> "MultiDiGraph":
        """Generate a multigraph of each transfer."""
        from networkx import MultiDiGraph, is_directed_acyclic_graph

        well_txs = (
            self.data.lazy().select(
                "Source Plate Name",
                "Source Well",
                "Destination Plate Name",
                "Destination Well",
                "Transfer Volume",
            )
        ).collect()

        G = MultiDiGraph()
        for sn, sw, dn, dw, tx in well_txs.iter_rows():
            G.add_edge((sn, sw), (dn, dw), weight=tx)

        if not is_directed_acyclic_graph(G):
            logger.warning("Well transfer graph is not a DAG")

        return G

    def _dest_plate_type_per_name(self) -> pl.DataFrame:
        # FIXME: havinge multiple consistent plate types is not an error
        df = (self.data.lazy()
            .group_by("Destination Plate Name")
            .agg(pl.col("Destination Plate Type").unique().alias("plate_types"))
            .with_columns(pl.col("plate_types").list.lengths().alias("n_plate_types"))
            .select("Destination Plate Name", "plate_types", "n_plate_types")
            .collect()
        )

        n = df.filter(pl.col("n_plate_types") > 1)
        if len(n) > 0:
            logger.error("Plate Name appears with multiple Plate Types: {}", n)
            raise ValueError("Plate Name appears with multiple Plate Types")   
        return df.select(plate_name=pl.col("Destination Plate Name"), plate_type=pl.col("plate_types").list.first())

    def _src_plate_type_per_name(self) -> pl.DataFrame:
        # FIXME: having multiple consistent plate types is not an error
        df = (self.data.lazy()
            .group_by("Source Plate Name")
            .agg(pl.col("Source Plate Type").unique().alias("plate_types"))
            .with_columns(pl.col("plate_types").list.lengths().alias("n_plate_types"))
            .select("Source Plate Name", "plate_types", "n_plate_types")
            .collect()
        )

        n = df.filter(pl.col("n_plate_types") > 1)
        if len(n) > 0:
            logger.error("Plate Name appears with multiple Plate Types: {}", n)
            raise ValueError("Plate Name appears with multiple Plate Types")   
        return df.select(plate_name=pl.col("Source Plate Name"), plate_type=pl.col("plate_types").list.first())

    def validate(
        self,
        labware: Labware | None | Literal[False] = None,
        # surveys: Sequence['Survey'] | None = None,
        raise_on_error: bool = True,
    ) -> Sequence[str]:
        errors = []

        # Check that every appearance of a Plate Name has the same Plate Type
        dest_plate_types = self._dest_plate_type_per_name()
        src_plate_types = self._src_plate_type_per_name()
        
        if labware is None:
            try:
                labware = get_default_labware()
            except ValueError:
                logger.warning("No default labware, not checking labware.")
                labware = False

        if labware is not False:
            labware_df = labware.to_polars()

            dest_plate_info = dest_plate_types.join(
                labware_df,
                on="plate_type",
                how="left",
            )
            if len(x := dest_plate_info.filter(pl.col("plate_type").is_null())) > 0:
                logger.error("Plate Type not found in labware definition: {}", x)
                raise ValueError("Plate Type not found in labware definition")
            
            if len(x := dest_plate_info.filter(pl.col("usage") != "DEST")) > 0:
                logger.error("Plate Type is not a DEST plate: {}", x)
                raise ValueError("Plate Type is not a DEST plate")
            
            src_plate_info = src_plate_types.join(
                labware_df,
                on="plate_type",
                how="left",
            )
            if len(x := src_plate_info.filter(pl.col("plate_type").is_null())) > 0:
                logger.error("Plate Type not found in labware definition: {}", x)
                raise ValueError("Plate Type not found in labware definition")
            
            if len(x := src_plate_info.filter(pl.col("usage") != "SRC")) > 0:
                logger.error("Plate Type is not a SRC plate: {}", x)
                raise ValueError("Plate Type is not a SRC plate")
            
            # TODO: add check that plates used for both source and dest have consistent
            # plate types.
            all_plate_info = dest_plate_info.vstack(src_plate_info)
            nu = all_plate_info.group_by("plate_name").agg(
                [pl.col(x).n_unique() for x in _CONSISTENT_COLS]
            )

            p_with_lb = (
                self.data.lazy()
                .join(
                    labware_df.lazy(),
                    left_on="Source Plate Name",
                    right_on="plate_type",
                    how="left",
                )
                .join(
                    labware_df.lazy(),
                    left_on="Destination Plate Name",
                    right_on="plate_type",
                    how="left",
                    suffix="_dest",
                )
            )

            wrongvolume = (
                p_with_lb.with_columns(
                    tx_mod=(pl.col("Transfer Volume") % pl.col("drop_volume"))
                )
                .filter(pl.col("tx_mod") != 0)
                .collect()
            )

            if len(wrongvolume) > 0:
                print("Transfer volumes are not multiples of drop volume:")
                print(wrongvolume)
                errors.append("Transfer volumes are not multiples of drop volume")

        return errors

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
            transfers_to = self.data.filter(
                (pl.col("Destination Plate Name") == plate)
                & (pl.col("Destination Well") == well)
            )
        elif (plate is None) and (well is None) and (name is not None):
            transfers_to = self.data.filter(pl.col("Destination Sample Name") == name)
        else:
            raise ValueError("Invalid combination of arguments")

        totvols = self._totvols().lazy()

        # If transfers_to does not have a "Source Concentration" column, add one filled with nulls
        if "Source Concentration" not in transfers_to.columns:
            transfers_to = transfers_to.with_columns(
                pl.lit(None).cast(pl.Float32).alias("Source Concentration")
            )

        # Lazily add a Source Concentration column to self.df if there isn't one
        if "Source Concentration" not in self.data.columns:
            selfdf = self.data.with_columns(
                pl.lit(None).cast(pl.Float32).alias("Source Concentration")
            ).lazy()
        else:
            selfdf = self.data.lazy()

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
                        pl.col("transfer_ratio")
                        * pl.col("Transfer Volume_int")
                        / pl.col("total_volume_right")
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
