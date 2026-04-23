"""Echo PickList support (Kithairon-extended)."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Literal, Self

import networkx as nx
import networkx.algorithms.approximation as nxaa
import polars as pl
import rich
from polars import LazyFrame

from kithairon import _native
from kithairon._util import well_to_tuple as well_to_tuple  # re-export

from .labware import Labware, get_default_labware

if TYPE_CHECKING:
    from collections.abc import Sequence

    from kithairon.surveys.surveydata import SurveyData


def _rotate_cycle(ln: Sequence[Any], elem: Any) -> Sequence[Any]:  # type: ignore
    i = ln.index(elem)
    assert ln[0] == ln[-1]
    if i == 0:
        assert ln[-1] == elem
        return ln
    return ln[i:] + ln[1:i] + [elem]  # type: ignore


def _dest_motion_distance(
    sp1: tuple[int, int],  # row, column
    dp1: tuple[int, int],
    sp2: tuple[int, int],
    dp2: tuple[int, int],
    swsx: float = 4.5,
    swsy: float = 4.5,
    dwsx: float = 4.5,
    dwsy: float = 4.5,
) -> float:
    off = (swsx * (sp2[1] - sp1[1]), swsy * (sp2[0] - sp1[0]))  # (x, y)
    vec = (dwsx * (dp1[1] - dp2[1]) - off[0], dwsy * (dp2[0] - dp1[0]) - off[1])
    # return (vec[0] ** 2 + vec[1] ** 2) ** 0.5
    return abs(vec[0]) + abs(vec[1])


def _dest_motion_distance_by_wells(
    sp1: str,
    dp1: str,
    sp2: str,
    dp2: str,
    swsx: float = 4.5,
    swsy: float = 4.5,
    dwsx: float = 4.5,
    dwsy: float = 4.5,
) -> float:
    sp1t: tuple[int, int] = well_to_tuple(sp1)
    dp1t: tuple[int, int] = well_to_tuple(dp1)
    sp2t: tuple[int, int] = well_to_tuple(sp2)
    dp2t: tuple[int, int] = well_to_tuple(dp2)
    return _dest_motion_distance(sp1t, dp1t, sp2t, dp2t, swsx, swsy, dwsx, dwsy)


def _transducer_motion_distance(
    sp1, dp1, sp2, dp2, swsx=4.5, swsy=4.5, dwsx=4.5, dwsy=4.5
) -> float:
    vec = ((sp2[1] - sp1[1]) * swsx, (sp2[0] - sp1[0]) * swsy)
    return abs(vec[0]) + abs(vec[1])


def _transducer_motion_distance_by_wells(
    sp1: str,
    dp1: str,
    sp2: str,
    dp2: str,
    swsx: float = 4.5,
    swsy: float = 4.5,
    dwsx: float = 4.5,
    dwsy: float = 4.5,
) -> float:
    sp1t: tuple[int, int] = well_to_tuple(sp1)
    dp1t: tuple[int, int] = well_to_tuple(dp1)
    sp2t: tuple[int, int] = well_to_tuple(sp2)
    dp2t: tuple[int, int] = well_to_tuple(dp2)
    return _transducer_motion_distance(sp1t, dp1t, sp2t, dp2t, swsx, swsy, dwsx, dwsy)


logger = logging.getLogger(__name__)

# from kithairon.surveys import SurveyData

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd
    from networkx import DiGraph, MultiDiGraph


def _build_survey_volumes_dict(
    picklist: PickList, surveys: SurveyData | None
) -> dict[str, dict[str, float]] | None:
    """Extract per-plate {well → volume (nL)} map from ``surveys``.

    Returns ``None`` if no surveys are provided, so the Rust validator can
    skip survey-based volume bookkeeping entirely.
    """
    if surveys is None:
        return None
    out: dict[str, dict[str, float]] = {}
    for plate in picklist.all_plate_names():
        try:
            sd = surveys.find_latest_survey(plate_name=plate)
        except KeyError:
            continue
        cols = sd.data.select(pl.col("well"), pl.col("volume"))
        well_map: dict[str, float] = {}
        for well, vol in cols.iter_rows():
            if vol is None:
                continue
            # Python SurveyData.volume is μL; Rust expects nL.
            well_map[str(well)] = float(vol) * 1000.0
        if well_map:
            out[str(plate)] = well_map
    return out or None


class PickList:
    """A PickList in Echo-software-compatible format."""

    data: pl.DataFrame

    def __init__(self, df: pl.DataFrame):
        self.data = df

    @classmethod
    def concat(cls, picklists: Sequence[PickList]) -> PickList:
        return cls(pl.concat((p.data for p in picklists), how="diagonal"))

    def select(self, *args, **kwargs) -> PickList:
        return self.__class__(self.data.select(*args, **kwargs))

    def filter(self, *args, **kwargs) -> PickList:
        return self.__class__(self.data.filter(*args, **kwargs))

    def with_columns(self, *args, **kwargs) -> PickList:
        return self.__class__(self.data.with_columns(*args, **kwargs))

    def join(self, *args, **kwargs) -> PickList:
        return self.__class__(self.data.join(*args, **kwargs))

    def __repr__(self):
        return repr(self.data)

    def __str__(self):
        return str(self.data)

    def __add__(self, other: PickList) -> PickList:
        return self.__class__(pl.concat([self.data, other.data], how="diagonal"))

    def _repr_html_(self) -> str:
        return self.data._repr_html_()

    def to_polars(self) -> pl.DataFrame:
        return self.data

    def to_pandas(self) -> pd.DataFrame:
        return self.data.to_pandas()

    @classmethod
    def read_csv(cls, path: str) -> Self:
        """Read a picklist from a csv file."""
        return cls(pl.read_csv(path))

    @classmethod
    def read_csv_native(cls, path: str) -> Self:
        """Read a picklist via the Rust parser.

        Handles optional/missing columns per Echo's convention and validates
        required columns up-front. Returns a ``PickList`` whose DataFrame has
        only columns that appeared in the file.
        """
        records = _native.read_picklist_csv_records(path)
        return cls(pl.DataFrame(records) if records else pl.DataFrame())

    def write_csv(self, path: str) -> None:
        """Write picklist to a csv file (usable by Labcyte/Beckman software)."""
        self.data.write_csv(path)

    def _totvols(self) -> pl.DataFrame:
        return self.data.group_by(["Destination Plate Name", "Destination Well"]).agg(
            pl.col("Transfer Volume").sum().alias("total_volume")
        )

    def plate_transfer_graph(self) -> DiGraph:
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

    def well_transfer_multigraph(self) -> MultiDiGraph:
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
        plate_types = (
            self.data.lazy()
            .group_by("Destination Plate Name")
            .agg(pl.col("Destination Plate Type").unique().alias("plate_types"))
            .with_columns(pl.col("plate_types").list.len().alias("n_plate_types"))
            .select("Destination Plate Name", "plate_types", "n_plate_types")
            .collect()
        )

        n = plate_types.filter(pl.col("n_plate_types") > 1)
        if len(n) > 0:
            logger.error("Plate Name appears with multiple Plate Types: %r", n)
            raise ValueError("Plate Name appears with multiple Plate Types")
        return plate_types.select(
            plate_name=pl.col("Destination Plate Name"),
            plate_type=pl.col("plate_types").list.first(),
        )

    def _src_plate_type_per_name(self) -> pl.DataFrame:
        # FIXME: having multiple consistent plate types is not an error
        plate_types = (
            self.data.lazy()
            .group_by("Source Plate Name")
            .agg(pl.col("Source Plate Type").unique().alias("plate_types"))
            .with_columns(pl.col("plate_types").list.len().alias("n_plate_types"))
            .select("Source Plate Name", "plate_types", "n_plate_types")
            .collect()
        )

        n = plate_types.filter(pl.col("n_plate_types") > 1)
        if len(n) > 0:
            logger.error("Plate Name appears with multiple Plate Types: %r", n)
            raise ValueError("Plate Name appears with multiple Plate Types")
        return plate_types.select(
            plate_name=pl.col("Source Plate Name"),
            plate_type=pl.col("plate_types").list.first(),
        )

    def all_plate_names(self) -> pl.Series:
        return pl.concat(
            (
                self.data.get_column("Source Plate Name"),
                self.data.get_column("Destination Plate Name"),
            )
        ).unique(maintain_order=True)

    def validate(
        self,
        surveys: SurveyData | None = None,
        labware: Labware | None = None,
        raise_on: Literal[False, True, "warning", "error"] = "error",
    ) -> tuple[list[str], list[str]]:
        """Check the picklist for errors and potential problems.

        Backed by the Rust ``validate_picklist_records`` native function.
        """
        if labware is None:
            try:
                labware = get_default_labware()
            except ValueError:
                err = "No labware definitions available."
                rich.print(f"[red]{err}[/red]")
                return [err], []

        survey_volumes = _build_survey_volumes_dict(self, surveys)

        records = self.data.to_dicts()
        errors, warnings = _native.validate_picklist_records(
            records,
            labware._inner,
            survey_volumes,
        )

        for w in warnings:
            rich.print(f"[orange1]{w}[/orange1]")
        for e in errors:
            rich.print(f"[red]{e}[/red]")

        if raise_on == "error":
            if errors:
                raise ValueError("Errors in picklist")
        elif raise_on == "warning":
            if warnings:
                raise ValueError("Warnings in picklist")
            if errors:
                raise ValueError("Errors in picklist")

        return errors, warnings

    def get_contents(
        self,
        plate: str | None = None,
        well: str | None = None,
        name: str | None = None,
    ) -> pl.DataFrame:
        """Recursively get the contents of a particular destination."""
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

        transfers_to: LazyFrame = (
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

    def optimize_well_transfer_order(
        self, labware: Labware | None = None, method: Literal["quick", "slow"] = "quick"
    ) -> PickList:
        if method == "quick":
            return self._optimize_well_transfer_order_quick()
        else:
            return self._optimize_well_transfer_order_full()

    def _optimize_well_transfer_order_quick(self) -> Self:
        records = self.data.to_dicts()
        if not records:
            return self.__class__(self.data.clone())
        perm = _native.picklist_quick_order_indices(records)
        reordered = self.data[perm]
        return self.__class__(reordered)

    def _optimize_well_transfer_order_full(
        self, labware: Labware | None = None
    ) -> Self:
        orders = []

        if labware is None:
            labware = get_default_labware()

        if "segment_index" not in self.data.columns:
            dat_with_order = self.data.with_columns(
                segment_index=(
                    (
                        pl.col("Source Plate Name").ne_missing(
                            pl.col("Source Plate Name").shift()
                        )
                    )
                    | (
                        pl.col("Destination Plate Name").ne_missing(
                            pl.col("Destination Plate Name").shift()
                        )
                    )
                ).cum_sum()
            )
        else:
            dat_with_order = self.data.with_columns()

        for _, ppdat in dat_with_order.group_by("segment_index", maintain_order=True):
            source_plate_name = ppdat.get_column("Source Plate Name")[0]
            dest_plate_name = ppdat.get_column("Destination Plate Name")[0]

            spti = labware[ppdat.get_column("Source Plate Type")[0]]
            dpti = labware[ppdat.get_column("Destination Plate Type")[0]]
            swsx = spti.center_spacing_x / 100.0
            swsy = spti.center_spacing_y / 100.0
            dwsx = dpti.center_spacing_x / 100.0
            dwsy = dpti.center_spacing_y / 100.0

            xx = ppdat.select(
                pl.col("Source Well").alias("sw"),
                pl.col("Destination Well").alias("dw"),
            )
            t = [tuple(x) for x in xx.to_numpy()]
            G = nx.Graph()
            G.add_nodes_from(t)
            G.add_node("fake")
            G.add_weighted_edges_from(
                [
                    (
                        t1,
                        t2,
                        _dest_motion_distance_by_wells(
                            t1[0], t1[1], t2[0], t2[1], swsx, swsy, dwsx, dwsy
                        ),
                    )
                    for t1 in t
                    for t2 in t
                    if t1 != t2
                ]
            )
            G.add_weighted_edges_from([("fake", t1, 0) for t1 in t])
            # trav = nxaa.greedy_tsp(G, source='fake')
            trav = _rotate_cycle(nxaa.christofides(G), "fake")  # type: ignore
            trav = nxaa.simulated_annealing_tsp(
                G, trav, max_iterations=400, source="fake"
            )
            trav = trav[1:-1]
            o = (
                pl.from_records(
                    trav, schema={"Source Well": str, "Destination Well": str}
                )
                .with_row_count("well_well_index")
                .with_columns(
                    pl.lit(source_plate_name).alias("Source Plate Name"),  # type: ignore
                    pl.lit(dest_plate_name).alias("Destination Plate Name"),  # type: ignore
                )
            )
            orders.append(o)

        ordersdf = pl.concat(orders)
        return self.__class__(
            dat_with_order.join(
                ordersdf,
                on=[
                    "Source Plate Name",
                    "Destination Plate Name",
                    "Source Well",
                    "Destination Well",
                ],
                how="left",
            ).sort(["segment_index", "well_well_index"])
        )

    def non_intermediate_transfers(self):
        return self.filter(
            pl.struct("Source Plate Name", "Source Well")
            .struct.rename_fields(["Plate", "Well"])
            .alias("Source")
            .is_in(
                pl.struct("Destination Plate Name", "Destination Well")
                .struct.rename_fields(["Plate", "Well"])
                .alias("Dest")
            )
            .not_()
        )

    def non_intermediate_source_plate_names(self):
        return (
            self.non_intermediate_transfers()
            .data.get_column("Source Plate Name")
            .unique(maintain_order=True)
        )

    def with_segment_index(self):
        return self.with_columns(
            segment_index=(
                (
                    pl.col("Source Plate Name").ne_missing(
                        pl.col("Source Plate Name").shift()
                    )
                )
                | (
                    pl.col("Destination Plate Name").ne_missing(
                        pl.col("Destination Plate Name").shift()
                    )
                )
            ).cum_sum()
        )
