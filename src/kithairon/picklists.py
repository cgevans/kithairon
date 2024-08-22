"""Echo PickList support (Kithairon-extended)."""

import logging
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Literal

import networkx as nx
import networkx.algorithms.approximation as nxaa
import polars as pl
import rich
from polars import LazyFrame
from typing_extensions import Self

from kithairon.surveys.surveydata import SurveyData

from .labware import Labware, get_default_labware


def _rotate_cycle(ln: Sequence[Any], elem: Any) -> Sequence[Any]:
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


def well_to_tuple(well: str) -> tuple[int, int]:
    # (row, column)
    return (ord(well[0]) - 65, int(well[1:]) - 1)


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


class PickList:
    """A PickList in Echo-software-compatible format."""

    data: pl.DataFrame

    def __init__(self, df: pl.DataFrame):
        self.data = df

    @classmethod
    def concat(cls, picklists: Sequence["PickList"]) -> "PickList":
        return cls(pl.concat(p.data for p in picklists))

    def select(self, *args, **kwargs) -> "PickList":
        return self.__class__(self.data.select(*args, **kwargs))

    def filter(self, *args, **kwargs) -> "PickList":
        return self.__class__(self.data.filter(*args, **kwargs))

    def with_columns(self, *args, **kwargs) -> "PickList":
        return self.__class__(self.data.with_columns(*args, **kwargs))

    def join(self, *args, **kwargs) -> "PickList":
        return self.__class__(self.data.join(*args, **kwargs))

    def __repr__(self):
        return repr(self.data)

    def __str__(self):
        return str(self.data)

    def __add__(self, other: "PickList") -> "PickList":
        return self.__class__(pl.concat([self.data, other.data], how="diagonal"))

    def _repr_html_(self) -> Self:
        return self.data._repr_html_()

    def to_polars(self) -> pl.DataFrame:
        return self.data

    def to_pandas(self) -> "pd.DataFrame":
        return self.data.to_pandas()

    @classmethod
    def read_csv(cls, path: str) -> Self:
        """Read a picklist from a csv file."""
        return cls(pl.read_csv(path))

    def write_csv(self, path: str) -> None:
        """Write picklist to a csv file (usable by Labcyte/Beckman software)."""
        self.data.write_csv(path)

    def _totvols(self) -> pl.DataFrame:
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

    def validate(  # noqa: PLR0912, PLR0915
        self,
        surveys: SurveyData | None = None,
        labware: Labware | None = None,
        raise_on: Literal[False, True, "warning", "error"] = "error",
    ) -> tuple[list[str], list[str]]:
        """Check the picklist for errors and potential problems."""
        errors = []
        warnings = []

        def add_warning(w):
            rich.print(f"[orange1]{w}[/orange1]")
            warnings.append(w)

        def add_error(w):
            rich.print(f"[red]{w}[/red]")
            errors.append(w)

        if surveys is None:
            surveys = SurveyData()

        # Check that every appearance of a Plate Name has the same Plate Type
        dest_plate_types = self._dest_plate_type_per_name()
        src_plate_types = self._src_plate_type_per_name()

        if labware is None:
            try:
                labware = get_default_labware()
            except ValueError:
                add_error("No labware definitions available.")
                return errors, warnings

        labware_df = labware.to_polars()

        dest_plate_info = dest_plate_types.join(
            labware_df,
            on="plate_type",
            how="left",
        )
        if len(x := dest_plate_info.filter(pl.col("plate_type").is_null())) > 0:
            logger.error("Plate Type not found in labware definition: %s", x)
            raise ValueError("Plate Type not found in labware definition")

        if len(x := dest_plate_info.filter(pl.col("usage") != "DEST")) > 0:
            logger.error("Plate Type is not a DEST plate: %s", x)
            raise ValueError("Plate Type is not a DEST plate")

        src_plate_info = src_plate_types.join(
            labware_df,
            on="plate_type",
            how="left",
        )
        if len(x := src_plate_info.filter(pl.col("plate_type").is_null())) > 0:
            logger.error("Plate Type not found in labware definition: %s", x)
            raise ValueError("Plate Type not found in labware definition")

        if len(x := src_plate_info.filter(pl.col("usage") != "SRC")) > 0:
            logger.error("Plate Type is not a SRC plate: %s", x)
            raise ValueError("Plate Type is not a SRC plate")

        # TODO: add check that plates used for both source and dest have consistent
        # plate types.
        # all_plate_info = dest_plate_info.vstack(src_plate_info)
        # nu = all_plate_info.group_by("plate_name").agg(
        #     [pl.col(x).n_unique() for x in _CONSISTENT_COLS]
        # )

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
            add_error(
                f"Transfer volumes are not multiples of drop volume: {wrongvolume}"
            )

        import networkx as nx

        g = self.well_transfer_multigraph()

        if not nx.is_directed_acyclic_graph(g):
            c = nx.find_cycle(g)
            add_warning(f"Well transfer multigraph has a cycle: {c}")

        a = list(enumerate(nx.topological_generations(g)))

        topogen = sum(([x[0]] * len(x[1]) for x in a), [])  # noqa: RUF017
        plate = [y[0] for x in a for y in x[1]]
        well = [y[1] for x in a for y in x[1]]

        tgl = pl.DataFrame({"plate": plate, "well": well, "topogen": topogen}).lazy()

        p = (
            self.data.lazy()
            .join(
                tgl,
                left_on=["Source Plate Name", "Source Well"],
                right_on=["plate", "well"],
                how="inner",
            )
            .join(
                tgl,
                left_on=["Destination Plate Name", "Destination Well"],
                right_on=["plate", "well"],
                how="inner",
                suffix="_dest",
            )
            .filter(
                pl.col("topogen")
                >= pl.col("topogen_dest").reverse().cum_min().reverse()
            )
            .collect()
        )

        if len(p) > 0:
            print("Transfers are not topologically ordered:")
            print(p)
            errors.append("Transfers are not topologically ordered")
        else:
            logger.info("Transfers are topologically ordered.")

        dwi = self.data.lazy().with_row_index()

        for p in self.all_plate_names():
            change_data = (
                dwi.filter(
                    (pl.col("Source Plate Name") == p)
                    | (pl.col("Destination Plate Name") == p)
                )
                .with_columns(
                    plate_well=pl.when(pl.col("Source Plate Name") == p)
                    .then(pl.col("Source Well"))
                    .otherwise(pl.col("Destination Well")),
                    use=pl.when(pl.col("Source Plate Name") == p)
                    .then(pl.lit("source"))
                    .otherwise(pl.lit("dest")),
                    volume_change=pl.when(pl.col("Source Plate Name") == p)
                    .then(-pl.col("Transfer Volume"))
                    .otherwise(pl.col("Transfer Volume")),
                )
                .join(
                    labware_df.lazy().select(
                        "plate_type",
                        pl.col.plate_format.alias("source_plate_format"),
                        (1000.0 * pl.col.min_well_vol).alias("min_well_vol"),
                        (1000.0 * pl.col.max_well_vol).alias("max_well_vol"),
                        "min_volume",
                        "drop_volume",
                        "max_vol_total",
                    ),
                    left_on="Source Plate Type",
                    right_on="plate_type",
                    how="left",
                )
            ).collect()

            # source_ever = change_data.select((pl.col.use == "source").any())[0, 0]
            # dest_ever = change_data.select((pl.col.use == "dest").any())[0, 0]

            source_first = change_data.select((pl.col.use == "source").first())[0, 0]

            # If a plate is a dest-first plate, we can assume that all wells have zero volume initially
            # If a plate is a source-first plate, we should warn if there is no survey
            try:
                survey = surveys.find_latest_survey(plate_name=p)
                change_data = pl.concat(
                    (
                        survey.data.select(
                            plate_well=pl.col("well"),
                            volume_change=pl.col("volume") * 1000,
                            use=pl.lit("survey"),
                        ),
                        change_data,
                    ),
                    how="diagonal",
                )
                have_survey = True
            except KeyError:
                if source_first:
                    add_warning(f"No survey data for {p}")
                have_survey = False

            change_data = change_data.with_columns(
                volume_after=pl.col.volume_change.cum_sum().over("plate_well"),
            ).with_columns(
                volume_before=pl.col.volume_after - pl.col.volume_change,
            )

            # In all cases

            # Is a well above max_well_vol when used as a source (before transfer)?
            above_max = change_data.filter(
                (pl.col.use == "source") & (pl.col.volume_before > pl.col.max_well_vol)
            )
            for row in above_max.iter_rows(named=True):
                tx = "ix {index}, {Transfer Volume} nL, {Source Plate Name} {Source Well} → {Destination Plate Name} {Destination Well}".format(
                    **row
                )
                add_warning(
                    "{Source Plate Name} {plate_well} is above max_well_vol ({volume_before} nL > {max_well_vol} nL) before transfer {tx}".format(
                        tx=tx, **row
                    )
                )

            # Is a transfer above max_vol_total?
            for row in change_data.filter(
                pl.col.use == "source", -pl.col.volume_change > pl.col.max_vol_total
            ).iter_rows(named=True):
                tx = "ix {index}, {Transfer Volume} nL, {Source Plate Name} {Source Well} → {Destination Plate Name} {Destination Well}".format(
                    **row
                )
                add_warning(
                    "{Source Plate Name} {plate_well} has a transfer above max_vol_total ({volume_change} nL < -{max_vol_total} ) {tx}".format(
                        tx=tx, **row
                    )
                )

            # If we have survey data for the plate, or it is a dest-first plate (assume zero initial volume):
            if have_survey or not source_first:
                # Does a well go below min_well_vol as a source?
                for row in change_data.filter(
                    pl.col.use == "source", pl.col.volume_after < pl.col.min_well_vol
                ).iter_rows(named=True):
                    tx = "ix {index}, {Transfer Volume} nL, {Source Plate Name} {Source Well} → {Destination Plate Name} {Destination Well}".format(
                        **row
                    )
                    add_warning(
                        "{Source Plate Name} {plate_well} goes below min_well_vol ({volume_after} nL < {min_well_vol} nL) {tx}".format(
                            tx=tx, **row
                        )
                    )
            else:
                # Does a well go below (min_well_vol - max_well_vol) when used as a source?
                for row in change_data.filter(
                    pl.col.use == "source",
                    pl.col.volume_before < (pl.col.min_well_vol - pl.col.max_well_vol),
                ).iter_rows(named=True):
                    tx = "ix {index}, {Transfer Volume} nL, {Source Plate Name} {Source Well} → {Destination Plate Name} {Destination Well}".format(
                        **row
                    )
                    add_warning(
                        "Net transfers for {Source Plate Name} {plate_well} (no survey) go below min_well_vol-max_well_vol ({volume_before} nL < {max_def} nL) {tx}".format(
                            tx=tx, **row
                        )
                    )

        if raise_on == "error":
            if len(errors) > 0:
                raise ValueError("Errors in picklist")
        elif raise_on == "warning":
            if len(warnings) > 0:
                raise ValueError("Warnings in picklist")
            if len(errors) > 0:
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
        self: "PickList", labware: Labware | None = None
    ) -> "PickList":
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
            .unique()
        )
