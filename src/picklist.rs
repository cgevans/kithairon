//! Echo picklists: CSV I/O, transfer graphs, validation, ordering.
//!
//! A picklist is an ordered list of [`Transfer`]s. Each transfer moves a
//! volume from one source well to one destination well. Known Echo CSV
//! columns are promoted to typed fields on [`Transfer`]; any additional
//! columns encountered on read are preserved in `Transfer::extra` so that
//! round-trips do not silently drop data.

use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

use indexmap::IndexMap;
use petgraph::algo::{is_cyclic_directed, toposort};
use petgraph::graph::{DiGraph, NodeIndex};
use serde::{Deserialize, Serialize};

use crate::LibraryError;

/// A single source→destination volume transfer, matching one row of an
/// Echo picklist CSV.
///
/// Only five columns are truly required to drive the instrument; everything
/// else is optional. Unknown columns round-trip through `extra`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Transfer {
    pub source_plate_name: String,
    pub source_well: String,
    pub destination_plate_name: String,
    pub destination_well: String,
    pub transfer_volume: f64,

    pub source_plate_type: Option<String>,
    pub source_plate_barcode: Option<String>,
    pub sample_name: Option<String>,
    pub source_concentration: Option<f64>,
    pub source_concentration_units: Option<String>,

    pub destination_plate_type: Option<String>,
    pub destination_plate_barcode: Option<String>,
    pub destination_sample_name: Option<String>,
    pub destination_concentration: Option<f64>,
    pub destination_concentration_units: Option<String>,

    /// Columns not recognized as well-known Echo fields, preserved in the
    /// order the CSV headers declared them.
    pub extra: IndexMap<String, String>,
}

/// Canonical Echo column names for fields we promote to typed accessors.
mod col {
    pub const SOURCE_PLATE_NAME: &str = "Source Plate Name";
    pub const SOURCE_WELL: &str = "Source Well";
    pub const DEST_PLATE_NAME: &str = "Destination Plate Name";
    pub const DEST_WELL: &str = "Destination Well";
    pub const TRANSFER_VOLUME: &str = "Transfer Volume";
    pub const SOURCE_PLATE_TYPE: &str = "Source Plate Type";
    pub const SOURCE_PLATE_BARCODE: &str = "Source Plate Barcode";
    pub const SAMPLE_NAME: &str = "Sample Name";
    pub const SOURCE_CONCENTRATION: &str = "Source Concentration";
    pub const SOURCE_CONCENTRATION_UNITS: &str = "Source Concentration Units";
    pub const DEST_PLATE_TYPE: &str = "Destination Plate Type";
    pub const DEST_PLATE_BARCODE: &str = "Destination Plate Barcode";
    pub const DEST_SAMPLE_NAME: &str = "Destination Sample Name";
    pub const DEST_CONCENTRATION: &str = "Destination Concentration";
    pub const DEST_CONCENTRATION_UNITS: &str = "Destination Concentration Units";

    pub const KNOWN: &[&str] = &[
        SOURCE_PLATE_NAME,
        SOURCE_PLATE_BARCODE,
        SOURCE_PLATE_TYPE,
        SOURCE_WELL,
        SAMPLE_NAME,
        SOURCE_CONCENTRATION,
        SOURCE_CONCENTRATION_UNITS,
        DEST_PLATE_NAME,
        DEST_PLATE_BARCODE,
        DEST_PLATE_TYPE,
        DEST_WELL,
        DEST_SAMPLE_NAME,
        DEST_CONCENTRATION,
        DEST_CONCENTRATION_UNITS,
        TRANSFER_VOLUME,
    ];
}

/// A picklist: an ordered sequence of [`Transfer`] rows plus the set of
/// extra-column headers seen on read (used to drive write-ordering).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PickList {
    pub transfers: Vec<Transfer>,
    /// Order of unknown columns as they appeared in the input CSV. Used on
    /// write so that `read_csv` → `write_csv` is byte-stable for unknown
    /// headers. Known columns are always written in a fixed canonical order.
    extra_headers: Vec<String>,
}

impl PickList {
    pub fn new(transfers: Vec<Transfer>) -> Self {
        let mut extra_headers: Vec<String> = Vec::new();
        let mut seen: BTreeSet<&str> = BTreeSet::new();
        for t in &transfers {
            for k in t.extra.keys() {
                if seen.insert(k) {
                    extra_headers.push(k.clone());
                }
            }
        }
        Self { transfers, extra_headers }
    }

    pub fn transfers(&self) -> &[Transfer] {
        &self.transfers
    }

    pub fn into_transfers(self) -> Vec<Transfer> {
        self.transfers
    }

    pub fn len(&self) -> usize {
        self.transfers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.transfers.is_empty()
    }

    // ----- CSV I/O -------------------------------------------------------

    pub fn from_csv_reader<R: Read>(rdr: R) -> Result<Self, LibraryError> {
        let mut csv_rdr = csv::ReaderBuilder::new()
            .has_headers(true)
            .flexible(true)
            .from_reader(rdr);

        let headers = csv_rdr
            .headers()
            .map_err(|e| LibraryError::PickListCsv(e.to_string()))?
            .clone();

        // Resolve column-name → header-index up front.
        let mut col_idx: HashMap<&'static str, usize> = HashMap::new();
        let mut extra_headers: Vec<String> = Vec::new();
        let mut extra_indices: Vec<(String, usize)> = Vec::new();
        for (i, h) in headers.iter().enumerate() {
            if let Some(&known) = col::KNOWN.iter().find(|k| **k == h) {
                col_idx.insert(known, i);
            } else {
                extra_headers.push(h.to_string());
                extra_indices.push((h.to_string(), i));
            }
        }

        for required in [
            col::SOURCE_PLATE_NAME,
            col::SOURCE_WELL,
            col::DEST_PLATE_NAME,
            col::DEST_WELL,
            col::TRANSFER_VOLUME,
        ] {
            if !col_idx.contains_key(required) {
                return Err(LibraryError::PickListCsv(format!(
                    "required column missing: {required:?}"
                )));
            }
        }

        let mut transfers = Vec::new();
        for (row_ix, rec) in csv_rdr.records().enumerate() {
            let rec = rec.map_err(|e| LibraryError::PickListCsv(e.to_string()))?;
            let get = |name: &str| -> Option<&str> {
                let i = *col_idx.get(name)?;
                let v = rec.get(i)?;
                if v.is_empty() { None } else { Some(v) }
            };
            let parse_f64 = |name: &str| -> Result<f64, LibraryError> {
                let v = get(name).ok_or_else(|| {
                    LibraryError::PickListCsv(format!(
                        "row {}: missing required value for {name:?}",
                        row_ix + 1
                    ))
                })?;
                v.trim().parse::<f64>().map_err(|_| {
                    LibraryError::PickListCsv(format!(
                        "row {}: could not parse {name:?} value {v:?} as number",
                        row_ix + 1
                    ))
                })
            };
            let parse_opt_f64 = |name: &str| -> Result<Option<f64>, LibraryError> {
                match get(name) {
                    None => Ok(None),
                    Some(v) => v.trim().parse::<f64>().map(Some).map_err(|_| {
                        LibraryError::PickListCsv(format!(
                            "row {}: could not parse {name:?} value {v:?} as number",
                            row_ix + 1
                        ))
                    }),
                }
            };

            let mut extra = IndexMap::new();
            for (name, i) in &extra_indices {
                if let Some(v) = rec.get(*i) {
                    extra.insert(name.clone(), v.to_string());
                }
            }

            transfers.push(Transfer {
                source_plate_name: get(col::SOURCE_PLATE_NAME)
                    .map(str::to_string)
                    .unwrap_or_default(),
                source_well: get(col::SOURCE_WELL).map(str::to_string).unwrap_or_default(),
                destination_plate_name: get(col::DEST_PLATE_NAME)
                    .map(str::to_string)
                    .unwrap_or_default(),
                destination_well: get(col::DEST_WELL).map(str::to_string).unwrap_or_default(),
                transfer_volume: parse_f64(col::TRANSFER_VOLUME)?,
                source_plate_type: get(col::SOURCE_PLATE_TYPE).map(str::to_string),
                source_plate_barcode: get(col::SOURCE_PLATE_BARCODE).map(str::to_string),
                sample_name: get(col::SAMPLE_NAME).map(str::to_string),
                source_concentration: parse_opt_f64(col::SOURCE_CONCENTRATION)?,
                source_concentration_units: get(col::SOURCE_CONCENTRATION_UNITS).map(str::to_string),
                destination_plate_type: get(col::DEST_PLATE_TYPE).map(str::to_string),
                destination_plate_barcode: get(col::DEST_PLATE_BARCODE).map(str::to_string),
                destination_sample_name: get(col::DEST_SAMPLE_NAME).map(str::to_string),
                destination_concentration: parse_opt_f64(col::DEST_CONCENTRATION)?,
                destination_concentration_units: get(col::DEST_CONCENTRATION_UNITS).map(str::to_string),
                extra,
            });
        }

        Ok(Self { transfers, extra_headers })
    }

    pub fn read_csv(path: impl AsRef<Path>) -> Result<Self, LibraryError> {
        let f = File::open(path.as_ref())?;
        Self::from_csv_reader(f)
    }

    /// Serialize to CSV writing only the columns that are in use. A known
    /// column is written if at least one row carries a value for it
    /// (required columns are always written). Unknown columns are written
    /// in their original order.
    pub fn to_csv_writer<W: Write>(&self, w: W) -> Result<(), LibraryError> {
        let mut wtr = csv::WriterBuilder::new().has_headers(true).from_writer(w);

        let used: HashMap<&'static str, bool> = self.used_columns();
        let mut headers: Vec<&str> = Vec::new();
        for &k in col::KNOWN {
            if *used.get(k).unwrap_or(&false) {
                headers.push(k);
            }
        }
        for e in &self.extra_headers {
            headers.push(e.as_str());
        }

        wtr.write_record(&headers)
            .map_err(|e| LibraryError::PickListCsv(e.to_string()))?;

        for t in &self.transfers {
            let mut row: Vec<String> = Vec::with_capacity(headers.len());
            for h in &headers {
                row.push(t.get_column(h).unwrap_or_default());
            }
            wtr.write_record(&row)
                .map_err(|e| LibraryError::PickListCsv(e.to_string()))?;
        }
        wtr.flush().map_err(|e| LibraryError::PickListCsv(e.to_string()))?;
        Ok(())
    }

    pub fn write_csv(&self, path: impl AsRef<Path>) -> Result<(), LibraryError> {
        let f = File::create(path.as_ref())?;
        self.to_csv_writer(f)
    }

    fn used_columns(&self) -> HashMap<&'static str, bool> {
        let mut m: HashMap<&'static str, bool> = HashMap::new();
        // Required columns always written.
        for &k in &[
            col::SOURCE_PLATE_NAME,
            col::SOURCE_WELL,
            col::DEST_PLATE_NAME,
            col::DEST_WELL,
            col::TRANSFER_VOLUME,
        ] {
            m.insert(k, true);
        }
        for t in &self.transfers {
            if t.source_plate_type.is_some() {
                m.insert(col::SOURCE_PLATE_TYPE, true);
            }
            if t.source_plate_barcode.is_some() {
                m.insert(col::SOURCE_PLATE_BARCODE, true);
            }
            if t.sample_name.is_some() {
                m.insert(col::SAMPLE_NAME, true);
            }
            if t.source_concentration.is_some() {
                m.insert(col::SOURCE_CONCENTRATION, true);
            }
            if t.source_concentration_units.is_some() {
                m.insert(col::SOURCE_CONCENTRATION_UNITS, true);
            }
            if t.destination_plate_type.is_some() {
                m.insert(col::DEST_PLATE_TYPE, true);
            }
            if t.destination_plate_barcode.is_some() {
                m.insert(col::DEST_PLATE_BARCODE, true);
            }
            if t.destination_sample_name.is_some() {
                m.insert(col::DEST_SAMPLE_NAME, true);
            }
            if t.destination_concentration.is_some() {
                m.insert(col::DEST_CONCENTRATION, true);
            }
            if t.destination_concentration_units.is_some() {
                m.insert(col::DEST_CONCENTRATION_UNITS, true);
            }
        }
        m
    }

    // ----- Graph construction --------------------------------------------

    /// Source-plate → destination-plate digraph. Edge weight is
    /// `(total_volume, n_txs)` summed over all transfers between the pair.
    pub fn plate_transfer_graph(&self) -> DiGraph<String, PlateEdge> {
        let mut graph: DiGraph<String, PlateEdge> = DiGraph::new();
        let mut nodes: HashMap<String, NodeIndex> = HashMap::new();
        let mut edges: HashMap<(NodeIndex, NodeIndex), PlateEdge> = HashMap::new();

        for t in &self.transfers {
            let s = *nodes
                .entry(t.source_plate_name.clone())
                .or_insert_with(|| graph.add_node(t.source_plate_name.clone()));
            let d = *nodes
                .entry(t.destination_plate_name.clone())
                .or_insert_with(|| graph.add_node(t.destination_plate_name.clone()));
            let e = edges.entry((s, d)).or_insert_with(|| PlateEdge { total_volume: 0.0, n_txs: 0 });
            e.total_volume += t.transfer_volume;
            e.n_txs += 1;
        }
        for ((s, d), w) in edges {
            graph.add_edge(s, d, w);
        }
        graph
    }

    /// (plate, well) → (plate, well) multi-edge graph. Each transfer row
    /// becomes one directed edge; parallel edges are permitted.
    pub fn well_transfer_multigraph(&self) -> DiGraph<(String, String), f64> {
        let mut graph: DiGraph<(String, String), f64> = DiGraph::new();
        let mut nodes: HashMap<(String, String), NodeIndex> = HashMap::new();
        for t in &self.transfers {
            let s_key = (t.source_plate_name.clone(), t.source_well.clone());
            let d_key = (t.destination_plate_name.clone(), t.destination_well.clone());
            let s = *nodes.entry(s_key.clone()).or_insert_with(|| graph.add_node(s_key));
            let d = *nodes.entry(d_key.clone()).or_insert_with(|| graph.add_node(d_key));
            graph.add_edge(s, d, t.transfer_volume);
        }
        graph
    }

    pub fn plate_transfer_is_dag(&self) -> bool {
        !is_cyclic_directed(&self.plate_transfer_graph())
    }

    pub fn well_transfer_is_dag(&self) -> bool {
        !is_cyclic_directed(&self.well_transfer_multigraph())
    }

    // ----- Simple derived views -----------------------------------------

    /// Unique plate names appearing as either source or destination, in
    /// first-encounter order.
    pub fn all_plate_names(&self) -> Vec<String> {
        let mut seen: BTreeSet<String> = BTreeSet::new();
        let mut out: Vec<String> = Vec::new();
        for t in &self.transfers {
            if seen.insert(t.source_plate_name.clone()) {
                out.push(t.source_plate_name.clone());
            }
        }
        for t in &self.transfers {
            if seen.insert(t.destination_plate_name.clone()) {
                out.push(t.destination_plate_name.clone());
            }
        }
        out
    }

    /// Transfers whose source well is never the destination of any other
    /// transfer — i.e. "leaf" source wells on the transfer DAG.
    pub fn non_intermediate_transfers(&self) -> Vec<Transfer> {
        let dests: BTreeSet<(String, String)> = self
            .transfers
            .iter()
            .map(|t| (t.destination_plate_name.clone(), t.destination_well.clone()))
            .collect();
        self.transfers
            .iter()
            .filter(|t| {
                !dests.contains(&(t.source_plate_name.clone(), t.source_well.clone()))
            })
            .cloned()
            .collect()
    }

    /// Assign a segment index to each row. A new segment starts whenever
    /// either the source plate name or the destination plate name differs
    /// from the previous row. Matches `with_segment_index` in the Python
    /// picklist.
    pub fn segment_indices(&self) -> Vec<u32> {
        let mut out = Vec::with_capacity(self.transfers.len());
        let mut last_src: Option<&str> = None;
        let mut last_dst: Option<&str> = None;
        let mut idx: u32 = 0;
        for t in &self.transfers {
            let changed = last_src != Some(t.source_plate_name.as_str())
                || last_dst != Some(t.destination_plate_name.as_str());
            if changed {
                if last_src.is_some() || last_dst.is_some() {
                    idx += 1;
                }
                last_src = Some(t.source_plate_name.as_str());
                last_dst = Some(t.destination_plate_name.as_str());
            }
            out.push(idx);
        }
        out
    }

    // ----- Quick well-transfer ordering ---------------------------------

    /// Reorder transfers within each segment in a snake pattern: sort by
    /// source row, zig-zag source column by row parity, then destination
    /// row, zig-zag destination column by row parity. This matches the
    /// Python `_optimize_well_transfer_order_quick` heuristic and gives a
    /// cheap, no-TSP ordering that's significantly faster than lex order
    /// on the Echo.
    pub fn optimize_well_transfer_order_quick(&self) -> Result<PickList, LibraryError> {
        let seg = self.segment_indices();
        let mut indexed: Vec<(usize, u32, i32, i32, i32, i32)> =
            Vec::with_capacity(self.transfers.len());

        for (i, t) in self.transfers.iter().enumerate() {
            let (sr, sc) = parse_well(&t.source_well)?;
            let (dr, dc) = parse_well(&t.destination_well)?;
            indexed.push((i, seg[i], sr, sc, dr, dc));
        }

        indexed.sort_by(|a, b| {
            let (_, a_seg, a_sr, a_sc, a_dr, a_dc) = *a;
            let (_, b_seg, b_sr, b_sc, b_dr, b_dc) = *b;
            let a_sc_eff = if a_sr % 2 == 0 { a_sc } else { -a_sc };
            let b_sc_eff = if b_sr % 2 == 0 { b_sc } else { -b_sc };
            let a_dc_eff = if a_dr % 2 == 0 { a_dc } else { -a_dc };
            let b_dc_eff = if b_dr % 2 == 0 { b_dc } else { -b_dc };
            a_seg
                .cmp(&b_seg)
                .then(a_sr.cmp(&b_sr))
                .then(a_sc_eff.cmp(&b_sc_eff))
                .then(a_dr.cmp(&b_dr))
                .then(a_dc_eff.cmp(&b_dc_eff))
        });

        let transfers: Vec<Transfer> = indexed
            .into_iter()
            .map(|(i, ..)| self.transfers[i].clone())
            .collect();

        Ok(PickList { transfers, extra_headers: self.extra_headers.clone() })
    }

    // ----- Validation ---------------------------------------------------

    /// Check the picklist for errors and potential problems.
    ///
    /// Mirrors the Python `PickList.validate()` but returns lists of
    /// error/warning strings rather than printing or raising. Caller can
    /// inspect and decide whether to abort.
    ///
    /// `survey_volumes` is an optional per-plate map of well → current
    /// volume in **nanolitres** (the same unit as `Transfer Volume`). If
    /// `None`, surveys are treated as absent for all plates.
    pub fn validate(
        &self,
        labware: &crate::labware::Labware,
        survey_volumes: Option<&HashMap<String, HashMap<String, f64>>>,
    ) -> ValidationReport {
        let mut errors: Vec<String> = Vec::new();
        let mut warnings: Vec<String> = Vec::new();

        // --- 1. Plate-name → plate-type consistency.
        if let Err(msg) = self.check_plate_name_type_consistency() {
            errors.push(msg);
        }

        // --- 2. Labware lookup + usage check. Collect per-plate info so
        //        we have drop_volume/min_well_vol/etc for later checks.
        let plate_info = match self.collect_plate_info(labware) {
            Ok(p) => p,
            Err(msg) => {
                errors.push(msg);
                return ValidationReport { errors, warnings };
            }
        };

        // --- 3 & 4. Drop-volume modulus + zero-volume checks.
        for (i, t) in self.transfers.iter().enumerate() {
            let src_info = plate_info.get(&t.source_plate_name);
            if t.transfer_volume == 0.0 {
                errors.push(format!(
                    "row {}: transfer volume is zero ({} {} → {} {})",
                    i + 1,
                    t.source_plate_name,
                    t.source_well,
                    t.destination_plate_name,
                    t.destination_well,
                ));
            }
            if let Some(pi) = src_info
                && let Some(dv) = pi.drop_volume
                && dv > 0.0
            {
                let rem = t.transfer_volume.rem_euclid(dv);
                // Allow tiny FP slop around zero or a full drop.
                if rem > 1e-6 && (dv - rem).abs() > 1e-6 {
                    errors.push(format!(
                        "row {}: transfer volume {} nL is not a multiple of drop volume {} nL ({} {} → {} {})",
                        i + 1,
                        t.transfer_volume,
                        dv,
                        t.source_plate_name,
                        t.source_well,
                        t.destination_plate_name,
                        t.destination_well,
                    ));
                }
            }
        }

        // --- 5. Well-transfer graph DAG + topological ordering check.
        let well_graph = self.well_transfer_multigraph();
        if is_cyclic_directed(&well_graph) {
            warnings.push("Well transfer multigraph has a cycle".to_string());
        } else if let Some(layers) = self.well_topological_generations() {
            let mut gen_of: HashMap<(String, String), usize> = HashMap::new();
            for (g, layer) in layers.iter().enumerate() {
                for node in layer {
                    gen_of.insert(node.clone(), g);
                }
            }
            let src_gens: Vec<usize> = self
                .transfers
                .iter()
                .map(|t| {
                    *gen_of
                        .get(&(t.source_plate_name.clone(), t.source_well.clone()))
                        .unwrap_or(&0)
                })
                .collect();
            let dst_gens: Vec<usize> = self
                .transfers
                .iter()
                .map(|t| {
                    *gen_of
                        .get(&(
                            t.destination_plate_name.clone(),
                            t.destination_well.clone(),
                        ))
                        .unwrap_or(&0)
                })
                .collect();
            // Suffix-min of destination generations: min_{j >= i} dst_gens[j].
            let n = self.transfers.len();
            let mut suffix_min: Vec<usize> = vec![usize::MAX; n];
            let mut running = usize::MAX;
            for i in (0..n).rev() {
                running = running.min(dst_gens[i]);
                suffix_min[i] = running;
            }
            let bad: Vec<usize> = (0..n)
                .filter(|&i| src_gens[i] >= suffix_min[i])
                .collect();
            if !bad.is_empty() {
                errors.push(format!(
                    "Transfers are not topologically ordered (first offending row {})",
                    bad[0] + 1
                ));
            }
        }

        // --- 6. Destination Sample Name uniqueness per dest well.
        self.check_destination_sample_names(&mut errors);

        // --- 7. Per-plate volume bookkeeping.
        self.check_plate_volumes(&plate_info, survey_volumes, &mut warnings);

        ValidationReport { errors, warnings }
    }

    fn check_plate_name_type_consistency(&self) -> Result<(), String> {
        let mut seen: HashMap<&str, Option<&str>> = HashMap::new();
        for t in &self.transfers {
            for (name, ty) in [
                (t.source_plate_name.as_str(), t.source_plate_type.as_deref()),
                (
                    t.destination_plate_name.as_str(),
                    t.destination_plate_type.as_deref(),
                ),
            ] {
                if let Some(new_ty) = ty {
                    match seen.get(name) {
                        None => {
                            seen.insert(name, Some(new_ty));
                        }
                        Some(None) => {
                            seen.insert(name, Some(new_ty));
                        }
                        Some(Some(existing)) => {
                            if *existing != new_ty {
                                return Err(format!(
                                    "Plate {name:?} appears with multiple plate types: {existing:?} and {new_ty:?}"
                                ));
                            }
                        }
                    }
                } else {
                    seen.entry(name).or_insert(None);
                }
            }
        }
        Ok(())
    }

    /// Resolve each plate name that appears in the picklist to its
    /// `PlateInfo` from `labware`, also checking that source plates have
    /// `usage == "SRC"` and destination plates have `usage == "DEST"`.
    fn collect_plate_info<'lw>(
        &self,
        labware: &'lw crate::labware::Labware,
    ) -> Result<HashMap<String, &'lw crate::labware::PlateInfo>, String> {
        let mut out: HashMap<String, &crate::labware::PlateInfo> = HashMap::new();
        let mut used_as_source: BTreeSet<String> = BTreeSet::new();
        let mut used_as_dest: BTreeSet<String> = BTreeSet::new();

        for t in &self.transfers {
            if let Some(ty) = &t.source_plate_type {
                used_as_source.insert(t.source_plate_name.clone());
                let pi = labware.get(ty).ok_or_else(|| {
                    format!(
                        "Source plate type {ty:?} (on plate {:?}) not found in labware",
                        t.source_plate_name
                    )
                })?;
                if pi.usage != "SRC" {
                    return Err(format!(
                        "Plate type {ty:?} used as source but has usage={:?}",
                        pi.usage
                    ));
                }
                out.insert(t.source_plate_name.clone(), pi);
            }
            if let Some(ty) = &t.destination_plate_type {
                used_as_dest.insert(t.destination_plate_name.clone());
                let pi = labware.get(ty).ok_or_else(|| {
                    format!(
                        "Destination plate type {ty:?} (on plate {:?}) not found in labware",
                        t.destination_plate_name
                    )
                })?;
                if pi.usage != "DEST" {
                    return Err(format!(
                        "Plate type {ty:?} used as destination but has usage={:?}",
                        pi.usage
                    ));
                }
                out.insert(t.destination_plate_name.clone(), pi);
            }
        }
        Ok(out)
    }

    fn check_destination_sample_names(&self, errors: &mut Vec<String>) {
        let mut per_well: HashMap<(String, String), BTreeSet<String>> = HashMap::new();
        for t in &self.transfers {
            if let Some(name) = &t.destination_sample_name {
                per_well
                    .entry((t.destination_plate_name.clone(), t.destination_well.clone()))
                    .or_default()
                    .insert(name.clone());
            }
        }
        let mut offenders: Vec<String> = per_well
            .into_iter()
            .filter(|(_, names)| names.len() > 1)
            .map(|((plate, well), names)| {
                let names: Vec<_> = names.into_iter().collect();
                format!("{plate} {well}: {names:?}")
            })
            .collect();
        offenders.sort();
        if !offenders.is_empty() {
            errors.push(format!(
                "Multiple sample names found in well(s): {}",
                offenders.join(", ")
            ));
        }
    }

    fn check_plate_volumes(
        &self,
        plate_info: &HashMap<String, &crate::labware::PlateInfo>,
        survey_volumes: Option<&HashMap<String, HashMap<String, f64>>>,
        warnings: &mut Vec<String>,
    ) {
        // Build per-plate list of events: (well, signed volume change, kind).
        // "kind" = "source" | "dest"; then prepend survey seed values as
        // positive pseudo-transfers if present.
        let plate_names = self.all_plate_names();
        for p in plate_names {
            // Collect rows where this plate is source or destination, in
            // file order.
            let mut events: Vec<PlateEvent> = Vec::new();
            let mut first_use_is_source: Option<bool> = None;
            for (i, t) in self.transfers.iter().enumerate() {
                if t.source_plate_name == p {
                    events.push(PlateEvent {
                        well: t.source_well.clone(),
                        change_nl: -t.transfer_volume,
                        is_source: true,
                        row_ix: i,
                    });
                    first_use_is_source.get_or_insert(true);
                }
                if t.destination_plate_name == p {
                    events.push(PlateEvent {
                        well: t.destination_well.clone(),
                        change_nl: t.transfer_volume,
                        is_source: false,
                        row_ix: i,
                    });
                    first_use_is_source.get_or_insert(false);
                }
            }
            let source_first = first_use_is_source.unwrap_or(false);

            // Survey seed values come first so running sums start from them.
            let have_survey = if let Some(sv) = survey_volumes
                && let Some(well_map) = sv.get(&p)
            {
                for (well, vol_nl) in well_map {
                    events.insert(
                        0,
                        PlateEvent {
                            well: well.clone(),
                            change_nl: *vol_nl,
                            is_source: false,
                            row_ix: usize::MAX,
                        },
                    );
                }
                true
            } else {
                if source_first {
                    warnings.push(format!("No survey data for {p}"));
                }
                false
            };

            // Running volume per well.
            let mut running: HashMap<String, f64> = HashMap::new();
            // Retrieve source-plate limits (as nL).
            let src_info = plate_info.get(&p);
            let (min_well_nl, max_well_nl, max_vol_total) = match src_info {
                Some(pi) => (
                    pi.min_well_vol.map(|v| v * 1000.0),
                    pi.max_well_vol.map(|v| v * 1000.0),
                    pi.max_vol_total,
                ),
                None => (None, None, None),
            };

            for ev in &events {
                let before = *running.get(&ev.well).unwrap_or(&0.0);
                let after = before + ev.change_nl;

                if ev.is_source {
                    if let Some(maxv) = max_well_nl
                        && before > maxv
                    {
                        warnings.push(format!(
                            "{p} {} is above max_well_vol ({before} nL > {maxv} nL) before transfer at row {}",
                            ev.well,
                            ev.row_ix + 1,
                        ));
                    }
                    if let Some(mvt) = max_vol_total
                        && -ev.change_nl > mvt
                    {
                        warnings.push(format!(
                            "{p} {} has a transfer above max_vol_total ({} nL < -{mvt} nL) at row {}",
                            ev.well,
                            ev.change_nl,
                            ev.row_ix + 1,
                        ));
                    }
                    if have_survey || !source_first {
                        if let Some(minv) = min_well_nl
                            && after < minv
                        {
                            warnings.push(format!(
                                "{p} {} goes below min_well_vol ({after} nL < {minv} nL) at row {}",
                                ev.well,
                                ev.row_ix + 1,
                            ));
                        }
                    } else if let (Some(minv), Some(maxv)) = (min_well_nl, max_well_nl)
                        && before < (minv - maxv)
                    {
                        warnings.push(format!(
                            "{p} {} (no survey) net outflow below min_well_vol-max_well_vol ({before} nL < {} nL) at row {}",
                            ev.well,
                            minv - maxv,
                            ev.row_ix + 1,
                        ));
                    }
                }

                running.insert(ev.well.clone(), after);
            }
        }
    }

    // ----- Topological layer computation (for validate) -----------------

    /// Compute topological "generations" (layers) of the well-transfer
    /// multigraph. Generation 0 contains all source-only wells; each later
    /// generation contains wells whose incoming transfers all come from
    /// earlier generations. Returns `None` if the graph has a cycle.
    pub fn well_topological_generations(&self) -> Option<Vec<Vec<(String, String)>>> {
        let graph = self.well_transfer_multigraph();
        toposort(&graph, None).ok()?;

        // Simple layered toposort: repeatedly peel nodes with in-degree 0.
        use petgraph::Direction::Incoming;
        let mut in_deg: HashMap<NodeIndex, usize> = HashMap::new();
        for n in graph.node_indices() {
            in_deg.insert(n, graph.neighbors_directed(n, Incoming).count());
        }
        let mut layers: Vec<Vec<(String, String)>> = Vec::new();
        while !in_deg.is_empty() {
            let ready: Vec<NodeIndex> = in_deg
                .iter()
                .filter_map(|(n, d)| if *d == 0 { Some(*n) } else { None })
                .collect();
            if ready.is_empty() {
                return None;
            }
            let mut layer: Vec<(String, String)> = Vec::with_capacity(ready.len());
            for n in &ready {
                layer.push(graph[*n].clone());
                in_deg.remove(n);
            }
            // Stable ordering within a layer for reproducibility.
            layer.sort();
            for n in &ready {
                for m in graph.neighbors_directed(*n, petgraph::Direction::Outgoing) {
                    if let Some(d) = in_deg.get_mut(&m) {
                        *d = d.saturating_sub(1);
                    }
                }
            }
            layers.push(layer);
        }
        Some(layers)
    }
}

// ---------------------------------------------------------------------------
// Helpers

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct PlateEdge {
    pub total_volume: f64,
    pub n_txs: u64,
}

/// Output of [`PickList::validate`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ValidationReport {
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}

impl ValidationReport {
    pub fn ok(&self) -> bool {
        self.errors.is_empty()
    }
}

#[derive(Debug, Clone)]
struct PlateEvent {
    well: String,
    change_nl: f64,
    is_source: bool,
    /// 0-based row index in the picklist, or `usize::MAX` for a survey seed.
    row_ix: usize,
}

impl Transfer {
    /// Read a single column by canonical Echo name. Returns the owned
    /// string form, or `None` for empty/absent.
    pub fn get_column(&self, name: &str) -> Option<String> {
        match name {
            col::SOURCE_PLATE_NAME => Some(self.source_plate_name.clone()),
            col::SOURCE_WELL => Some(self.source_well.clone()),
            col::DEST_PLATE_NAME => Some(self.destination_plate_name.clone()),
            col::DEST_WELL => Some(self.destination_well.clone()),
            col::TRANSFER_VOLUME => Some(format_vol(self.transfer_volume)),
            col::SOURCE_PLATE_TYPE => self.source_plate_type.clone(),
            col::SOURCE_PLATE_BARCODE => self.source_plate_barcode.clone(),
            col::SAMPLE_NAME => self.sample_name.clone(),
            col::SOURCE_CONCENTRATION => self.source_concentration.map(format_vol),
            col::SOURCE_CONCENTRATION_UNITS => self.source_concentration_units.clone(),
            col::DEST_PLATE_TYPE => self.destination_plate_type.clone(),
            col::DEST_PLATE_BARCODE => self.destination_plate_barcode.clone(),
            col::DEST_SAMPLE_NAME => self.destination_sample_name.clone(),
            col::DEST_CONCENTRATION => self.destination_concentration.map(format_vol),
            col::DEST_CONCENTRATION_UNITS => self.destination_concentration_units.clone(),
            _ => self.extra.get(name).cloned(),
        }
    }
}

fn format_vol(v: f64) -> String {
    // Match polars default CSV formatting: full precision, but trim
    // trailing zeros to avoid "25.0" → "25.0" vs "25" mismatch on
    // integer-valued nanolitres. `{}` Display already does the right
    // thing for this.
    format!("{v}")
}

/// Parse a well name like "A1", "P23" into zero-based (row, column).
pub fn parse_well(well: &str) -> Result<(i32, i32), LibraryError> {
    let bytes = well.as_bytes();
    if bytes.is_empty() {
        return Err(LibraryError::InvalidWellName(well.to_string()));
    }
    let row_byte = bytes[0];
    if !row_byte.is_ascii_uppercase() {
        return Err(LibraryError::InvalidWellName(well.to_string()));
    }
    let row = (row_byte - b'A') as i32;
    let col_str = std::str::from_utf8(&bytes[1..])
        .map_err(|_| LibraryError::InvalidWellName(well.to_string()))?;
    let col: i32 = col_str
        .parse()
        .map_err(|_| LibraryError::InvalidWellName(well.to_string()))?;
    if col < 1 {
        return Err(LibraryError::InvalidWellName(well.to_string()));
    }
    Ok((row, col - 1))
}

// ---------------------------------------------------------------------------
// Tests

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_CSV: &str = "\
Source Plate Name,Source Plate Type,Source Well,Destination Plate Name,Destination Plate Type,Destination Well,Transfer Volume
SrcA,384PP_AQ_BP,A1,DstA,384LDV_Plus_AQ_GP,B2,25
SrcA,384PP_AQ_BP,A2,DstA,384LDV_Plus_AQ_GP,B3,50
SrcB,384PP_AQ_BP,C5,DstA,384LDV_Plus_AQ_GP,D7,75
";

    #[test]
    fn read_csv_basic() {
        let pl = PickList::from_csv_reader(SAMPLE_CSV.as_bytes()).unwrap();
        assert_eq!(pl.len(), 3);
        assert_eq!(pl.transfers[0].source_plate_name, "SrcA");
        assert_eq!(pl.transfers[0].source_well, "A1");
        assert_eq!(pl.transfers[0].transfer_volume, 25.0);
        assert_eq!(pl.transfers[0].source_plate_type.as_deref(), Some("384PP_AQ_BP"));
    }

    #[test]
    fn write_csv_round_trips() {
        let pl = PickList::from_csv_reader(SAMPLE_CSV.as_bytes()).unwrap();
        let mut out: Vec<u8> = Vec::new();
        pl.to_csv_writer(&mut out).unwrap();
        let pl2 = PickList::from_csv_reader(out.as_slice()).unwrap();
        assert_eq!(pl, pl2);
    }

    #[test]
    fn unknown_columns_preserved() {
        let csv = "\
Source Plate Name,Source Well,Destination Plate Name,Destination Well,Transfer Volume,Note
SrcA,A1,DstA,B2,25,hello
SrcA,A2,DstA,B3,50,world
";
        let pl = PickList::from_csv_reader(csv.as_bytes()).unwrap();
        assert_eq!(pl.transfers[0].extra.get("Note").map(|s| s.as_str()), Some("hello"));
        let mut out: Vec<u8> = Vec::new();
        pl.to_csv_writer(&mut out).unwrap();
        let written = String::from_utf8(out).unwrap();
        assert!(written.contains("Note"));
        assert!(written.contains("hello"));
    }

    #[test]
    fn plate_transfer_graph_counts() {
        let pl = PickList::from_csv_reader(SAMPLE_CSV.as_bytes()).unwrap();
        let g = pl.plate_transfer_graph();
        assert_eq!(g.node_count(), 3); // SrcA, DstA, SrcB
        assert_eq!(g.edge_count(), 2); // SrcA→DstA, SrcB→DstA
    }

    #[test]
    fn well_transfer_graph_has_one_edge_per_row() {
        let pl = PickList::from_csv_reader(SAMPLE_CSV.as_bytes()).unwrap();
        let g = pl.well_transfer_multigraph();
        assert_eq!(g.edge_count(), 3);
    }

    #[test]
    fn segment_indices_change_on_plate_change() {
        let pl = PickList::from_csv_reader(SAMPLE_CSV.as_bytes()).unwrap();
        let seg = pl.segment_indices();
        assert_eq!(seg, vec![0, 0, 1]);
    }

    #[test]
    fn parse_well_ok() {
        assert_eq!(parse_well("A1").unwrap(), (0, 0));
        assert_eq!(parse_well("P24").unwrap(), (15, 23));
        assert!(parse_well("").is_err());
        assert!(parse_well("a1").is_err());
        assert!(parse_well("A0").is_err());
    }

    // Minimal labware hand-built for validate tests (avoid tying these to
    // the real ELWX fixture, which has many unrelated plates).
    fn tiny_labware() -> crate::labware::Labware {
        use crate::labware::{Labware, PlateInfo};
        let src = PlateInfo {
            plate_type: "384PP_AQ_BP".into(),
            plate_format: "384PP".into(),
            usage: "SRC".into(),
            fluid: None,
            manufacturer: "test".into(),
            lot_number: "lot".into(),
            part_number: "part".into(),
            rows: 16,
            cols: 24,
            a1_offset_y: 0,
            center_spacing_x: 450,
            center_spacing_y: 450,
            plate_height: 1440,
            skirt_height: 900,
            well_width: 360,
            well_length: 360,
            well_capacity: 65000,
            bottom_inset: 0.0,
            center_well_pos_x: 0.0,
            center_well_pos_y: 0.0,
            min_well_vol: Some(15.0),  // μL
            max_well_vol: Some(65.0),  // μL
            max_vol_total: Some(1000.0),
            min_volume: Some(25.0),
            drop_volume: Some(25.0),
        };
        let dst = PlateInfo {
            plate_type: "384LDV_Plus_AQ_GP".into(),
            plate_format: "384LDV".into(),
            usage: "DEST".into(),
            ..src.clone()
        };
        let dst = PlateInfo {
            min_well_vol: Some(2.5),
            max_well_vol: Some(12.0),
            drop_volume: Some(25.0),
            ..dst
        };
        Labware::new(vec![src, dst])
    }

    #[test]
    fn validate_happy_path() {
        let pl = PickList::from_csv_reader(SAMPLE_CSV.as_bytes()).unwrap();
        let lw = tiny_labware();
        let rep = pl.validate(&lw, None);
        // Volumes are all multiples of 25 (drop_volume), so no volume
        // errors; but SrcA has no survey so there should be a warning.
        assert!(rep.errors.is_empty(), "unexpected errors: {:?}", rep.errors);
        assert!(
            rep.warnings.iter().any(|w| w.contains("No survey data")),
            "expected survey-absence warning: {:?}",
            rep.warnings
        );
    }

    #[test]
    fn validate_bad_drop_volume() {
        let csv = "\
Source Plate Name,Source Plate Type,Source Well,Destination Plate Name,Destination Plate Type,Destination Well,Transfer Volume
SrcA,384PP_AQ_BP,A1,DstA,384LDV_Plus_AQ_GP,B2,30
";
        let pl = PickList::from_csv_reader(csv.as_bytes()).unwrap();
        let rep = pl.validate(&tiny_labware(), None);
        assert!(
            rep.errors.iter().any(|e| e.contains("not a multiple of drop volume")),
            "expected drop-volume error: {:?}",
            rep.errors
        );
    }

    #[test]
    fn validate_zero_volume() {
        let csv = "\
Source Plate Name,Source Plate Type,Source Well,Destination Plate Name,Destination Plate Type,Destination Well,Transfer Volume
SrcA,384PP_AQ_BP,A1,DstA,384LDV_Plus_AQ_GP,B2,0
";
        let pl = PickList::from_csv_reader(csv.as_bytes()).unwrap();
        let rep = pl.validate(&tiny_labware(), None);
        assert!(
            rep.errors.iter().any(|e| e.contains("transfer volume is zero")),
            "expected zero-volume error: {:?}",
            rep.errors
        );
    }

    #[test]
    fn validate_wrong_usage() {
        // Use a SRC-only plate type as a destination.
        let csv = "\
Source Plate Name,Source Plate Type,Source Well,Destination Plate Name,Destination Plate Type,Destination Well,Transfer Volume
SrcA,384LDV_Plus_AQ_GP,A1,DstA,384PP_AQ_BP,B2,25
";
        let pl = PickList::from_csv_reader(csv.as_bytes()).unwrap();
        let rep = pl.validate(&tiny_labware(), None);
        // collect_plate_info returns early with an error — message mentions usage.
        assert!(
            rep.errors.iter().any(|e| e.contains("usage")),
            "expected usage error: {:?}",
            rep.errors
        );
    }

    #[test]
    fn validate_dest_sample_name_conflict() {
        let csv = "\
Source Plate Name,Source Plate Type,Source Well,Destination Plate Name,Destination Plate Type,Destination Well,Destination Sample Name,Transfer Volume
SrcA,384PP_AQ_BP,A1,DstA,384LDV_Plus_AQ_GP,B2,alpha,25
SrcA,384PP_AQ_BP,A2,DstA,384LDV_Plus_AQ_GP,B2,beta,25
";
        let pl = PickList::from_csv_reader(csv.as_bytes()).unwrap();
        let rep = pl.validate(&tiny_labware(), None);
        assert!(
            rep.errors.iter().any(|e| e.contains("Multiple sample names")),
            "expected dest-sample-name error: {:?}",
            rep.errors
        );
    }

    #[test]
    fn quick_order_is_stable_and_nonempty() {
        // Eight transfers across two rows in a single segment.
        let csv = "\
Source Plate Name,Source Well,Destination Plate Name,Destination Well,Transfer Volume
S,A1,D,A1,25
S,A2,D,A2,25
S,A3,D,A3,25
S,B1,D,B1,25
S,B2,D,B2,25
S,B3,D,B3,25
";
        let pl = PickList::from_csv_reader(csv.as_bytes()).unwrap();
        let opt = pl.optimize_well_transfer_order_quick().unwrap();
        assert_eq!(opt.len(), pl.len());
        // Row A (even parity) ascending column, then row B (odd parity)
        // descending column.
        let seq: Vec<_> = opt.transfers.iter().map(|t| t.source_well.clone()).collect();
        assert_eq!(seq, vec!["A1", "A2", "A3", "B3", "B2", "B1"]);
    }
}
