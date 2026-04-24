use std::fs::File;
use std::collections::HashMap;
use std::path::Path;

use chrono::{DateTime, NaiveDateTime, Utc};
use polars::prelude::{
    AnyValue, DataFrame, DataType, Field, NamedFrom, ParquetReader, ParquetWriter, SerReader,
    Series, TimeUnit,
};

use super::platesurvey::{EchoSignal, PlateSurvey, SignalFeature, WellSurvey};
use crate::LibraryError;

pub fn read_survey_parquet(path: impl AsRef<Path>) -> Result<PlateSurvey, LibraryError> {
    let file = File::open(path)?;
    let df = ParquetReader::new(file).finish()?;
    survey_from_dataframe(&df)
}

pub fn read_validation_volumes_parquet(
    path: impl AsRef<Path>,
) -> Result<HashMap<String, HashMap<String, f64>>, LibraryError> {
    let file = File::open(path)?;
    let df = ParquetReader::new(file).finish()?;
    validation_volumes_from_dataframe(&df)
}

pub fn write_survey_parquet(
    path: impl AsRef<Path>,
    survey: &PlateSurvey,
) -> Result<(), LibraryError> {
    let mut file = File::create(path)?;
    let mut df = dataframe_from_survey(survey)?;
    ParquetWriter::new(&mut file).finish(&mut df)?;
    Ok(())
}

pub fn write_survey_csv(path: impl AsRef<Path>, survey: &PlateSurvey) -> Result<(), LibraryError> {
    let mut writer = csv::Writer::from_path(path).map_err(csv_err)?;
    writer.write_record([
        "row",
        "column",
        "well",
        "volume",
        "current_volume",
        "status",
        "fluid",
        "fluid_units",
        "meniscus_x",
        "meniscus_y",
        "fluid_composition",
        "dmso_homogeneous",
        "dmso_inhomogeneous",
        "fluid_thickness",
        "current_fluid_thickness",
        "bottom_thickness",
        "fluid_thickness_homogeneous",
        "fluid_thickness_imhomogeneous",
        "outlier",
        "corrective_action",
        "plate_type",
        "plate_barcode",
        "timestamp",
        "instrument_serial_number",
        "vtl",
        "original",
        "data_format_version",
        "survey_rows",
        "survey_columns",
        "survey_total_wells",
        "plate_name",
        "comment",
    ])
    .map_err(csv_err)?;
    for well in &survey.wells {
        writer
            .write_record([
                well.row.to_string(),
                well.column.to_string(),
                well.well.clone(),
                opt_num(well.volume),
                opt_num(well.current_volume),
                well.status.clone(),
                well.fluid.clone(),
                well.fluid_units.clone(),
                well.meniscus_x.to_string(),
                well.meniscus_y.to_string(),
                well.fluid_composition.to_string(),
                well.dmso_homogeneous.to_string(),
                well.dmso_inhomogeneous.to_string(),
                well.fluid_thickness.to_string(),
                well.current_fluid_thickness.to_string(),
                well.bottom_thickness.to_string(),
                well.fluid_thickness_homogeneous.to_string(),
                well.fluid_thickness_imhomogeneous.to_string(),
                well.outlier.to_string(),
                well.corrective_action.clone(),
                survey.plate_type.clone(),
                survey.plate_barcode.clone().unwrap_or_default(),
                survey.timestamp.format("%Y-%m-%d %H:%M:%S%.f").to_string(),
                survey.instrument_serial_number.clone(),
                survey.vtl.to_string(),
                survey.original.to_string(),
                survey.data_format_version.to_string(),
                survey.survey_rows.to_string(),
                survey.survey_columns.to_string(),
                survey.survey_total_wells.to_string(),
                survey.plate_name.clone().unwrap_or_default(),
                survey.comment.clone().unwrap_or_default(),
            ])
            .map_err(csv_err)?;
    }
    writer.flush()?;
    Ok(())
}

fn survey_from_dataframe(df: &DataFrame) -> Result<PlateSurvey, LibraryError> {
    if df.height() == 0 {
        return Err(LibraryError::EmptySurveyData);
    }
    ensure_single_survey(df)?;

    let mut wells = Vec::with_capacity(df.height());
    for idx in 0..df.height() {
        wells.push(WellSurvey {
            row: required_i64(df, "row", idx)? as u32,
            column: required_i64(df, "column", idx)? as u32,
            well: required_string(df, "well", idx)?,
            volume: optional_f64(df, "volume", idx)?,
            current_volume: optional_f64(df, "current_volume", idx)?,
            status: optional_string(df, "status", idx)?.unwrap_or_default(),
            fluid: optional_string(df, "fluid", idx)?.unwrap_or_default(),
            fluid_units: optional_string(df, "fluid_units", idx)?.unwrap_or_default(),
            meniscus_x: optional_f64(df, "meniscus_x", idx)?.unwrap_or_default(),
            meniscus_y: optional_f64(df, "meniscus_y", idx)?.unwrap_or_default(),
            fluid_composition: optional_f64(df, "fluid_composition", idx)?.unwrap_or_default(),
            dmso_homogeneous: optional_f64(df, "dmso_homogeneous", idx)?.unwrap_or_default(),
            dmso_inhomogeneous: optional_f64(df, "dmso_inhomogeneous", idx)?.unwrap_or_default(),
            fluid_thickness: optional_f64(df, "fluid_thickness", idx)?.unwrap_or_default(),
            current_fluid_thickness: optional_f64(df, "current_fluid_thickness", idx)?
                .unwrap_or_default(),
            bottom_thickness: optional_f64(df, "bottom_thickness", idx)?.unwrap_or_default(),
            fluid_thickness_homogeneous: optional_f64(
                df,
                "fluid_thickness_homogeneous",
                idx,
            )?
            .unwrap_or_default(),
            fluid_thickness_imhomogeneous: optional_f64(
                df,
                "fluid_thickness_imhomogeneous",
                idx,
            )?
            .unwrap_or_default(),
            outlier: optional_f64(df, "outlier", idx)?.unwrap_or_default(),
            corrective_action: optional_string(df, "corrective_action", idx)?.unwrap_or_default(),
            echo_signal: optional_echo_signal(df, idx)?,
        });
    }

    Ok(PlateSurvey {
        plate_type: required_string(df, "plate_type", 0)?,
        plate_barcode: optional_string(df, "plate_barcode", 0)?,
        timestamp: required_timestamp(df, "timestamp", 0)?,
        instrument_serial_number: optional_string(df, "instrument_serial_number", 0)?
            .unwrap_or_default(),
        vtl: optional_i64(df, "vtl", 0)?.unwrap_or_default() as i32,
        original: optional_i64(df, "original", 0)?.unwrap_or_default() as i32,
        data_format_version: optional_i64(df, "data_format_version", 0)?.unwrap_or(1) as i32,
        survey_rows: optional_i64(df, "survey_rows", 0)?.unwrap_or_else(|| inferred_rows(df))
            as i32,
        survey_columns: optional_i64(df, "survey_columns", 0)?
            .unwrap_or_else(|| inferred_columns(df)) as i32,
        survey_total_wells: optional_i64(df, "survey_total_wells", 0)?
            .unwrap_or(df.height() as i64) as i32,
        plate_name: optional_string(df, "plate_name", 0)?,
        comment: optional_string(df, "comment", 0)?,
        wells,
    })
}

fn validation_volumes_from_dataframe(
    df: &DataFrame,
) -> Result<HashMap<String, HashMap<String, f64>>, LibraryError> {
    if df.height() == 0 {
        return Err(LibraryError::EmptySurveyData);
    }

    let mut latest_timestamps: HashMap<String, NaiveDateTime> = HashMap::new();
    let mut volumes: HashMap<String, HashMap<String, f64>> = HashMap::new();

    for idx in 0..df.height() {
        let plate_name = required_string(df, "plate_name", idx)?;
        if plate_name.trim().is_empty() {
            return Err(LibraryError::InvalidSurveyDataValue {
                column: "plate_name".into(),
                value: "<empty>".into(),
            });
        }
        let timestamp = required_timestamp(df, "timestamp", idx)?;
        let well = required_string(df, "well", idx)?;
        let volume_nl = optional_f64(df, "volume", idx)?.map(|v| v * 1000.0);

        match latest_timestamps.get(&plate_name) {
            Some(current) if timestamp < *current => continue,
            Some(current) if timestamp > *current => {
                latest_timestamps.insert(plate_name.clone(), timestamp);
                volumes.insert(plate_name.clone(), HashMap::new());
            }
            None => {
                latest_timestamps.insert(plate_name.clone(), timestamp);
                volumes.insert(plate_name.clone(), HashMap::new());
            }
            _ => {}
        }

        if Some(timestamp) == latest_timestamps.get(&plate_name).copied()
            && let Some(volume_nl) = volume_nl
        {
            volumes
                .entry(plate_name)
                .or_default()
                .insert(well, volume_nl);
        }
    }

    Ok(volumes)
}

fn dataframe_from_survey(survey: &PlateSurvey) -> Result<DataFrame, LibraryError> {
    let n = survey.wells.len();
    let timestamp_series = Series::new(
        "timestamp".into(),
        vec![survey.timestamp.and_utc().timestamp_micros(); n],
    )
    .cast(&DataType::Datetime(TimeUnit::Microseconds, None))?;
    let echo_signal_values = survey
        .wells
        .iter()
        .map(|well| echo_signal_any_value(well.echo_signal.as_ref()))
        .collect::<Vec<_>>();
    let echo_signal_series =
        Series::from_any_values("echo_signal".into(), &echo_signal_values, false)?;

    DataFrame::new(n, vec![
        Series::new(
            "row".into(),
            survey.wells.iter().map(|w| w.row as i64).collect::<Vec<_>>(),
        )
        .into(),
        Series::new(
            "column".into(),
            survey.wells.iter().map(|w| w.column as i64).collect::<Vec<_>>(),
        )
        .into(),
        Series::new(
            "well".into(),
            survey.wells.iter().map(|w| w.well.as_str()).collect::<Vec<_>>(),
        )
        .into(),
        Series::new(
            "volume".into(),
            survey.wells.iter().map(|w| w.volume).collect::<Vec<_>>(),
        )
        .into(),
        Series::new(
            "current_volume".into(),
            survey.wells.iter().map(|w| w.current_volume).collect::<Vec<_>>(),
        )
        .into(),
        Series::new(
            "status".into(),
            survey.wells.iter().map(|w| w.status.as_str()).collect::<Vec<_>>(),
        )
        .into(),
        Series::new(
            "fluid".into(),
            survey.wells.iter().map(|w| w.fluid.as_str()).collect::<Vec<_>>(),
        )
        .into(),
        Series::new(
            "fluid_units".into(),
            survey
                .wells
                .iter()
                .map(|w| w.fluid_units.as_str())
                .collect::<Vec<_>>(),
        )
        .into(),
        Series::new(
            "meniscus_x".into(),
            survey.wells.iter().map(|w| w.meniscus_x).collect::<Vec<_>>(),
        )
        .into(),
        Series::new(
            "meniscus_y".into(),
            survey.wells.iter().map(|w| w.meniscus_y).collect::<Vec<_>>(),
        )
        .into(),
        Series::new(
            "fluid_composition".into(),
            survey
                .wells
                .iter()
                .map(|w| w.fluid_composition)
                .collect::<Vec<_>>(),
        )
        .into(),
        Series::new(
            "dmso_homogeneous".into(),
            survey
                .wells
                .iter()
                .map(|w| w.dmso_homogeneous)
                .collect::<Vec<_>>(),
        )
        .into(),
        Series::new(
            "dmso_inhomogeneous".into(),
            survey
                .wells
                .iter()
                .map(|w| w.dmso_inhomogeneous)
                .collect::<Vec<_>>(),
        )
        .into(),
        Series::new(
            "fluid_thickness".into(),
            survey
                .wells
                .iter()
                .map(|w| w.fluid_thickness)
                .collect::<Vec<_>>(),
        )
        .into(),
        Series::new(
            "current_fluid_thickness".into(),
            survey
                .wells
                .iter()
                .map(|w| w.current_fluid_thickness)
                .collect::<Vec<_>>(),
        )
        .into(),
        Series::new(
            "bottom_thickness".into(),
            survey
                .wells
                .iter()
                .map(|w| w.bottom_thickness)
                .collect::<Vec<_>>(),
        )
        .into(),
        Series::new(
            "fluid_thickness_homogeneous".into(),
            survey
                .wells
                .iter()
                .map(|w| w.fluid_thickness_homogeneous)
                .collect::<Vec<_>>(),
        )
        .into(),
        Series::new(
            "fluid_thickness_imhomogeneous".into(),
            survey
                .wells
                .iter()
                .map(|w| w.fluid_thickness_imhomogeneous)
                .collect::<Vec<_>>(),
        )
        .into(),
        Series::new(
            "outlier".into(),
            survey.wells.iter().map(|w| w.outlier).collect::<Vec<_>>(),
        )
        .into(),
        Series::new(
            "corrective_action".into(),
            survey
                .wells
                .iter()
                .map(|w| w.corrective_action.as_str())
                .collect::<Vec<_>>(),
        )
        .into(),
        echo_signal_series.into(),
        Series::new("plate_type".into(), vec![survey.plate_type.as_str(); n]).into(),
        Series::new(
            "plate_barcode".into(),
            vec![survey.plate_barcode.as_deref(); n],
        )
        .into(),
        timestamp_series.into(),
        Series::new(
            "instrument_serial_number".into(),
            vec![survey.instrument_serial_number.as_str(); n],
        )
        .into(),
        Series::new("vtl".into(), vec![survey.vtl as i64; n]).into(),
        Series::new("original".into(), vec![survey.original as i64; n]).into(),
        Series::new(
            "data_format_version".into(),
            vec![survey.data_format_version as i64; n],
        )
        .into(),
        Series::new("survey_rows".into(), vec![survey.survey_rows as i64; n]).into(),
        Series::new(
            "survey_columns".into(),
            vec![survey.survey_columns as i64; n],
        )
        .into(),
        Series::new(
            "survey_total_wells".into(),
            vec![survey.survey_total_wells as i64; n],
        )
        .into(),
        Series::new("plate_name".into(), vec![survey.plate_name.as_deref(); n]).into(),
        Series::new("comment".into(), vec![survey.comment.as_deref(); n]).into(),
    ])
    .map_err(Into::into)
}

fn ensure_single_survey(df: &DataFrame) -> Result<(), LibraryError> {
    for field in ["timestamp", "plate_type", "plate_name", "plate_barcode"] {
        let Ok(col) = df.column(field) else {
            continue;
        };
        let first = comparable_value(col.get(0)?);
        for idx in 1..df.height() {
            if comparable_value(col.get(idx)?) != first {
                return Err(LibraryError::MultipleSurveysInParquet(field));
            }
        }
    }
    Ok(())
}

fn comparable_value(value: AnyValue<'_>) -> String {
    match value {
        AnyValue::Null => String::new(),
        AnyValue::String(v) => v.to_string(),
        AnyValue::StringOwned(v) => v.to_string(),
        AnyValue::Datetime(v, unit, _) => timestamp_from_epoch(v, unit)
            .map(|dt| dt.and_utc().timestamp_micros().to_string())
            .unwrap_or_default(),
        AnyValue::DatetimeOwned(v, unit, _) => timestamp_from_epoch(v, unit)
            .map(|dt| dt.and_utc().timestamp_micros().to_string())
            .unwrap_or_default(),
        other => other.to_string(),
    }
}

fn required_string(df: &DataFrame, name: &str, idx: usize) -> Result<String, LibraryError> {
    optional_string(df, name, idx)?
        .ok_or_else(|| LibraryError::MissingSurveyDataColumn(name.to_string()))
}

fn optional_string(
    df: &DataFrame,
    name: &str,
    idx: usize,
) -> Result<Option<String>, LibraryError> {
    let Ok(col) = df.column(name) else {
        return Ok(None);
    };
    match col.get(idx)? {
        AnyValue::Null => Ok(None),
        AnyValue::String(v) => Ok(Some(v.to_string())),
        AnyValue::StringOwned(v) => Ok(Some(v.to_string())),
        other => Ok(Some(other.to_string())),
    }
}

fn required_i64(df: &DataFrame, name: &str, idx: usize) -> Result<i64, LibraryError> {
    optional_i64(df, name, idx)?
        .ok_or_else(|| LibraryError::MissingSurveyDataColumn(name.to_string()))
}

fn optional_i64(df: &DataFrame, name: &str, idx: usize) -> Result<Option<i64>, LibraryError> {
    let Ok(col) = df.column(name) else {
        return Ok(None);
    };
    Ok(match col.get(idx)? {
        AnyValue::Null => None,
        AnyValue::Int64(v) => Some(v),
        AnyValue::Int32(v) => Some(v as i64),
        AnyValue::UInt64(v) => Some(v as i64),
        AnyValue::UInt32(v) => Some(v as i64),
        AnyValue::UInt16(v) => Some(v as i64),
        AnyValue::UInt8(v) => Some(v as i64),
        AnyValue::Int16(v) => Some(v as i64),
        AnyValue::Int8(v) => Some(v as i64),
        AnyValue::Float64(v) => Some(v as i64),
        AnyValue::Float32(v) => Some(v as i64),
        other => {
            return Err(LibraryError::InvalidSurveyDataValue {
                column: name.to_string(),
                value: other.to_string(),
            });
        }
    })
}

fn optional_f64(df: &DataFrame, name: &str, idx: usize) -> Result<Option<f64>, LibraryError> {
    let Ok(col) = df.column(name) else {
        return Ok(None);
    };
    Ok(match col.get(idx)? {
        AnyValue::Null => None,
        AnyValue::Float64(v) => Some(v),
        AnyValue::Float32(v) => Some(v as f64),
        AnyValue::Int64(v) => Some(v as f64),
        AnyValue::Int32(v) => Some(v as f64),
        AnyValue::UInt64(v) => Some(v as f64),
        AnyValue::UInt32(v) => Some(v as f64),
        AnyValue::UInt16(v) => Some(v as f64),
        AnyValue::UInt8(v) => Some(v as f64),
        AnyValue::Int16(v) => Some(v as f64),
        AnyValue::Int8(v) => Some(v as f64),
        other => {
            return Err(LibraryError::InvalidSurveyDataValue {
                column: name.to_string(),
                value: other.to_string(),
            });
        }
    })
}

fn required_timestamp(
    df: &DataFrame,
    name: &str,
    idx: usize,
) -> Result<NaiveDateTime, LibraryError> {
    let col = df
        .column(name)
        .map_err(|_| LibraryError::MissingSurveyDataColumn(name.to_string()))?;
    match col.get(idx)? {
        AnyValue::Datetime(v, unit, _) => timestamp_from_epoch(v, unit),
        AnyValue::DatetimeOwned(v, unit, _) => timestamp_from_epoch(v, unit),
        AnyValue::String(v) => parse_timestamp(v),
        AnyValue::StringOwned(v) => parse_timestamp(&v),
        other => Err(LibraryError::InvalidSurveyDataValue {
            column: name.to_string(),
            value: other.to_string(),
        }),
    }
}

fn parse_timestamp(value: &str) -> Result<NaiveDateTime, LibraryError> {
    NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S"))
        .map_err(|e| LibraryError::InvalidTimestamp {
            input: value.to_string(),
            reason: e.to_string(),
        })
}

fn timestamp_from_epoch(value: i64, unit: TimeUnit) -> Result<NaiveDateTime, LibraryError> {
    let dt = match unit {
        TimeUnit::Nanoseconds => DateTime::<Utc>::from_timestamp_nanos(value),
        TimeUnit::Microseconds => DateTime::<Utc>::from_timestamp_micros(value)
            .ok_or_else(|| LibraryError::InvalidTimestamp {
                input: value.to_string(),
                reason: "microsecond timestamp out of range".into(),
            })?,
        TimeUnit::Milliseconds => DateTime::<Utc>::from_timestamp_millis(value)
            .ok_or_else(|| LibraryError::InvalidTimestamp {
                input: value.to_string(),
                reason: "millisecond timestamp out of range".into(),
            })?,
    };
    Ok(dt.naive_utc())
}

fn inferred_rows(df: &DataFrame) -> i64 {
    (0..df.height())
        .filter_map(|idx| optional_i64(df, "row", idx).ok().flatten())
        .max()
        .map(|v| v + 1)
        .unwrap_or(0)
}

fn inferred_columns(df: &DataFrame) -> i64 {
    (0..df.height())
        .filter_map(|idx| optional_i64(df, "column", idx).ok().flatten())
        .max()
        .map(|v| v + 1)
        .unwrap_or(0)
}

fn optional_echo_signal(df: &DataFrame, idx: usize) -> Result<Option<EchoSignal>, LibraryError> {
    let Ok(column) = df.column("echo_signal") else {
        return Ok(None);
    };
    match column.get(idx)?.into_static() {
        AnyValue::Null => Ok(None),
        AnyValue::StructOwned(payload) => Ok(Some(EchoSignal {
            signal_type: struct_string(&payload.0, &payload.1, "signal_type")?,
            transducer_x: struct_f64(&payload.0, &payload.1, "transducer_x")?,
            transducer_y: struct_f64(&payload.0, &payload.1, "transducer_y")?,
            transducer_z: struct_f64(&payload.0, &payload.1, "transducer_z")?,
            features: struct_features(&payload.0, &payload.1, "features")?,
        })),
        other => Err(LibraryError::InvalidSurveyDataValue {
            column: "echo_signal".into(),
            value: other.to_string(),
        }),
    }
}

fn struct_string(
    values: &[AnyValue<'_>],
    fields: &[Field],
    name: &str,
) -> Result<String, LibraryError> {
    match &values[struct_field(fields, name)?] {
        AnyValue::Null => Ok(String::new()),
        AnyValue::String(v) => Ok((*v).to_string()),
        AnyValue::StringOwned(v) => Ok(v.to_string()),
        other => Ok(other.to_string()),
    }
}

fn struct_f64(values: &[AnyValue<'_>], fields: &[Field], name: &str) -> Result<f64, LibraryError> {
    match values[struct_field(fields, name)?] {
        AnyValue::Null => Ok(0.0),
        AnyValue::Float64(v) => Ok(v),
        AnyValue::Float32(v) => Ok(v as f64),
        AnyValue::Int64(v) => Ok(v as f64),
        AnyValue::Int32(v) => Ok(v as f64),
        AnyValue::UInt64(v) => Ok(v as f64),
        AnyValue::UInt32(v) => Ok(v as f64),
        AnyValue::UInt16(v) => Ok(v as f64),
        AnyValue::UInt8(v) => Ok(v as f64),
        AnyValue::Int16(v) => Ok(v as f64),
        AnyValue::Int8(v) => Ok(v as f64),
        ref other => Err(LibraryError::InvalidSurveyDataValue {
            column: name.to_string(),
            value: other.to_string(),
        }),
    }
}

fn struct_features(
    values: &[AnyValue<'_>],
    fields: &[Field],
    name: &str,
) -> Result<Vec<SignalFeature>, LibraryError> {
    match &values[struct_field(fields, name)?] {
        AnyValue::Null => Ok(Vec::new()),
        AnyValue::List(series) => {
            let mut features = Vec::with_capacity(series.len());
            for idx in 0..series.len() {
                match series.get(idx)?.into_static() {
                    AnyValue::StructOwned(payload) => features.push(SignalFeature {
                        feature_type: struct_string(&payload.0, &payload.1, "feature_type")?,
                        tof: struct_f64(&payload.0, &payload.1, "tof")?,
                        vpp: struct_f64(&payload.0, &payload.1, "vpp")?,
                    }),
                    AnyValue::Null => {}
                    other => {
                        return Err(LibraryError::InvalidSurveyDataValue {
                            column: "features".into(),
                            value: other.to_string(),
                        });
                    }
                }
            }
            Ok(features)
        }
        other => Err(LibraryError::InvalidSurveyDataValue {
            column: name.to_string(),
            value: other.to_string(),
        }),
    }
}

fn struct_field(fields: &[Field], name: &str) -> Result<usize, LibraryError> {
    fields
        .iter()
        .position(|field| field.name().as_str() == name)
        .ok_or_else(|| LibraryError::MissingSurveyDataColumn(name.to_string()))
}

fn opt_num(value: Option<f64>) -> String {
    value.map(|v| v.to_string()).unwrap_or_default()
}

fn csv_err(error: csv::Error) -> LibraryError {
    LibraryError::PickListCsv(error.to_string())
}

fn echo_signal_any_value(signal: Option<&EchoSignal>) -> AnyValue<'static> {
    match signal {
        None => AnyValue::Null,
        Some(signal) => AnyValue::StructOwned(Box::new((
            vec![
                AnyValue::StringOwned(signal.signal_type.clone().into()),
                AnyValue::Float64(signal.transducer_x),
                AnyValue::Float64(signal.transducer_y),
                AnyValue::Float64(signal.transducer_z),
                AnyValue::List(
                    Series::from_any_values(
                        "features".into(),
                        &signal
                            .features
                            .iter()
                            .map(feature_any_value)
                            .collect::<Vec<_>>(),
                        false,
                    )
                    .expect("feature series"),
                ),
            ],
            vec![
                Field::new("signal_type".into(), DataType::String),
                Field::new("transducer_x".into(), DataType::Float64),
                Field::new("transducer_y".into(), DataType::Float64),
                Field::new("transducer_z".into(), DataType::Float64),
                Field::new(
                    "features".into(),
                    DataType::List(Box::new(DataType::Struct(vec![
                        Field::new("feature_type".into(), DataType::String),
                        Field::new("tof".into(), DataType::Float64),
                        Field::new("vpp".into(), DataType::Float64),
                    ]))),
                ),
            ],
        ))),
    }
}

fn feature_any_value(feature: &SignalFeature) -> AnyValue<'static> {
    AnyValue::StructOwned(Box::new((
        vec![
            AnyValue::StringOwned(feature.feature_type.clone().into()),
            AnyValue::Float64(feature.tof),
            AnyValue::Float64(feature.vpp),
        ],
        vec![
            Field::new("feature_type".into(), DataType::String),
            Field::new("tof".into(), DataType::Float64),
            Field::new("vpp".into(), DataType::Float64),
        ],
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    const PLATE_SURVEY_XML: &str =
        include_str!("../../tests/test_data/platesurvey.xml");

    #[test]
    fn surveydata_parquet_round_trip_preserves_core_fields() {
        let survey = PlateSurvey::from_platesurvey_xml(PLATE_SURVEY_XML).expect("parse xml");
        let path = std::env::temp_dir().join(format!(
            "kithairon-survey-{}-{}.parquet",
            std::process::id(),
            survey.wells.len()
        ));
        write_survey_parquet(&path, &survey).expect("write parquet");
        let reparsed = read_survey_parquet(&path).expect("read parquet");
        std::fs::remove_file(&path).ok();

        assert_eq!(reparsed.plate_type, survey.plate_type);
        assert_eq!(reparsed.plate_barcode, survey.plate_barcode);
        assert_eq!(reparsed.timestamp, survey.timestamp);
        assert_eq!(reparsed.survey_rows, survey.survey_rows);
        assert_eq!(reparsed.survey_columns, survey.survey_columns);
        assert_eq!(reparsed.survey_total_wells, survey.survey_total_wells);
        assert_eq!(reparsed.wells.len(), survey.wells.len());
        assert_eq!(reparsed.wells[0].well, survey.wells[0].well);
        assert_eq!(reparsed.wells[0].volume, survey.wells[0].volume);
    }

    #[test]
    fn validation_volumes_convert_ul_to_nl() {
        let mut survey = PlateSurvey::from_platesurvey_xml(PLATE_SURVEY_XML).expect("parse xml");
        survey.plate_name = Some("SourcePlate1".into());
        let path = std::env::temp_dir().join(format!(
            "kithairon-survey-vols-{}-{}.parquet",
            std::process::id(),
            survey.wells.len()
        ));
        write_survey_parquet(&path, &survey).expect("write parquet");
        let volumes = read_validation_volumes_parquet(&path).expect("read validation volumes");
        std::fs::remove_file(&path).ok();

        let plate_name = survey.plate_name.clone().unwrap();
        let a1 = volumes
            .get(&plate_name)
            .and_then(|plate| plate.get("A1"))
            .copied();
        let expected = survey
            .wells
            .iter()
            .find(|well| well.well == "A1")
            .and_then(|well| well.volume)
            .map(|v| v * 1000.0);
        assert_eq!(a1, expected);
    }
}
