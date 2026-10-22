// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `COPY <label | edge type> TO | FROM '<path>'`: file export and import.
//!
//! # File layout
//!
//! Both formats (`csv`, `parquet`) share one layout:
//!
//! * a vertex file has a `_vid` column, then one column per property;
//! * an edge file has `_eid`, `_src` and `_dst` columns, then one column per
//!   property.
//!
//! The property columns are exactly the user-visible properties: the union,
//! over every exported entity, of the keys `properties()` returns for it —
//! declared and undeclared alike, from L0 and storage — sorted by name. An
//! entity without one of them has an empty cell (CSV) or a null (Parquet).
//! Internal storage columns (`_uid`, `_version`, `overflow_json`, ...) never
//! appear. The identity columns use reserved names, so they cannot collide
//! with a property; on import they address endpoints and are not stored.
//!
//! The rows are read through the query engine — `MATCH (n:L) RETURN id(n),
//! properties(n)` — so an export cannot disagree with what a query sees.
//!
//! # Types
//!
//! Parquet carries types: a declared property keeps its declared storage
//! type, an undeclared one gets the single type its values share (integer,
//! float, boolean, string, bytes) or, when they are mixed or structured, the
//! engine's tagged value encoding, which import decodes back. CSV is text: on
//! import a declared property is parsed as its declared type and an
//! undeclared one is read as a string.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use anyhow::{Result, anyhow};
use arrow_array::{Array, ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::{Field, Schema as ArrowSchema};
use uni_common::core::id::{Eid, Vid};
use uni_common::core::schema::{DataType, PropertyMeta, is_reserved_property_name};
use uni_store::cloud::{build_store_from_url, is_cloud_url};
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::storage::arrow_convert::{self, PropertyExtractor};

use super::core::Executor;
use crate::query::planner::QueryPlanner;
use crate::types::Value;

/// Column holding a vertex's id in a COPY file.
const VID_COLUMN: &str = "_vid";
/// Column holding an edge's id in a COPY file.
const EID_COLUMN: &str = "_eid";
/// Column holding an edge's source vertex id in a COPY file.
const SRC_COLUMN: &str = "_src";
/// Column holding an edge's destination vertex id in a COPY file.
const DST_COLUMN: &str = "_dst";

/// IDs allocated per allocator round-trip during import.
const IMPORT_ID_CHUNK: usize = 256;

/// What a COPY statement names: a label or an edge type, schema-canonical.
enum CopyTarget {
    Label(String),
    EdgeType(String),
}

impl CopyTarget {
    fn name(&self) -> &str {
        match self {
            Self::Label(n) | Self::EdgeType(n) => n,
        }
    }

    fn id_columns(&self) -> &'static [&'static str] {
        match self {
            Self::Label(_) => &[VID_COLUMN],
            Self::EdgeType(_) => &[EID_COLUMN, SRC_COLUMN, DST_COLUMN],
        }
    }
}

/// One exported entity: its identity columns and its user properties.
struct ExportRow {
    ids: Vec<u64>,
    props: HashMap<String, Value>,
}

/// One imported entity: its endpoints (edges only) and its user properties.
struct ImportRow {
    endpoints: Option<(Vid, Vid)>,
    props: HashMap<String, Value>,
}

/// A COPY format the exporters and importers implement.
fn check_format(format: &str, direction: &str) -> Result<()> {
    match format {
        "csv" | "parquet" => Ok(()),
        _ => Err(anyhow!(
            "COPY {direction} only supports 'parquet' and 'csv' formats, got '{format}'"
        )),
    }
}

/// Backtick-quote a name for embedding in a Cypher statement.
fn quote_identifier(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// The CSV delimiter and header flag from the COPY options.
///
/// `header` and `headers` are both accepted for the header flag.
fn csv_dialect(options: &HashMap<String, Value>) -> (u8, bool) {
    let delimiter = options
        .get("delimiter")
        .and_then(Value::as_str)
        .and_then(|s| s.as_bytes().first().copied())
        .unwrap_or(b',');
    let has_header = options
        .get("header")
        .or_else(|| options.get("headers"))
        .and_then(Value::as_bool)
        .unwrap_or(true);
    (delimiter, has_header)
}

/// A property value as CSV text; null is the empty cell.
fn format_csv_value(val: &Value) -> String {
    match val {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Bool(b) => b.to_string(),
        other => format!("{other}"),
    }
}

/// Parse a CSV cell as a declared property's type; the empty cell is null.
fn parse_csv_value(s: &str, data_type: &DataType, prop_name: &str) -> Result<Value> {
    if s.is_empty() {
        return Ok(Value::Null);
    }
    let bad = |what: &str| anyhow!("Failed to parse {what} for property '{prop_name}': {s}");
    match data_type {
        DataType::Int32 | DataType::Int64 => {
            s.parse::<i64>().map(Value::Int).map_err(|_| bad("integer"))
        }
        DataType::Float32 | DataType::Float64 => {
            s.parse::<f64>().map(Value::Float).map_err(|_| bad("float"))
        }
        DataType::Bool => s
            .to_lowercase()
            .parse::<bool>()
            .map(Value::Bool)
            .map_err(|_| bad("boolean")),
        DataType::CypherValue => serde_json::from_str::<serde_json::Value>(s)
            .map(Value::from)
            .map_err(|_| bad("JSON")),
        DataType::Vector { .. } => serde_json::from_str::<Vec<f32>>(s)
            .map(Value::Vector)
            .map_err(|_| bad("vector")),
        _ => Ok(Value::String(s.to_string())),
    }
}

/// A vertex or edge id from a COPY cell.
fn id_from_value(value: &Value, column: &str) -> Result<u64> {
    match value {
        Value::Int(i) if *i >= 0 => Ok(*i as u64),
        Value::String(s) => s
            .trim()
            .parse::<u64>()
            .map_err(|_| anyhow!("Column '{column}' holds '{s}', not a vertex id")),
        other => Err(anyhow!(
            "Column '{column}' holds {other:?}, not a vertex id"
        )),
    }
}

/// The storage type an undeclared Parquet column gets from its values.
///
/// One shared scalar type keeps that type; anything else (mixed types,
/// lists, maps, temporals) uses the tagged value encoding, which the import
/// path decodes back to the original values.
fn infer_column_type<'v>(values: impl Iterator<Item = &'v Value>) -> DataType {
    let mut seen: Option<DataType> = None;
    for v in values {
        let t = match v {
            Value::Null => continue,
            Value::Int(_) => DataType::Int64,
            Value::Float(_) => DataType::Float64,
            Value::Bool(_) => DataType::Bool,
            Value::String(_) => DataType::String,
            Value::Bytes(_) => DataType::Bytes,
            _ => return DataType::CypherValue,
        };
        match &seen {
            None => seen = Some(t),
            Some(s) if *s == t => {}
            Some(_) => return DataType::CypherValue,
        }
    }
    seen.unwrap_or(DataType::String)
}

/// The Arrow field for a property column of the given storage type.
fn property_field(name: &str, data_type: &DataType) -> Field {
    let field = Field::new(name, data_type.to_arrow(), true);
    if matches!(data_type, DataType::Bytes) {
        field.with_metadata(uni_common::core::schema::raw_bytes_field_metadata())
    } else {
        field
    }
}

impl Executor {
    /// Resolve a COPY identifier to a label or edge type.
    ///
    /// Declared and schemaless edge types resolve as edge types; anything
    /// else is a label, declared or not.
    fn resolve_copy_target(&self, identifier: &str) -> CopyTarget {
        let schema = self.storage.schema_manager().schema();
        if let Some(name) = schema
            .edge_type_id_unified_case_insensitive(identifier)
            .and_then(|id| schema.edge_type_name_by_id_unified(id))
        {
            return CopyTarget::EdgeType(name);
        }
        CopyTarget::Label(
            schema
                .canonical_label_name(identifier)
                .unwrap_or_else(|| identifier.to_string()),
        )
    }

    /// The declared properties of a label or edge type.
    fn declared_properties(&self, target: &CopyTarget) -> HashMap<String, PropertyMeta> {
        self.storage
            .schema_manager()
            .schema()
            .properties
            .get(target.name())
            .cloned()
            .unwrap_or_default()
    }

    /// Export a label's or edge type's entities to a CSV or Parquet file.
    ///
    /// See the module documentation for the file layout. Returns the number
    /// of entities written.
    ///
    /// # Errors
    ///
    /// Returns an error for an unsupported format, a destination the file
    /// sandbox refuses, an identifier that names neither a declared label,
    /// an edge type nor any existing vertex, or a failed read or write.
    pub(crate) async fn execute_copy_to(
        &self,
        identifier: &str,
        path: &str,
        format: &str,
        options: &HashMap<String, Value>,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
    ) -> Result<usize> {
        check_format(format, "TO")?;
        let target = self.resolve_copy_target(identifier);
        let rows = self.read_export_rows(&target, prop_manager, params).await?;

        let schema = self.storage.schema_manager().schema();
        if rows.is_empty()
            && matches!(target, CopyTarget::Label(_))
            && schema.canonical_label_name(identifier).is_none()
        {
            return Err(anyhow!("Unknown label or edge type: '{identifier}'"));
        }

        let columns: Vec<String> = rows
            .iter()
            .flat_map(|r| r.props.keys())
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();

        let bytes = match format {
            "csv" => Self::export_csv(&target, &columns, &rows, options)?,
            _ => {
                let declared = self.declared_properties(&target);
                Self::export_parquet(&target, &columns, &rows, &declared)?
            }
        };
        self.write_copy_destination(path, bytes).await?;
        Ok(rows.len())
    }

    /// Read every entity of `target` with its id columns and `properties()`.
    async fn read_export_rows(
        &self,
        target: &CopyTarget,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
    ) -> Result<Vec<ExportRow>> {
        let name = quote_identifier(target.name());
        let (cypher, id_keys): (String, &[&str]) = match target {
            CopyTarget::Label(_) => (
                format!("MATCH (n:{name}) RETURN id(n) AS i0, properties(n) AS p ORDER BY i0"),
                &["i0"],
            ),
            CopyTarget::EdgeType(_) => (
                format!(
                    "MATCH (a)-[r:{name}]->(b) \
                     RETURN id(r) AS i0, id(a) AS i1, id(b) AS i2, properties(r) AS p \
                     ORDER BY i0"
                ),
                &["i0", "i1", "i2"],
            ),
        };
        let planner = QueryPlanner::new(self.storage.schema_manager().schema());
        let plan = planner.plan(uni_cypher::parse(&cypher)?)?;
        let batches = self.execute_datafusion(plan, prop_manager, params).await?;
        let raw = self.record_batches_to_rows(batches)?;

        raw.into_iter()
            .map(|mut row| {
                let ids = id_keys
                    .iter()
                    .map(|k| {
                        row.get(*k)
                            .ok_or_else(|| anyhow!("COPY TO: missing id column {k}"))
                            .and_then(|v| id_from_value(v, k))
                    })
                    .collect::<Result<Vec<u64>>>()?;
                let props = match row.remove("p") {
                    Some(Value::Map(m)) => m
                        .into_iter()
                        .filter(|(k, v)| !v.is_null() && !is_reserved_property_name(k))
                        .collect(),
                    Some(Value::Null) | None => HashMap::new(),
                    Some(other) => {
                        return Err(anyhow!("COPY TO: properties() returned {other:?}"));
                    }
                };
                Ok(ExportRow { ids, props })
            })
            .collect()
    }

    /// Serialize export rows as CSV.
    fn export_csv(
        target: &CopyTarget,
        columns: &[String],
        rows: &[ExportRow],
        options: &HashMap<String, Value>,
    ) -> Result<Vec<u8>> {
        let (delimiter, has_header) = csv_dialect(options);
        let mut wtr = csv::WriterBuilder::new()
            .delimiter(delimiter)
            .from_writer(Vec::new());
        if has_header {
            let header: Vec<&str> = target
                .id_columns()
                .iter()
                .copied()
                .chain(columns.iter().map(String::as_str))
                .collect();
            wtr.write_record(&header)?;
        }
        for row in rows {
            let record: Vec<String> = row
                .ids
                .iter()
                .map(u64::to_string)
                .chain(
                    columns
                        .iter()
                        .map(|c| row.props.get(c).map(format_csv_value).unwrap_or_default()),
                )
                .collect();
            wtr.write_record(&record)?;
        }
        wtr.into_inner()
            .map_err(|e| anyhow!("CSV export failed: {}", e.error()))
    }

    /// Serialize export rows as one Parquet file.
    fn export_parquet(
        target: &CopyTarget,
        columns: &[String],
        rows: &[ExportRow],
        declared: &HashMap<String, PropertyMeta>,
    ) -> Result<Vec<u8>> {
        let mut fields: Vec<Field> = Vec::new();
        let mut arrays: Vec<ArrayRef> = Vec::new();

        for (i, id_col) in target.id_columns().iter().enumerate() {
            fields.push(Field::new(*id_col, arrow_schema::DataType::UInt64, false));
            arrays.push(Arc::new(UInt64Array::from_iter_values(
                rows.iter().map(|r| r.ids[i]),
            )));
        }

        let not_deleted = vec![false; rows.len()];
        for name in columns {
            let data_type = declared.get(name).map_or_else(
                || infer_column_type(rows.iter().filter_map(|r| r.props.get(name))),
                |meta| meta.r#type.clone(),
            );
            let array = PropertyExtractor::new(name, &data_type).build_column(
                rows.len(),
                &not_deleted,
                |i| rows[i].props.get(name),
            )?;
            fields.push(property_field(name, &data_type));
            arrays.push(array);
        }

        let schema = Arc::new(ArrowSchema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), arrays)?;
        let mut buffer = Vec::new();
        let mut writer = parquet::arrow::ArrowWriter::try_new(&mut buffer, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(buffer)
    }

    /// Write a COPY file to a cloud URL or a sandbox-checked local path.
    async fn write_copy_destination(&self, dest: &str, bytes: Vec<u8>) -> Result<()> {
        if is_cloud_url(dest) {
            use object_store::ObjectStoreExt;
            let (store, path) = build_store_from_url(dest)?;
            store.put(&path, bytes::Bytes::from(bytes).into()).await?;
        } else {
            std::fs::write(self.validate_path(dest)?, bytes)?;
        }
        Ok(())
    }

    /// Read a COPY file from a cloud URL or a sandbox-checked local path.
    async fn read_copy_source(&self, source: &str) -> Result<bytes::Bytes> {
        if is_cloud_url(source) {
            use object_store::ObjectStoreExt;
            let (store, path) = build_store_from_url(source)?;
            Ok(store.get(&path).await?.bytes().await?)
        } else {
            Ok(bytes::Bytes::from(std::fs::read(
                self.validate_path(source)?,
            )?))
        }
    }

    /// Import entities from a CSV or Parquet file into a label or edge type.
    ///
    /// Reads the layout [`Self::execute_copy_to`] writes, and any file whose
    /// columns are property names. Reserved columns are never stored. Edge
    /// endpoints come from the `src_col` / `dst_col` options, defaulting to
    /// `_src` / `_dst` when the file has them and to `src` / `dst` otherwise.
    /// Returns the number of entities created.
    ///
    /// # Errors
    ///
    /// Returns an error for an unsupported format, a source the file sandbox
    /// refuses, an undeclared target under `strict_schema`, a cell a declared
    /// type cannot parse, a missing or malformed endpoint, or a failed write.
    pub(crate) async fn execute_copy_from(
        &self,
        identifier: &str,
        path: &str,
        format: &str,
        options: &HashMap<String, Value>,
    ) -> Result<usize> {
        check_format(format, "FROM")?;
        let writer = self
            .writer
            .as_ref()
            .ok_or_else(|| anyhow!("COPY FROM requires a writer"))?;
        let target = self.resolve_copy_target(identifier);
        let schema = self.storage.schema_manager().schema();
        let declared_target = match &target {
            CopyTarget::Label(n) => schema.labels.contains_key(n),
            CopyTarget::EdgeType(n) => schema.edge_types.contains_key(n),
        };
        if self.config.strict_schema && !declared_target {
            return Err(anyhow!(
                "COPY FROM: '{identifier}' is not declared (strict_schema is enabled)"
            ));
        }
        let declared = self.declared_properties(&target);
        let bytes = self.read_copy_source(path).await?;

        let endpoint_cols = |columns: &[String]| -> (String, String) {
            let pick = |opt: &str, preferred: &str, fallback: &str| {
                options
                    .get(opt)
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .unwrap_or_else(|| {
                        if columns.iter().any(|c| c == preferred) {
                            preferred.to_string()
                        } else {
                            fallback.to_string()
                        }
                    })
            };
            (
                pick("src_col", SRC_COLUMN, "src"),
                pick("dst_col", DST_COLUMN, "dst"),
            )
        };
        let is_edge = matches!(target, CopyTarget::EdgeType(_));

        let rows = match format {
            "csv" => Self::read_csv_rows(&bytes, options, &declared, is_edge, &endpoint_cols)?,
            _ => Self::read_parquet_rows(bytes, &declared, is_edge, &endpoint_cols)?,
        };

        let writer: &uni_store::Writer = writer.as_ref();
        // Inside a transaction the rows go to its private L0 like any other
        // write: visible to the transaction only, gone on rollback, bounded by
        // its memory limit, and made durable by its commit. Without one (a
        // writer-level import) they go to the main L0 and are flushed below.
        let tx_l0 = self.transaction_l0_override.as_ref();
        let count = rows.len();
        match &target {
            CopyTarget::Label(label) => {
                let labels = [label.clone()];
                for chunk in rows.chunks(IMPORT_ID_CHUNK) {
                    let vids = writer.allocate_vids(chunk.len()).await?;
                    for (row, vid) in chunk.iter().zip(vids) {
                        writer
                            .insert_vertex_with_labels(vid, row.props.clone(), &labels, tx_l0)
                            .await?;
                    }
                }
            }
            CopyTarget::EdgeType(type_name) => {
                let type_id = self
                    .storage
                    .schema_manager()
                    .get_or_assign_edge_type_id(type_name);
                for chunk in rows.chunks(IMPORT_ID_CHUNK) {
                    let eids: Vec<Eid> = writer.allocate_eids(chunk.len()).await?;
                    for (row, eid) in chunk.iter().zip(eids) {
                        let (src, dst) = row
                            .endpoints
                            .ok_or_else(|| anyhow!("COPY FROM: edge row without endpoints"))?;
                        writer
                            .insert_edge(
                                src,
                                dst,
                                type_id,
                                eid,
                                row.props.clone(),
                                Some(type_name.clone()),
                                tx_l0,
                            )
                            .await?;
                    }
                }
            }
        }

        log::info!(
            "Imported {count} rows from '{path}' into '{}'",
            target.name()
        );
        if count > 0 && tx_l0.is_none() {
            writer.flush_to_l1(None).await?;
        }
        Ok(count)
    }

    /// Decode a CSV file into import rows.
    fn read_csv_rows(
        bytes: &[u8],
        options: &HashMap<String, Value>,
        declared: &HashMap<String, PropertyMeta>,
        is_edge: bool,
        endpoint_cols: &dyn Fn(&[String]) -> (String, String),
    ) -> Result<Vec<ImportRow>> {
        let (delimiter, has_header) = csv_dialect(options);
        let mut rdr = csv::ReaderBuilder::new()
            .delimiter(delimiter)
            .has_headers(has_header)
            .from_reader(bytes);
        let mut headers: Vec<String> = if has_header {
            rdr.headers()?.iter().map(str::to_string).collect()
        } else {
            Vec::new()
        };
        let mut out = Vec::new();
        for record in rdr.records() {
            let record = record?;
            if headers.is_empty() {
                headers = (0..record.len()).map(|i| format!("col{i}")).collect();
            }
            let (src_col, dst_col) = endpoint_cols(&headers);
            let mut props = HashMap::new();
            let (mut src, mut dst) = (None, None);
            for (header, cell) in headers.iter().zip(record.iter()) {
                if is_edge && *header == src_col {
                    src = Some(Vid::new(id_from_value(
                        &Value::String(cell.into()),
                        header,
                    )?));
                } else if is_edge && *header == dst_col {
                    dst = Some(Vid::new(id_from_value(
                        &Value::String(cell.into()),
                        header,
                    )?));
                } else if is_reserved_property_name(header) {
                    continue;
                } else {
                    let value = match declared.get(header) {
                        Some(meta) => parse_csv_value(cell, &meta.r#type, header)?,
                        None if cell.is_empty() => Value::Null,
                        None => Value::String(cell.to_string()),
                    };
                    if !value.is_null() {
                        props.insert(header.clone(), value);
                    }
                }
            }
            out.push(Self::import_row(
                props, src, dst, is_edge, &src_col, &dst_col,
            )?);
        }
        Ok(out)
    }

    /// Decode a Parquet file into import rows.
    fn read_parquet_rows(
        bytes: bytes::Bytes,
        declared: &HashMap<String, PropertyMeta>,
        is_edge: bool,
        endpoint_cols: &dyn Fn(&[String]) -> (String, String),
    ) -> Result<Vec<ImportRow>> {
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(bytes)?
            .build()?;
        let mut out = Vec::new();
        for batch in reader {
            let batch = batch?;
            let schema = batch.schema();
            let names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
            let (src_col, dst_col) = endpoint_cols(&names);
            for row in 0..batch.num_rows() {
                let mut props = HashMap::new();
                let (mut src, mut dst) = (None, None);
                for (field, column) in schema.fields().iter().zip(batch.columns()) {
                    let name = field.name();
                    if !is_edge && is_reserved_property_name(name) {
                        continue;
                    }
                    let hint = declared
                        .get(name)
                        .map(|m| &m.r#type)
                        .or_else(|| arrow_convert::type_hint_for_field(field));
                    let value = if column.is_null(row) {
                        Value::Null
                    } else {
                        arrow_convert::arrow_to_value(column.as_ref(), row, hint)?
                    };
                    if is_edge && *name == src_col {
                        src = Some(Vid::new(id_from_value(&value, name)?));
                    } else if is_edge && *name == dst_col {
                        dst = Some(Vid::new(id_from_value(&value, name)?));
                    } else if !is_reserved_property_name(name) && !value.is_null() {
                        props.insert(name.clone(), value);
                    }
                }
                out.push(Self::import_row(
                    props, src, dst, is_edge, &src_col, &dst_col,
                )?);
            }
        }
        Ok(out)
    }

    /// Assemble one import row, requiring both endpoints for an edge.
    fn import_row(
        props: HashMap<String, Value>,
        src: Option<Vid>,
        dst: Option<Vid>,
        is_edge: bool,
        src_col: &str,
        dst_col: &str,
    ) -> Result<ImportRow> {
        let endpoints = if is_edge {
            Some((
                src.ok_or_else(|| anyhow!("Missing source vertex id column '{src_col}'"))?,
                dst.ok_or_else(|| anyhow!("Missing destination vertex id column '{dst_col}'"))?,
            ))
        } else {
            None
        };
        Ok(ImportRow { endpoints, props })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn undeclared_column_type_is_shared_scalar_or_tagged() {
        let ints = [Value::Int(1), Value::Null, Value::Int(2)];
        assert_eq!(infer_column_type(ints.iter()), DataType::Int64);
        let mixed = [Value::Int(1), Value::String("a".into())];
        assert_eq!(infer_column_type(mixed.iter()), DataType::CypherValue);
        let list = [Value::List(vec![Value::Int(1)])];
        assert_eq!(infer_column_type(list.iter()), DataType::CypherValue);
    }

    #[test]
    fn csv_dialect_reads_both_header_spellings() {
        let opts = HashMap::from([("headers".to_string(), Value::Bool(false))]);
        assert_eq!(csv_dialect(&opts), (b',', false));
        let opts = HashMap::from([
            ("header".to_string(), Value::Bool(false)),
            ("delimiter".to_string(), Value::String(";".into())),
        ]);
        assert_eq!(csv_dialect(&opts), (b';', false));
    }
}
