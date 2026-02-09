// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use super::core::*;
use crate::query::planner::LogicalPlan;
use anyhow::{Result, anyhow};
use lancedb::query::{ExecutableQuery, QueryBase};
use std::collections::HashMap;
use uni_common::Value;
use std::sync::Arc;
use uni_common::DataType;
use uni_common::core::id::Vid;
use uni_common::core::schema::{Constraint, ConstraintTarget, ConstraintType, SchemaManager};
use uni_cypher::ast::{
    AlterAction, AlterEdgeType, AlterLabel, BinaryOp, ConstraintType as AstConstraintType,
    CreateConstraint, CreateEdgeType, CreateLabel, CypherLiteral, Direction, DropConstraint,
    DropEdgeType, DropLabel, Expr, Pattern, PatternElement, RemoveItem, SetClause, SetItem,
};
use uni_store::QueryContext;
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::runtime::writer::Writer;

impl Executor {
    /// Extract labels from a node value (Map with _labels field)
    pub(crate) fn extract_labels_from_node(node_val: &Value) -> Option<Vec<String>> {
        if let Value::Map(map) = node_val {
            // Check for _labels (plural array)
            if let Some(Value::List(labels_arr)) = map.get("_labels") {
                let labels: Vec<String> = labels_arr
                    .iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect();
                if !labels.is_empty() {
                    return Some(labels);
                }
            }
            // Fallback to _label (singular string) - used by scan results
            if let Some(Value::String(label)) = map.get("_label")
                && !label.is_empty()
            {
                return Some(vec![label.clone()]);
            }
        }
        None
    }

    pub(crate) async fn execute_vacuum(&self) -> Result<()> {
        if let Some(writer_arc) = &self.writer {
            // Flush first while holding the lock
            {
                let mut writer = writer_arc.write().await;
                writer.flush_to_l1(None).await?;
            } // Drop lock before compacting to avoid blocking reads/writes

            // Compaction can run without holding the writer lock
            let compactor = uni_store::storage::compaction::Compactor::new(self.storage.clone());
            compactor.compact_all().await?;
        }
        Ok(())
    }

    pub(crate) async fn execute_checkpoint(&self) -> Result<()> {
        if let Some(writer_arc) = &self.writer {
            let mut writer = writer_arc.write().await;
            writer.flush_to_l1(Some("checkpoint".to_string())).await?;
        }
        Ok(())
    }

    pub(crate) async fn execute_copy_to(
        &self,
        identifier: &str,
        path: &str,
        format: &str,
        options: &HashMap<String, Value>,
    ) -> Result<usize> {
        // Check schema to determine if identifier is an edge type or vertex label
        let schema = self.storage.schema_manager().schema();

        // Try as edge type first
        if let Some(_edge_type_meta) = schema.get_edge_type_case_insensitive(identifier) {
            return self
                .export_edge_type_in_format(identifier, path, format)
                .await;
        }

        // Try as vertex label
        if schema.get_label_case_insensitive(identifier).is_some() {
            return self
                .export_vertex_label_in_format(identifier, path, format, options)
                .await;
        }

        // Neither edge type nor vertex label found
        Err(anyhow!("Unknown label or edge type: '{}'", identifier))
    }

    async fn export_vertex_label_in_format(
        &self,
        label: &str,
        path: &str,
        format: &str,
        _options: &HashMap<String, Value>,
    ) -> Result<usize> {
        match format {
            "parquet" => self.export_vertex_label(label, path).await,
            "csv" => {
                // Get dataset and open table
                let dataset = self.storage.vertex_dataset(label)?;
                let table = dataset.open_lancedb(self.storage.lancedb_store()).await?;

                // Query all data
                let mut stream = table.query().execute().await?;

                // Collect all batches
                let mut all_rows = Vec::new();
                let mut column_names = Vec::new();

                // Iterate stream using StreamExt
                use futures::StreamExt;
                while let Some(batch_result) = stream.next().await {
                    let batch = batch_result?;

                    // Get column names from first batch
                    if column_names.is_empty() {
                        column_names = batch
                            .schema()
                            .fields()
                            .iter()
                            .filter(|f| !f.name().starts_with('_') && f.name() != "ext_id")
                            .map(|f| f.name().clone())
                            .collect();
                    }

                    // Convert batch to rows
                    for row_idx in 0..batch.num_rows() {
                        let mut row = Vec::new();
                        for field in batch.schema().fields() {
                            if field.name().starts_with('_') || field.name() == "ext_id" {
                                continue;
                            }

                            let col_idx = batch.schema().index_of(field.name())?;
                            let column = batch.column(col_idx);
                            let value = self.arrow_value_to_json(column, row_idx)?;

                            // Convert value to CSV string
                            let csv_value = match value {
                                Value::Null => String::new(),
                                Value::Bool(b) => b.to_string(),
                                Value::Int(i) => i.to_string(),
                                Value::Float(f) => f.to_string(),
                                Value::String(s) => s,
                                _ => format!("{}", value),
                            };
                            row.push(csv_value);
                        }
                        all_rows.push(row);
                    }
                }

                // Write CSV
                let file = std::fs::File::create(path)?;
                let mut wtr = csv::Writer::from_writer(file);

                // Write headers
                log::debug!("CSV export headers: {:?}", column_names);
                wtr.write_record(&column_names)?;

                // Write rows
                for (i, row) in all_rows.iter().enumerate() {
                    log::debug!("CSV export row {}: {:?}", i, row);
                    wtr.write_record(row)?;
                }

                wtr.flush()?;
                Ok(all_rows.len())
            }
            _ => Err(anyhow!(
                "COPY TO only supports 'parquet' and 'csv' formats, got '{}'",
                format
            )),
        }
    }

    async fn export_edge_type_in_format(
        &self,
        edge_type: &str,
        path: &str,
        format: &str,
    ) -> Result<usize> {
        match format {
            "parquet" => self.export_edge_type(edge_type, path).await,
            "csv" => Err(anyhow!("CSV export not yet supported for edge types")),
            _ => Err(anyhow!(
                "COPY TO only supports 'parquet' and 'csv' formats, got '{}'",
                format
            )),
        }
    }

    /// Write a stream of record batches to a Parquet file.
    /// Returns the total number of rows written, or 0 if the stream is empty.
    async fn write_batches_to_parquet(
        mut stream: impl futures::Stream<Item = Result<arrow_array::RecordBatch, lancedb::Error>>
        + Unpin,
        path: &str,
        entity_description: &str,
    ) -> Result<usize> {
        use futures::TryStreamExt;

        // Get first batch to determine schema and create writer
        let first_batch = match stream.try_next().await? {
            Some(batch) => batch,
            None => {
                log::info!("No data to export from {}", entity_description);
                return Ok(0);
            }
        };

        // Create Parquet writer using schema from first batch
        let file = std::fs::File::create(path)?;
        let arrow_schema = first_batch.schema();
        let mut writer = parquet::arrow::ArrowWriter::try_new(file, arrow_schema, None)?;

        // Write first batch
        let mut count = first_batch.num_rows();
        writer.write(&first_batch)?;

        // Write remaining batches
        while let Some(batch) = stream.try_next().await? {
            count += batch.num_rows();
            writer.write(&batch)?;
        }

        writer.close()?;

        log::info!(
            "Exported {} rows from {} to '{}'",
            count,
            entity_description,
            path
        );
        Ok(count)
    }

    /// Export vertices of a specific label to Parquet
    async fn export_vertex_label(&self, label: &str, path: &str) -> Result<usize> {
        let dataset = self.storage.vertex_dataset(label)?;
        let lancedb_store = self.storage.lancedb_store();
        let table = dataset.open_lancedb(lancedb_store).await?;
        let query = table.query();
        let stream = query.execute().await?;

        Self::write_batches_to_parquet(stream, path, &format!("label '{}'", label)).await
    }

    /// Export edges of a specific type to Parquet
    async fn export_edge_type(&self, edge_type: &str, path: &str) -> Result<usize> {
        let schema = self.storage.schema_manager().schema();
        let _edge_type_meta = schema
            .edge_types
            .get(edge_type)
            .ok_or_else(|| anyhow!("Edge type '{}' not found", edge_type))?;

        let lancedb_store = self.storage.lancedb_store();
        let table = lancedb_store.open_main_edge_table().await?;
        let query = table.query().only_if(format!("type = '{}'", edge_type));
        let stream = query.execute().await?;

        Self::write_batches_to_parquet(stream, path, &format!("edge type '{}'", edge_type)).await
    }

    pub(crate) async fn execute_copy_from(
        &self,
        label: &str,
        path: &str,
        format: &str,
        options: &HashMap<String, Value>,
    ) -> Result<usize> {
        // Read data from file
        let batches = match format {
            "parquet" => self.read_parquet_file(path)?,
            "csv" => self.read_csv_file(path, label, options)?,
            _ => {
                return Err(anyhow!(
                    "COPY FROM only supports 'parquet' and 'csv' formats, got '{}'",
                    format
                ));
            }
        };

        // Get writer
        let writer_arc = self
            .writer
            .as_ref()
            .ok_or_else(|| anyhow!("No writer available"))?;

        let db_schema = self.storage.schema_manager().schema();

        // Check if this is a label (vertex) or edge type
        let is_edge = db_schema.edge_type_id_by_name(label).is_some();

        if is_edge {
            // Import edges
            let edge_type_id = db_schema
                .edge_type_id_by_name(label)
                .ok_or_else(|| anyhow!("Edge type '{}' not found in schema", label))?;

            // Get src and dst column names from options
            let src_col = options
                .get("src_col")
                .and_then(|v| v.as_str())
                .unwrap_or("src");
            let dst_col = options
                .get("dst_col")
                .and_then(|v| v.as_str())
                .unwrap_or("dst");

            let mut total_rows = 0;
            for batch in batches {
                let num_rows = batch.num_rows();

                for row_idx in 0..num_rows {
                    let mut properties = HashMap::new();
                    let mut src_vid: Option<Vid> = None;
                    let mut dst_vid: Option<Vid> = None;

                    // Extract properties and VIDs from each column
                    for (col_idx, field) in batch.schema().fields().iter().enumerate() {
                        let col_name = field.name();
                        let column = batch.column(col_idx);
                        let value = self.arrow_value_to_json(column, row_idx)?;

                        if col_name == src_col {
                            let raw = value.as_u64().unwrap_or_else(|| {
                                value.as_str().and_then(|s| s.parse().ok()).unwrap_or(0)
                            });
                            src_vid = Some(Vid::new(raw));
                        } else if col_name == dst_col {
                            let raw = value.as_u64().unwrap_or_else(|| {
                                value.as_str().and_then(|s| s.parse().ok()).unwrap_or(0)
                            });
                            dst_vid = Some(Vid::new(raw));
                        } else if !col_name.starts_with('_') && !value.is_null() {
                            properties.insert(col_name.clone(), value);
                        }
                    }

                    let src = src_vid
                        .ok_or_else(|| anyhow!("Missing source VID column '{}'", src_col))?;
                    let dst = dst_vid
                        .ok_or_else(|| anyhow!("Missing destination VID column '{}'", dst_col))?;

                    // Generate EID and insert edge
                    let mut writer = writer_arc.write().await;
                    let eid = writer.next_eid(edge_type_id).await?;
                    writer
                        .insert_edge(src, dst, edge_type_id, eid, properties)
                        .await?;

                    total_rows += 1;
                }
            }

            log::info!(
                "Imported {} edge rows from '{}' into edge type '{}'",
                total_rows,
                path,
                label
            );

            // Flush to persist edges
            if total_rows > 0 {
                let mut writer = writer_arc.write().await;
                writer.flush_to_l1(None).await?;
            }

            Ok(total_rows)
        } else {
            // Import vertices
            // Validate the label exists in schema
            db_schema
                .label_id_by_name_case_insensitive(label)
                .ok_or_else(|| anyhow!("Label '{}' not found in schema", label))?;

            let mut total_rows = 0;
            for batch in batches {
                let num_rows = batch.num_rows();

                // Convert Arrow batch to rows
                for row_idx in 0..num_rows {
                    let mut properties = HashMap::new();

                    // Extract properties from each column
                    for (col_idx, field) in batch.schema().fields().iter().enumerate() {
                        let col_name = field.name();

                        // Skip internal columns
                        if col_name.starts_with('_') {
                            continue;
                        }

                        let column = batch.column(col_idx);
                        let value = self.arrow_value_to_json(column, row_idx)?;

                        if !value.is_null() {
                            properties.insert(col_name.clone(), value);
                        }
                    }

                    // Generate VID and insert
                    let mut writer = writer_arc.write().await;
                    let vid = writer.next_vid().await?;
                    let _ = writer
                        .insert_vertex_with_labels(vid, properties, vec![label.to_string()])
                        .await?;

                    total_rows += 1;
                }
            }

            log::info!(
                "Imported {} rows from '{}' into label '{}'",
                total_rows,
                path,
                label
            );

            // Flush to persist vertices
            if total_rows > 0 {
                let mut writer = writer_arc.write().await;
                writer.flush_to_l1(None).await?;
            }

            Ok(total_rows)
        }
    }

    fn arrow_value_to_json(&self, column: &arrow_array::ArrayRef, row_idx: usize) -> Result<Value> {
        use arrow_array::Array;
        use arrow_schema::DataType as ArrowDataType;

        if column.is_null(row_idx) {
            return Ok(Value::Null);
        }

        match column.data_type() {
            ArrowDataType::Utf8 => {
                let array = column
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .ok_or_else(|| anyhow!("Failed to downcast to StringArray"))?;
                Ok(Value::String(array.value(row_idx).to_string()))
            }
            ArrowDataType::Int32 => {
                let array = column
                    .as_any()
                    .downcast_ref::<arrow_array::Int32Array>()
                    .ok_or_else(|| anyhow!("Failed to downcast to Int32Array"))?;
                Ok(Value::Int(array.value(row_idx) as i64))
            }
            ArrowDataType::Int64 => {
                let array = column
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .ok_or_else(|| anyhow!("Failed to downcast to Int64Array"))?;
                Ok(Value::Int(array.value(row_idx)))
            }
            ArrowDataType::Float32 => {
                let array = column
                    .as_any()
                    .downcast_ref::<arrow_array::Float32Array>()
                    .ok_or_else(|| anyhow!("Failed to downcast to Float32Array"))?;
                Ok(Value::Float(array.value(row_idx) as f64))
            }
            ArrowDataType::Float64 => {
                let array = column
                    .as_any()
                    .downcast_ref::<arrow_array::Float64Array>()
                    .ok_or_else(|| anyhow!("Failed to downcast to Float64Array"))?;
                Ok(Value::Float(array.value(row_idx)))
            }
            ArrowDataType::Boolean => {
                let array = column
                    .as_any()
                    .downcast_ref::<arrow_array::BooleanArray>()
                    .ok_or_else(|| anyhow!("Failed to downcast to BooleanArray"))?;
                Ok(Value::Bool(array.value(row_idx)))
            }
            ArrowDataType::UInt64 => {
                let array = column
                    .as_any()
                    .downcast_ref::<arrow_array::UInt64Array>()
                    .ok_or_else(|| anyhow!("Failed to downcast to UInt64Array"))?;
                Ok(Value::Int(array.value(row_idx) as i64))
            }
            _ => {
                // For other types, try to convert to string
                let array = column.as_any().downcast_ref::<arrow_array::StringArray>();
                if let Some(arr) = array {
                    Ok(Value::String(arr.value(row_idx).to_string()))
                } else {
                    Ok(Value::Null)
                }
            }
        }
    }

    fn read_parquet_file(&self, path: &str) -> Result<Vec<arrow_array::RecordBatch>> {
        let file = std::fs::File::open(path)?;
        let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?;
        let reader = builder.build()?;

        let mut batches = Vec::new();
        for batch_result in reader {
            batches.push(batch_result?);
        }

        Ok(batches)
    }

    fn read_csv_file(
        &self,
        path: &str,
        label: &str,
        options: &HashMap<String, Value>,
    ) -> Result<Vec<arrow_array::RecordBatch>> {
        use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
        use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
        use std::sync::Arc;

        // Parse CSV options
        let has_headers = options
            .get("headers")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        // Read CSV file
        let file = std::fs::File::open(path)?;
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(has_headers)
            .from_reader(file);

        // Get schema for type conversion
        let db_schema = self.storage.schema_manager().schema();
        let properties = db_schema.properties.get(label);

        // Collect all rows first to determine schema
        let mut rows: Vec<Vec<String>> = Vec::new();
        let headers: Vec<String> = if has_headers {
            rdr.headers()?.iter().map(|s| s.to_string()).collect()
        } else {
            Vec::new()
        };

        for result in rdr.records() {
            let record = result?;
            rows.push(record.iter().map(|s| s.to_string()).collect());
        }

        if rows.is_empty() {
            return Ok(Vec::new());
        }

        // Build Arrow schema with proper types based on DB schema
        let mut arrow_fields: Vec<Arc<Field>> = Vec::new();
        let col_names: Vec<String> = if has_headers {
            headers.clone()
        } else {
            (0..rows[0].len()).map(|i| format!("col{}", i)).collect()
        };

        for name in &col_names {
            let arrow_type = if let Some(props) = properties {
                if let Some(prop_meta) = props.get(name) {
                    match prop_meta.r#type {
                        DataType::Int32 => ArrowDataType::Int32,
                        DataType::Int64 => ArrowDataType::Int64,
                        DataType::Float32 => ArrowDataType::Float32,
                        DataType::Float64 => ArrowDataType::Float64,
                        DataType::Bool => ArrowDataType::Boolean,
                        _ => ArrowDataType::Utf8,
                    }
                } else {
                    ArrowDataType::Utf8
                }
            } else {
                ArrowDataType::Utf8
            };
            arrow_fields.push(Arc::new(Field::new(name, arrow_type, true)));
        }

        let arrow_schema = Arc::new(ArrowSchema::new(arrow_fields.clone()));

        // Convert rows to Arrow arrays with proper types
        let mut columns: Vec<ArrayRef> = Vec::new();
        for (col_idx, field) in arrow_fields.iter().enumerate() {
            match field.data_type() {
                ArrowDataType::Int32 => {
                    let values: Vec<Option<i32>> = rows
                        .iter()
                        .map(|row| {
                            if col_idx < row.len() {
                                row[col_idx].parse().ok()
                            } else {
                                None
                            }
                        })
                        .collect();
                    columns.push(Arc::new(Int32Array::from(values)));
                }
                _ => {
                    // Default to string
                    let values: Vec<Option<String>> = rows
                        .iter()
                        .map(|row| {
                            if col_idx < row.len() {
                                Some(row[col_idx].clone())
                            } else {
                                None
                            }
                        })
                        .collect();
                    columns.push(Arc::new(StringArray::from(values)));
                }
            }
        }

        let batch = RecordBatch::try_new(arrow_schema, columns)?;
        Ok(vec![batch])
    }

    fn parse_data_type(type_str: &str) -> Result<DataType> {
        use uni_common::core::schema::{CrdtType, PointType};
        let type_str = type_str.to_lowercase();
        let type_str = type_str.trim();
        match type_str {
            "string" | "text" | "varchar" => Ok(DataType::String),
            "int" | "integer" | "int32" => Ok(DataType::Int32),
            "long" | "int64" | "bigint" => Ok(DataType::Int64),
            "float" | "float32" | "real" => Ok(DataType::Float32),
            "double" | "float64" => Ok(DataType::Float64),
            "bool" | "boolean" => Ok(DataType::Bool),
            "timestamp" => Ok(DataType::Timestamp),
            "date" => Ok(DataType::Date),
            "time" => Ok(DataType::Time),
            "datetime" => Ok(DataType::DateTime),
            "duration" => Ok(DataType::Duration),
            "json" | "jsonb" => Ok(DataType::Json),
            "point" => Ok(DataType::Point(PointType::Cartesian2D)),
            "point3d" => Ok(DataType::Point(PointType::Cartesian3D)),
            "geopoint" | "geographic" => Ok(DataType::Point(PointType::Geographic)),
            s if s.starts_with("vector(") && s.ends_with(')') => {
                let dims_str = &s[7..s.len() - 1];
                let dimensions = dims_str
                    .parse::<usize>()
                    .map_err(|_| anyhow!("Invalid vector dimensions: {}", dims_str))?;
                Ok(DataType::Vector { dimensions })
            }
            s if s.starts_with("list<") && s.ends_with('>') => {
                let inner_type_str = &s[5..s.len() - 1];
                let inner_type = Self::parse_data_type(inner_type_str)?;
                Ok(DataType::List(Box::new(inner_type)))
            }
            "gcounter" => Ok(DataType::Crdt(CrdtType::GCounter)),
            "lwwregister" => Ok(DataType::Crdt(CrdtType::LWWRegister)),
            _ => Err(anyhow!("Unknown data type: {}", type_str)),
        }
    }

    pub(crate) async fn execute_create_label(&self, clause: CreateLabel) -> Result<()> {
        let sm = self.storage.schema_manager_arc();
        if clause.if_not_exists && sm.schema().labels.contains_key(&clause.name) {
            return Ok(());
        }
        sm.add_label(&clause.name)?;
        for prop in clause.properties {
            let dt = Self::parse_data_type(&prop.data_type)?;
            sm.add_property(&clause.name, &prop.name, dt, prop.nullable)?;
            if prop.unique {
                let constraint = Constraint {
                    name: format!("{}_{}_unique", clause.name, prop.name),
                    constraint_type: ConstraintType::Unique {
                        properties: vec![prop.name],
                    },
                    target: ConstraintTarget::Label(clause.name.clone()),
                    enabled: true,
                };
                sm.add_constraint(constraint)?;
            }
        }
        sm.save().await?;
        Ok(())
    }

    pub(crate) async fn enrich_properties_with_generated_columns(
        &self,
        label_name: &str,
        properties: &mut HashMap<String, Value>,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<()> {
        let schema = self.storage.schema_manager().schema();

        if let Some(props_meta) = schema.properties.get(label_name) {
            let mut generators = Vec::new();
            for (prop_name, meta) in props_meta {
                if let Some(expr_str) = &meta.generation_expression {
                    generators.push((prop_name.clone(), expr_str.clone()));
                }
            }

            for (prop_name, expr_str) in generators {
                let cache_key = (label_name.to_string(), prop_name.clone());
                let expr = {
                    let cache = self.gen_expr_cache.read().await;
                    cache.get(&cache_key).cloned()
                };

                let expr = match expr {
                    Some(e) => e,
                    None => {
                        let parsed = uni_cypher::parse_expression(&expr_str)
                            .map_err(|e| anyhow!("Failed to parse generation expression: {}", e))?;
                        let mut cache = self.gen_expr_cache.write().await;
                        cache.insert(cache_key, parsed.clone());
                        parsed
                    }
                };

                let mut scope = HashMap::new();

                // If expression has an explicit variable, use it as an object
                if let Some(var) = expr.extract_variable() {
                    let props_map: HashMap<String, Value> = properties
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    scope.insert(var, Value::Map(props_map));
                } else {
                    // No explicit variable - add properties directly to scope for bare references
                    // e.g., "lower(email)" can reference "email" directly
                    for (k, v) in properties.iter() {
                        scope.insert(k.clone(), v.clone());
                    }
                }

                let val = self
                    .evaluate_expr(&expr, &scope, prop_manager, params, ctx)
                    .await?;
                properties.insert(prop_name, val);
            }
        }
        Ok(())
    }

    pub(crate) async fn execute_create_edge_type(&self, clause: CreateEdgeType) -> Result<()> {
        let sm = self.storage.schema_manager_arc();
        if clause.if_not_exists && sm.schema().edge_types.contains_key(&clause.name) {
            return Ok(());
        }
        sm.add_edge_type(&clause.name, clause.src_labels, clause.dst_labels)?;
        for prop in clause.properties {
            let dt = Self::parse_data_type(&prop.data_type)?;
            sm.add_property(&clause.name, &prop.name, dt, prop.nullable)?;
        }
        sm.save().await?;
        Ok(())
    }

    /// Executes an ALTER action on a schema entity.
    ///
    /// This is a shared helper for both `execute_alter_label` and
    /// `execute_alter_edge_type` since they have identical logic.
    pub(crate) async fn execute_alter_entity(
        sm: &Arc<SchemaManager>,
        entity_name: &str,
        action: AlterAction,
    ) -> Result<()> {
        match action {
            AlterAction::AddProperty(prop) => {
                let dt = Self::parse_data_type(&prop.data_type)?;
                sm.add_property(entity_name, &prop.name, dt, prop.nullable)?;
            }
            AlterAction::DropProperty(prop_name) => {
                sm.drop_property(entity_name, &prop_name)?;
            }
            AlterAction::RenameProperty { old_name, new_name } => {
                sm.rename_property(entity_name, &old_name, &new_name)?;
            }
        }
        sm.save().await?;
        Ok(())
    }

    pub(crate) async fn execute_alter_label(&self, clause: AlterLabel) -> Result<()> {
        Self::execute_alter_entity(
            &self.storage.schema_manager_arc(),
            &clause.name,
            clause.action,
        )
        .await
    }

    pub(crate) async fn execute_alter_edge_type(&self, clause: AlterEdgeType) -> Result<()> {
        Self::execute_alter_entity(
            &self.storage.schema_manager_arc(),
            &clause.name,
            clause.action,
        )
        .await
    }

    pub(crate) async fn execute_drop_label(&self, clause: DropLabel) -> Result<()> {
        let sm = self.storage.schema_manager_arc();
        sm.drop_label(&clause.name, clause.if_exists)?;
        sm.save().await?;
        Ok(())
    }

    pub(crate) async fn execute_drop_edge_type(&self, clause: DropEdgeType) -> Result<()> {
        let sm = self.storage.schema_manager_arc();
        sm.drop_edge_type(&clause.name, clause.if_exists)?;
        sm.save().await?;
        Ok(())
    }

    pub(crate) async fn execute_create_constraint(&self, clause: CreateConstraint) -> Result<()> {
        let sm = self.storage.schema_manager_arc();
        let target = ConstraintTarget::Label(clause.label);
        let c_type = match clause.constraint_type {
            AstConstraintType::Unique | AstConstraintType::NodeKey => ConstraintType::Unique {
                properties: clause.properties,
            },
            AstConstraintType::Exists => {
                let property = clause
                    .properties
                    .into_iter()
                    .next()
                    .ok_or_else(|| anyhow!("EXISTS constraint requires a property"))?;
                ConstraintType::Exists { property }
            }
            AstConstraintType::Check => {
                let expression = clause
                    .expression
                    .ok_or_else(|| anyhow!("CHECK constraint requires an expression"))?;
                ConstraintType::Check {
                    expression: expression.to_string_repr(),
                }
            }
        };

        let constraint = Constraint {
            name: clause.name.unwrap_or_else(|| "auto_constraint".to_string()),
            constraint_type: c_type,
            target,
            enabled: true,
        };

        sm.add_constraint(constraint)?;
        sm.save().await?;
        Ok(())
    }

    pub(crate) async fn execute_drop_constraint(&self, clause: DropConstraint) -> Result<()> {
        let sm = self.storage.schema_manager_arc();
        sm.drop_constraint(&clause.name, false)?;
        sm.save().await?;
        Ok(())
    }

    fn get_composite_constraint(&self, label: &str) -> Option<Constraint> {
        let schema = self.storage.schema_manager().schema();
        schema
            .constraints
            .iter()
            .find(|c| {
                if !c.enabled {
                    return false;
                }
                match &c.target {
                    ConstraintTarget::Label(l) if l == label => {
                        matches!(c.constraint_type, ConstraintType::Unique { .. })
                    }
                    _ => false,
                }
            })
            .cloned()
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn execute_merge(
        &self,
        rows: Vec<HashMap<String, Value>>,
        pattern: &Pattern,
        on_match: Option<&SetClause>,
        on_create: Option<&SetClause>,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        let writer_lock = self
            .writer
            .as_ref()
            .ok_or_else(|| anyhow!("Write operation requires a Writer"))?;

        let mut results = Vec::new();
        for mut row in rows {
            // Optimization: Check for single node pattern with unique constraint
            let mut optimized_vid = None;
            if pattern.paths.len() == 1 {
                let path = &pattern.paths[0];
                if path.elements.len() == 1
                    && let PatternElement::Node(n) = &path.elements[0]
                    && n.labels.len() == 1
                    && let Some(constraint) = self.get_composite_constraint(&n.labels[0])
                    && let ConstraintType::Unique { properties } = constraint.constraint_type
                {
                    let label = &n.labels[0];
                    // Evaluate pattern properties
                    let mut pattern_props = HashMap::new();
                    if let Some(props_expr) = &n.properties {
                        let val = self
                            .evaluate_expr(props_expr, &row, prop_manager, params, ctx)
                            .await?;
                        if let Value::Map(map) = val {
                            for (k, v) in map {
                                pattern_props.insert(k, v);
                            }
                        }
                    }

                    // Check if all constraint properties are present
                    let has_all_keys = properties.iter().all(|p| pattern_props.contains_key(p));
                    if has_all_keys {
                        // Extract key properties and convert to serde_json::Value for index lookup
                        let key_props: HashMap<String, serde_json::Value> = properties
                            .iter()
                            .filter_map(|p| {
                                pattern_props
                                    .get(p)
                                    .map(|v| (p.clone(), v.clone().into()))
                            })
                            .collect();

                        // Use optimized lookup
                        if let Ok(Some(vid)) = self
                            .storage
                            .index_manager()
                            .composite_lookup(label, &key_props)
                            .await
                        {
                            optimized_vid = Some((vid, pattern_props));
                        }
                    }
                }
            }

            if let Some((vid, _pattern_props)) = optimized_vid {
                // Optimized Path: Node found via index
                let mut writer = writer_lock.write().await;
                let mut match_row = row.clone();
                if let PatternElement::Node(n) = &pattern.paths[0].elements[0]
                    && let Some(var) = &n.variable
                {
                    match_row.insert(var.clone(), Value::Int(vid.as_u64() as i64));
                }

                if let Some(set) = on_match {
                    self.execute_set_items_locked(
                        &set.items,
                        &mut match_row,
                        &mut writer,
                        prop_manager,
                        params,
                        ctx,
                    )
                    .await?;
                }
                results.push(match_row);
            } else {
                // Fallback to standard execution
                let matches = self
                    .execute_merge_match(pattern, &row, prop_manager, params, ctx)
                    .await?;
                let mut writer = writer_lock.write().await;
                if !matches.is_empty() {
                    for mut m in matches {
                        if let Some(set) = on_match {
                            self.execute_set_items_locked(
                                &set.items,
                                &mut m,
                                &mut writer,
                                prop_manager,
                                params,
                                ctx,
                            )
                            .await?;
                        }
                        results.push(m);
                    }
                } else {
                    self.execute_create_pattern(
                        pattern,
                        &mut row,
                        &mut writer,
                        prop_manager,
                        params,
                        ctx,
                    )
                    .await?;
                    if let Some(set) = on_create {
                        self.execute_set_items_locked(
                            &set.items,
                            &mut row,
                            &mut writer,
                            prop_manager,
                            params,
                            ctx,
                        )
                        .await?;
                    }
                    results.push(row);
                }
            }
        }
        Ok(results)
    }

    /// Execute a CREATE pattern, inserting new vertices and edges into the graph.
    pub(crate) async fn execute_create_pattern(
        &self,
        pattern: &Pattern,
        row: &mut HashMap<String, Value>,
        writer: &mut Writer,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<()> {
        for path in &pattern.paths {
            let mut prev_vid: Option<Vid> = None;
            // (rel_var, type_id, type_name, props_expr, direction)
            type PendingRel = (String, u32, String, Option<Expr>, Direction);
            let mut rel_pending: Option<PendingRel> = None;

            for element in &path.elements {
                match element {
                    PatternElement::Node(n) => {
                        let mut vid = None;

                        // Check if node variable already bound in row
                        if let Some(var) = &n.variable
                            && let Some(val) = row.get(var)
                            && let Ok(existing_vid) = Self::vid_from_value(val)
                        {
                            vid = Some(existing_vid);
                        }

                        // If not bound, create it
                        if vid.is_none() {
                            let mut props = HashMap::new();
                            if let Some(props_expr) = &n.properties {
                                let props_val = self
                                    .evaluate_expr(props_expr, row, prop_manager, params, ctx)
                                    .await?;
                                if let Value::Map(map) = props_val {
                                    for (k, v) in map {
                                        props.insert(k, v);
                                    }
                                } else {
                                    return Err(anyhow!("Properties must evaluate to a map"));
                                }
                            }

                            // Support unlabeled nodes and unknown labels (schemaless)
                            let schema = self.storage.schema_manager().schema();

                            // VID generation is label-independent
                            let new_vid = writer.next_vid().await?;

                            // Enrich with generated columns only for known labels
                            for label_name in &n.labels {
                                if schema.get_label_case_insensitive(label_name).is_some() {
                                    self.enrich_properties_with_generated_columns(
                                        label_name,
                                        &mut props,
                                        prop_manager,
                                        params,
                                        ctx,
                                    )
                                    .await?;
                                }
                            }

                            // Insert vertex and get back final properties (includes auto-generated embeddings)
                            let final_props = writer
                                .insert_vertex_with_labels(new_vid, props, n.labels.clone())
                                .await?;

                            // Build node object with final properties (includes embeddings)
                            if let Some(var) = &n.variable {
                                let mut obj = HashMap::new();
                                obj.insert("_vid".to_string(), Value::Int(new_vid.as_u64() as i64));
                                for (k, v) in &final_props {
                                    obj.insert(k.clone(), v.clone());
                                }
                                // Store node as a Map with _vid, matching MATCH behavior
                                row.insert(var.clone(), Value::Map(obj));
                            }
                            vid = Some(new_vid);
                        }

                        let current_vid = vid.unwrap();

                        if let Some((rel_var, type_id, type_name, rel_props_expr, dir)) =
                            rel_pending.take()
                            && let Some(src) = prev_vid
                        {
                            let is_rel_bound = !rel_var.is_empty() && row.contains_key(&rel_var);

                            if !is_rel_bound {
                                let mut rel_props = HashMap::new();
                                if let Some(expr) = rel_props_expr {
                                    let val = self
                                        .evaluate_expr(&expr, row, prop_manager, params, ctx)
                                        .await?;
                                    if let Value::Map(map) = val {
                                        rel_props.extend(map);
                                    }
                                }
                                let eid = writer.next_eid(type_id).await?;

                                // For incoming edges like (a)<-[:R]-(b), swap so the edge points b -> a
                                let (edge_src, edge_dst) = match dir {
                                    Direction::Incoming => (current_vid, src),
                                    _ => (src, current_vid),
                                };

                                writer
                                    .insert_edge(edge_src, edge_dst, type_id, eid, rel_props)
                                    .await?;

                                // Store edge type name for all edges
                                writer.set_edge_type(eid, type_name.clone());

                                if !rel_var.is_empty() {
                                    let mut edge_map = HashMap::new();
                                    edge_map.insert("_eid".to_string(), Value::Int(eid.as_u64() as i64));
                                    edge_map.insert("_src".to_string(), Value::Int(edge_src.as_u64() as i64));
                                    edge_map.insert("_dst".to_string(), Value::Int(edge_dst.as_u64() as i64));
                                    edge_map.insert("_type".to_string(), Value::Int(type_id as i64));
                                    row.insert(rel_var, Value::Map(edge_map));
                                }
                            }
                        }
                        prev_vid = Some(current_vid);
                    }
                    PatternElement::Relationship(r) => {
                        if r.types.len() != 1 {
                            return Err(anyhow!(
                                "CREATE relationship must specify exactly one type"
                            ));
                        }
                        let type_name = &r.types[0];
                        // Get or assign edge type ID (schemaless types get bit 31 = 1)
                        let type_id = self
                            .storage
                            .schema_manager()
                            .get_or_assign_edge_type_id(type_name);

                        rel_pending = Some((
                            r.variable.clone().unwrap_or_default(),
                            type_id,
                            type_name.clone(),
                            r.properties.clone(),
                            r.direction.clone(),
                        ));
                    }
                    PatternElement::Parenthesized { .. } => {
                        return Err(anyhow!("Parenthesized pattern not supported in CREATE"));
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn execute_set_items_locked(
        &self,
        items: &[SetItem],
        row: &mut HashMap<String, Value>,
        writer: &mut Writer,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<()> {
        for item in items {
            match item {
                SetItem::Property { expr, value } => {
                    if let Expr::Property(var_expr, prop_name) = expr
                        && let Expr::Variable(var_name) = &**var_expr
                        && let Some(node_val) = row.get(var_name)
                    {
                        if let Ok(vid) = Self::vid_from_value(node_val) {
                            let mut props = prop_manager
                                .get_all_vertex_props_with_ctx(vid, ctx)
                                .await?
                                .unwrap_or_default();
                            let val = self
                                .evaluate_expr(value, row, prop_manager, params, ctx)
                                .await?;
                            props.insert(prop_name.clone(), val);

                            // Enrich with generated columns
                            // In the new storage model, get labels from the node value or context
                            let labels =
                                Self::extract_labels_from_node(node_val).unwrap_or_default();
                            for label_name in &labels {
                                self.enrich_properties_with_generated_columns(
                                    label_name,
                                    &mut props,
                                    prop_manager,
                                    params,
                                    ctx,
                                )
                                .await?;
                            }

                            let _ = writer.insert_vertex_with_labels(vid, props, labels).await?;
                        } else if let Value::Map(map) = node_val
                            && let (Some(eid_v), Some(src_v), Some(dst_v), Some(type_v)) = (
                                map.get("_eid"),
                                map.get("_src"),
                                map.get("_dst"),
                                map.get("_type"),
                            )
                        {
                            let eid = uni_common::core::id::Eid::from(
                                eid_v.as_u64().ok_or(anyhow!("Invalid _eid"))?,
                            );
                            let src = Vid::from(src_v.as_u64().ok_or(anyhow!("Invalid _src"))?);
                            let dst = Vid::from(dst_v.as_u64().ok_or(anyhow!("Invalid _dst"))?);
                            let etype = type_v.as_u64().ok_or(anyhow!("Invalid _type"))? as u32;

                            let mut props = prop_manager
                                .get_all_edge_props_with_ctx(eid, ctx)
                                .await?
                                .unwrap_or_default();
                            let val = self
                                .evaluate_expr(value, row, prop_manager, params, ctx)
                                .await?;
                            props.insert(prop_name.clone(), val);
                            writer.insert_edge(src, dst, etype, eid, props).await?;
                        }
                    }
                }
                SetItem::Labels { variable, labels } => {
                    if let Some(node_val) = row.get(variable)
                        && let Ok(vid) = Self::vid_from_value(node_val)
                    {
                        // Get current labels from node value
                        let current_labels =
                            Self::extract_labels_from_node(node_val).unwrap_or_default();

                        // Determine new labels to add (skip duplicates)
                        let labels_to_add: Vec<_> = labels
                            .iter()
                            .filter(|l| !current_labels.contains(l))
                            .cloned()
                            .collect();

                        if !labels_to_add.is_empty() {
                            // Validate that all new labels exist in schema
                            let schema = self.storage.schema_manager().schema();
                            for label in &labels_to_add {
                                if schema.get_label_case_insensitive(label).is_none() {
                                    return Err(anyhow!("Label {} not found in schema", label));
                                }
                            }

                            // Add labels via L0Buffer
                            if let Some(ctx) = ctx {
                                ctx.l0.write().add_vertex_labels(vid, labels_to_add.clone());
                            }

                            // Update the node value in the row with new labels
                            if let Some(Value::Map(obj)) = row.get_mut(variable) {
                                let mut updated_labels = current_labels;
                                updated_labels.extend(labels_to_add);
                                let labels_list = updated_labels.into_iter().map(Value::String).collect();
                                obj.insert("_labels".to_string(), Value::List(labels_list));
                            }
                        }
                    }
                }
                SetItem::Variable { .. } => {
                    return Err(anyhow!(
                        "SET variable = expression is not yet supported in AST migration. \
                         Use SET variable.property = value instead."
                    ));
                }
                SetItem::VariablePlus { .. } => {
                    return Err(anyhow!(
                        "SET variable += expression is not yet supported in AST migration. \
                         Use SET variable.property = value instead."
                    ));
                }
            }
        }
        Ok(())
    }

    /// Execute REMOVE clause items (property removal or label removal).
    pub(crate) async fn execute_remove_items_locked(
        &self,
        items: &[RemoveItem],
        row: &mut HashMap<String, Value>,
        writer: &mut Writer,
        prop_manager: &PropertyManager,
        ctx: Option<&QueryContext>,
    ) -> Result<()> {
        for item in items {
            match item {
                RemoveItem::Property(expr) => {
                    self.execute_remove_property(expr, row, writer, prop_manager, ctx)
                        .await?;
                }
                RemoveItem::Labels { variable, labels } => {
                    self.execute_remove_labels(variable, labels, row, ctx)?;
                }
            }
        }
        Ok(())
    }

    /// Execute property removal for a vertex or edge.
    pub(crate) async fn execute_remove_property(
        &self,
        expr: &Expr,
        row: &mut HashMap<String, Value>,
        writer: &mut Writer,
        prop_manager: &PropertyManager,
        ctx: Option<&QueryContext>,
    ) -> Result<()> {
        if let Expr::Property(var_expr, prop_name) = expr
            && let Expr::Variable(var_name) = &**var_expr
            && let Some(node_val) = row.get(var_name)
        {
            if let Ok(vid) = Self::vid_from_value(node_val) {
                // Remove property from vertex
                let mut props = prop_manager
                    .get_all_vertex_props_with_ctx(vid, ctx)
                    .await?
                    .unwrap_or_default();
                props.insert(prop_name.clone(), Value::Null);
                let labels = Self::extract_labels_from_node(node_val).unwrap_or_default();
                let _ = writer.insert_vertex_with_labels(vid, props, labels).await?;

                // Update the row to reflect the property removal
                if let Some(Value::Map(node_map)) = row.get_mut(var_name) {
                    node_map.insert(prop_name.clone(), Value::Null);
                }
            } else if let Value::Map(map) = node_val {
                // Remove property from edge
                self.execute_remove_edge_property(map, prop_name, writer, prop_manager, ctx)
                    .await?;

                // Update the row to reflect the property removal
                if let Some(Value::Map(edge_map)) = row.get_mut(var_name) {
                    edge_map.insert(prop_name.clone(), Value::Null);
                }
            }
        }
        Ok(())
    }

    /// Execute property removal from an edge.
    pub(crate) async fn execute_remove_edge_property(
        &self,
        map: &HashMap<String, Value>,
        prop_name: &str,
        writer: &mut Writer,
        prop_manager: &PropertyManager,
        ctx: Option<&QueryContext>,
    ) -> Result<()> {
        if let (Some(eid_v), Some(src_v), Some(dst_v), Some(type_v)) = (
            map.get("_eid"),
            map.get("_src"),
            map.get("_dst"),
            map.get("_type"),
        ) {
            let eid =
                uni_common::core::id::Eid::from(eid_v.as_u64().ok_or(anyhow!("Invalid _eid"))?);
            let src = Vid::from(src_v.as_u64().ok_or(anyhow!("Invalid _src"))?);
            let dst = Vid::from(dst_v.as_u64().ok_or(anyhow!("Invalid _dst"))?);
            let etype = type_v.as_u64().ok_or(anyhow!("Invalid _type"))? as u32;

            let mut props = prop_manager
                .get_all_edge_props_with_ctx(eid, ctx)
                .await?
                .unwrap_or_default();
            props.insert(prop_name.to_string(), Value::Null);
            writer.insert_edge(src, dst, etype, eid, props).await?;
        }
        Ok(())
    }

    /// Execute label removal.
    pub(crate) fn execute_remove_labels(
        &self,
        variable: &str,
        labels: &[String],
        row: &mut HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<()> {
        if let Some(node_val) = row.get(variable)
            && let Ok(vid) = Self::vid_from_value(node_val)
        {
            // Get current labels from node value
            let current_labels = Self::extract_labels_from_node(node_val).unwrap_or_default();

            // Determine which labels to actually remove (only those currently present)
            let labels_to_remove: Vec<_> = labels
                .iter()
                .filter(|l| current_labels.contains(l))
                .collect();

            if !labels_to_remove.is_empty() {
                // Remove labels via L0Buffer
                if let Some(ctx) = ctx {
                    let mut l0 = ctx.l0.write();
                    for label in &labels_to_remove {
                        l0.remove_vertex_label(vid, label);
                    }
                }

                // Update the node value in the row with remaining labels
                if let Some(Value::Map(obj)) = row.get_mut(variable) {
                    let remaining_labels: Vec<_> = current_labels
                        .iter()
                        .filter(|l| !labels_to_remove.contains(l))
                        .cloned()
                        .collect();
                    let labels_list = remaining_labels.into_iter().map(Value::String).collect();
                    obj.insert("_labels".to_string(), Value::List(labels_list));
                }
            }
        }
        Ok(())
    }

    /// Execute DELETE clause for a single item (vertex or edge).
    pub(crate) async fn execute_delete_item_locked(
        &self,
        val: &Value,
        detach: bool,
        writer: &mut Writer,
    ) -> Result<()> {
        if let Ok(vid) = Self::vid_from_value(val) {
            let labels = Self::extract_labels_from_node(val);
            self.execute_delete_vertex(vid, detach, labels, writer)
                .await?;
        } else if let Value::Map(map) = val {
            self.execute_delete_edge_from_map(map, writer).await?;
        }
        Ok(())
    }

    /// Execute vertex deletion with optional detach.
    pub(crate) async fn execute_delete_vertex(
        &self,
        vid: Vid,
        detach: bool,
        labels: Option<Vec<String>>,
        writer: &mut Writer,
    ) -> Result<()> {
        if detach {
            self.detach_delete_vertex(vid, writer).await?;
        } else {
            self.check_vertex_has_no_edges(vid, writer).await?;
        }
        writer.delete_vertex(vid, labels).await?;
        Ok(())
    }

    /// Check that a vertex has no edges (required for non-DETACH DELETE).
    pub(crate) async fn check_vertex_has_no_edges(&self, vid: Vid, writer: &Writer) -> Result<()> {
        let schema = self.storage.schema_manager().schema();
        let edge_type_ids: Vec<u32> = schema.edge_types.values().map(|m| m.id).collect();

        let out_graph = self
            .storage
            .load_subgraph_cached(
                &[vid],
                &edge_type_ids,
                1,
                uni_store::runtime::Direction::Outgoing,
                Some(writer.l0_manager.get_current()),
            )
            .await?;
        let has_out = out_graph.edges().next().is_some();

        let in_graph = self
            .storage
            .load_subgraph_cached(
                &[vid],
                &edge_type_ids,
                1,
                uni_store::runtime::Direction::Incoming,
                Some(writer.l0_manager.get_current()),
            )
            .await?;
        let has_in = in_graph.edges().next().is_some();

        if has_out || has_in {
            return Err(anyhow!(
                "Cannot delete node {}, because it still has relationships. To delete the node and its relationships, use DETACH DELETE.",
                vid
            ));
        }
        Ok(())
    }

    /// Execute edge deletion from a map representation.
    pub(crate) async fn execute_delete_edge_from_map(
        &self,
        map: &HashMap<String, Value>,
        writer: &mut Writer,
    ) -> Result<()> {
        if let (Some(eid_v), Some(src_v), Some(dst_v), Some(type_v)) = (
            map.get("_eid"),
            map.get("_src"),
            map.get("_dst"),
            map.get("_type"),
        ) {
            let eid =
                uni_common::core::id::Eid::from(eid_v.as_u64().ok_or(anyhow!("Invalid _eid"))?);
            let src = Vid::from(src_v.as_u64().ok_or(anyhow!("Invalid _src"))?);
            let dst = Vid::from(dst_v.as_u64().ok_or(anyhow!("Invalid _dst"))?);
            let etype = type_v.as_u64().ok_or(anyhow!("Invalid _type"))? as u32;
            writer.delete_edge(eid, src, dst, etype).await?;
        }
        Ok(())
    }

    pub(crate) async fn execute_merge_match(
        &self,
        pattern: &Pattern,
        row: &HashMap<String, Value>,
        prop_manager: &PropertyManager,
        params: &HashMap<String, Value>,
        ctx: Option<&QueryContext>,
    ) -> Result<Vec<HashMap<String, Value>>> {
        // Construct a LogicalPlan for the MATCH part of MERGE
        let planner = crate::query::planner::QueryPlanner::new(Arc::new(
            self.storage.schema_manager().schema().clone(),
        ));

        // We need to construct a CypherQuery to use the planner's plan() method,
        // or we can manually construct the LogicalPlan.
        // Manual construction is safer as we don't have to round-trip through AST.

        let mut plan = LogicalPlan::Empty;
        let mut vars_in_scope = Vec::new();

        // Add existing bound variables from row to scope
        for key in row.keys() {
            vars_in_scope.push(key.clone());
        }

        // Reconstruct Match logic from Planner (simplified for MERGE pattern)
        for path in &pattern.paths {
            let elements = &path.elements;
            let mut i = 0;
            while i < elements.len() {
                let part = &elements[i];
                match part {
                    PatternElement::Node(n) => {
                        let variable = n.variable.clone().unwrap_or_default();

                        // If variable is already bound in the input row, we filter
                        let is_bound = !variable.is_empty() && row.contains_key(&variable);

                        if is_bound {
                            // If bound, we must Scan this specific VID to start the chain
                            // Extract VID from row
                            let val = row.get(&variable).unwrap();
                            let vid = Self::vid_from_value(val)?;

                            // In the new storage model, VIDs don't embed label info.
                            // We get label from the node value if available, otherwise use 0 to scan all.
                            let extracted_labels =
                                Self::extract_labels_from_node(val).unwrap_or_default();
                            let label_id = {
                                let schema = self.storage.schema_manager().schema();
                                extracted_labels
                                    .first()
                                    .and_then(|l| schema.label_id_by_name(l))
                                    .unwrap_or(0)
                            };

                            let prop_filter = planner.properties_to_expr(&variable, &n.properties);

                            // Create a filter expression for VID: variable._vid = vid
                            // But our expression engine handles `Expr::Variable` as column.
                            // We can inject a filter `id(variable) = vid` if we had `id()` function.
                            // Or we use internal property `_vid`.

                            // Note: Scan supports `filter`.
                            // We can manually construct an Expr::BinaryOp(Eq, Prop(var, _vid), Literal(vid))

                            let vid_filter = Expr::BinaryOp {
                                left: Box::new(Expr::Property(
                                    Box::new(Expr::Variable(variable.clone())),
                                    "_vid".to_string(),
                                )),
                                op: BinaryOp::Eq,
                                right: Box::new(Expr::Literal(CypherLiteral::Integer(
                                    vid.as_u64() as i64,
                                ))),
                            };

                            let combined_filter = if let Some(pf) = prop_filter {
                                Some(Expr::BinaryOp {
                                    left: Box::new(vid_filter),
                                    op: BinaryOp::And,
                                    right: Box::new(pf),
                                })
                            } else {
                                Some(vid_filter)
                            };

                            let scan = LogicalPlan::Scan {
                                label_id,
                                labels: extracted_labels,
                                variable: variable.clone(),
                                filter: combined_filter,
                                optional: false,
                            };

                            if matches!(plan, LogicalPlan::Empty) {
                                plan = scan;
                            } else {
                                plan = LogicalPlan::CrossJoin {
                                    left: Box::new(plan),
                                    right: Box::new(scan),
                                };
                            }
                        } else {
                            if n.labels.is_empty() {
                                return Err(anyhow!("MERGE node must have a label"));
                            }
                            let label_name = &n.labels[0];
                            let schema = self.storage.schema_manager().schema();
                            let label_meta = schema
                                .labels
                                .get(label_name)
                                .ok_or_else(|| anyhow!("Label {} not found", label_name))?;

                            let prop_filter = planner.properties_to_expr(&variable, &n.properties);
                            let scan = LogicalPlan::Scan {
                                label_id: label_meta.id,
                                labels: n.labels.clone(),
                                variable: variable.clone(),
                                filter: prop_filter,
                                optional: false, // MERGE MATCH is strict
                            };

                            if matches!(plan, LogicalPlan::Empty) {
                                plan = scan;
                            } else {
                                plan = LogicalPlan::CrossJoin {
                                    left: Box::new(plan),
                                    right: Box::new(scan),
                                };
                            }

                            if !variable.is_empty() {
                                vars_in_scope.push(variable.clone());
                            }
                        }

                        // Now look ahead for relationship
                        i += 1;
                        while i < elements.len() {
                            if let PatternElement::Relationship(r) = &elements[i] {
                                let target_node_part = &elements[i + 1];
                                if let PatternElement::Node(n_target) = target_node_part {
                                    let schema = self.storage.schema_manager().schema();
                                    let mut edge_type_ids = Vec::new();

                                    if r.types.is_empty() {
                                        return Err(anyhow!("MERGE edge must have a type"));
                                    } else if r.types.len() > 1 {
                                        return Err(anyhow!(
                                            "MERGE does not support multiple edge types"
                                        ));
                                    } else {
                                        let type_name = &r.types[0];
                                        let edge_meta = schema
                                            .get_edge_type_case_insensitive(type_name)
                                            .ok_or_else(|| {
                                                anyhow!("Edge type {} not found", type_name)
                                            })?;
                                        edge_type_ids.push(edge_meta.id);
                                    }

                                    let target_label_meta = if let Some(lbl) =
                                        n_target.labels.first()
                                    {
                                        schema
                                            .get_label_case_insensitive(lbl)
                                            .ok_or_else(|| anyhow!("Label {} not found", lbl))?
                                    } else if let Some(var) = &n_target.variable {
                                        if let Some(val) = row.get(var) {
                                            // In the new storage model, get labels from node value
                                            if let Some(labels) =
                                                Self::extract_labels_from_node(val)
                                            {
                                                if let Some(first_label) = labels.first() {
                                                    schema
                                                        .get_label_case_insensitive(first_label)
                                                        .ok_or_else(|| {
                                                        anyhow!("Label {} not found", first_label)
                                                    })?
                                                } else {
                                                    return Err(anyhow!(
                                                        "Variable {} has no labels",
                                                        var
                                                    ));
                                                }
                                            } else if Self::vid_from_value(val).is_ok() {
                                                return Err(anyhow!(
                                                    "Variable {} is a node without labels info",
                                                    var
                                                ));
                                            } else {
                                                return Err(anyhow!(
                                                    "Variable {} is not a node",
                                                    var
                                                ));
                                            }
                                        } else {
                                            return Err(anyhow!(
                                                "MERGE pattern node must have a label or be a bound variable"
                                            ));
                                        }
                                    } else {
                                        return Err(anyhow!(
                                            "MERGE pattern node must have a label"
                                        ));
                                    };

                                    let target_variable =
                                        n_target.variable.clone().unwrap_or_default();
                                    let source_variable = match &elements[i - 1] {
                                        PatternElement::Node(n) => {
                                            n.variable.clone().unwrap_or_default()
                                        }
                                        _ => String::new(),
                                    };

                                    plan = LogicalPlan::Traverse {
                                        input: Box::new(plan),
                                        edge_type_ids,
                                        direction: r.direction.clone(),
                                        source_variable,
                                        target_variable: target_variable.clone(),
                                        target_label_id: target_label_meta.id,
                                        step_variable: r.variable.clone(),
                                        min_hops: r.range.as_ref().and_then(|r| r.min).unwrap_or(1)
                                            as usize,
                                        max_hops: r.range.as_ref().and_then(|r| r.max).unwrap_or(1)
                                            as usize,
                                        optional: false,
                                        target_filter: None,
                                        path_variable: None,
                                        edge_properties: std::collections::HashSet::new(),
                                        optional_pattern_vars: std::collections::HashSet::new(),
                                    };

                                    // Apply property filters for relationship
                                    if r.properties.is_some()
                                        && let Some(r_var) = &r.variable
                                        && let Some(prop_filter) =
                                            planner.properties_to_expr(r_var, &r.properties)
                                    {
                                        plan = LogicalPlan::Filter {
                                            input: Box::new(plan),
                                            predicate: prop_filter,
                                            optional_variables: std::collections::HashSet::new(),
                                        };
                                    }

                                    // Apply property filters for target node if it was new
                                    if !target_variable.is_empty() {
                                        if let Some(prop_filter) = planner.properties_to_expr(
                                            &target_variable,
                                            &n_target.properties,
                                        ) {
                                            plan = LogicalPlan::Filter {
                                                input: Box::new(plan),
                                                predicate: prop_filter,
                                                optional_variables: std::collections::HashSet::new(
                                                ),
                                            };
                                        }
                                        vars_in_scope.push(target_variable.clone());
                                    }

                                    if let Some(sv) = &r.variable {
                                        vars_in_scope.push(sv.clone());
                                    }
                                    i += 2;
                                } else {
                                    break;
                                }
                            } else {
                                break;
                            }
                        }
                    }
                    _ => return Err(anyhow!("Pattern must start with a node")),
                }
            }

            // Execute the plan
            // We need to inject the current row into the execution if the plan starts with Empty?
            // Actually, if we use CrossJoin with existing row, we can simulate the context.
            // But Scan operators ignore input row usually?
            // Our LogicalPlan execution model passes the result of previous step as input to next.
            // Scan ignores input usually.
            // But if we bind variables from `row`, we need to ensure they are available.

            // The simple Scan operator returns all nodes.
            // If we want to filter by existing `row` values, we need to inject `row` into the stream?
            // Or we treat `row` as initial state.

            // Since `execute` takes a plan, and `Scan` generates new rows...
            // If `plan` is `CrossJoin(Empty, Scan)`, and `Empty` produces 1 row (empty map), then result is Scan rows.
            // But we want to filter `Scan` rows based on `row` values if variable matches.

            // Actually, `evaluate_expr` uses `row`.
            // If we have `LogicalPlan::Filter`, it uses the row passed to it.
            // But `Scan` produces NEW rows.
            // We need to carry over the `row` context.

            // Solution: Use `LogicalPlan::Project` or similar to inject initial context?
            // Or simply execute the plan, which returns all matches in DB, and then filter/join with `row`.

            // BUT: execute_subplan handles the flow.
            // If we pass `row` as initial context? No, `execute` starts from scratch.

            // Strategy:
            // 1. Execute the plan to find ALL matches in the DB that satisfy the pattern.
            // 2. Filter the results to keep only those that match the BOUND variables in `row`.
        }

        let db_matches = self
            .execute_subplan(plan, prop_manager, params, ctx)
            .await?;

        let mut final_matches = Vec::new();
        for db_match in db_matches {
            // Check consistency with input row
            let mut consistent = true;
            for (key, val) in row {
                if let Some(db_val) = db_match.get(key)
                    && db_val != val
                {
                    // Mismatch? Check if they represent the same VID
                    let vid1 = Self::vid_from_value(val);
                    let vid2 = Self::vid_from_value(db_val);
                    if let (Ok(v1), Ok(v2)) = (vid1, vid2) {
                        if v1 != v2 {
                            consistent = false;
                            break;
                        }
                    } else {
                        consistent = false;
                        break;
                    }
                }
            }

            if consistent {
                // Merge db_match into row
                let mut merged = row.clone();
                merged.extend(db_match);
                final_matches.push(merged);
            }
        }

        Ok(final_matches)
    }
}
