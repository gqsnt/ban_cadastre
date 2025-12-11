use crate::structures::MatchOutput;
use anyhow::{Context, Result};
use arrow::array::{Float32Array, StringArray, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

pub struct MatchWriter {
    writer: ArrowWriter<File>,
    batch_buffer: Vec<MatchOutput>,
    batch_size: usize,
    schema: Arc<Schema>,
}

impl MatchWriter {
    pub fn new(path: &Path, batch_size: usize) -> Result<Self> {
        let file = File::create(path)
            .with_context(|| format!("Failed to create output file: {:?}", path))?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id_ban", DataType::Utf8, false),
            Field::new("id_parcelle", DataType::Utf8, true),
            Field::new("match_type", DataType::Utf8, false),
            Field::new("distance_m", DataType::Float32, false),
            Field::new("confidence", DataType::UInt32, false),
        ]));

        let props = WriterProperties::builder().build();
        let writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
            .context("Failed to create ArrowWriter")?;

        Ok(Self {
            writer,
            batch_buffer: Vec::with_capacity(batch_size),
            batch_size,
            schema,
        })
    }

    pub fn write(&mut self, match_output: MatchOutput) -> Result<()> {
        self.batch_buffer.push(match_output);
        if self.batch_buffer.len() >= self.batch_size {
            self.flush_buffer()?;
        }
        Ok(())
    }

    pub fn flush_buffer(&mut self) -> Result<()> {
        if self.batch_buffer.is_empty() {
            return Ok(());
        }

        let len = self.batch_buffer.len();
        let mut id_ban_builder = Vec::with_capacity(len);
        let mut id_parcelle_builder = Vec::with_capacity(len);
        let mut match_type_builder = Vec::with_capacity(len);
        let mut distance_m_builder = Vec::with_capacity(len);
        let mut confidence_builder = Vec::with_capacity(len);

        for m in &self.batch_buffer {
            id_ban_builder.push(m.id_ban.clone());
            id_parcelle_builder.push(m.id_parcelle.clone());
            match_type_builder.push(m.match_type.to_string());
            distance_m_builder.push(m.distance_m);
            confidence_builder.push(m.confidence);
        }

        let id_ban_array = StringArray::from(id_ban_builder);
        let id_parcelle_array = StringArray::from(id_parcelle_builder); // Handle Option automatically? No, StringArray::from(Vec<Option<String>>)
        let match_type_array = StringArray::from(match_type_builder);
        let distance_m_array = Float32Array::from(distance_m_builder);
        let confidence_array = UInt32Array::from(confidence_builder);

        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(id_ban_array),
                Arc::new(id_parcelle_array),
                Arc::new(match_type_array),
                Arc::new(distance_m_array),
                Arc::new(confidence_array),
            ],
        )?;

        self.writer.write(&batch)?;
        self.batch_buffer.clear();

        Ok(())
    }

    pub fn close(mut self) -> Result<()> {
        self.flush_buffer()?;
        self.writer.close()?;
        Ok(())
    }
}
