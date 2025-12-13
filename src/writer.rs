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

        // Move out of buffer (no clones).
        let mut id_ban_builder: Vec<String> = Vec::with_capacity(len);
        let mut id_parcelle_builder: Vec<Option<String>> = Vec::with_capacity(len);
        let mut match_type_builder: Vec<String> = Vec::with_capacity(len);
        let mut distance_m_builder: Vec<f32> = Vec::with_capacity(len);
        let mut confidence_builder: Vec<u32> = Vec::with_capacity(len);

        for m in self.batch_buffer.drain(..) {
            id_ban_builder.push(m.id_ban);
            id_parcelle_builder.push(m.id_parcelle);
            match_type_builder.push(m.match_type.to_string());
            distance_m_builder.push(m.distance_m);
            confidence_builder.push(m.confidence);
        }

        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(StringArray::from(id_ban_builder)),
                Arc::new(StringArray::from(id_parcelle_builder)),
                Arc::new(StringArray::from(match_type_builder)),
                Arc::new(Float32Array::from(distance_m_builder)),
                Arc::new(UInt32Array::from(confidence_builder)),
            ],
        )?;

        self.writer.write(&batch)?;
        Ok(())
    }

    pub fn close(mut self) -> Result<()> {
        self.flush_buffer()?;
        self.writer.close()?;
        Ok(())
    }
}
