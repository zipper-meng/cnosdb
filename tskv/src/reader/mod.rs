#![allow(clippy::arc_with_non_send_sync)]

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow_schema::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::PhysicalExpr;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
pub use iterator::QueryOption;
use models::field_value::DataType;
use models::predicate::domain::{TimeRange, TimeRanges};
use models::schema::tskv_table_schema::{PhysicalCType, TskvTableSchema};
use models::schema::TIME_FIELD_NAME;
use models::ColumnId;
use parking_lot::RwLock;

use self::utils::{CombinedRecordBatchStream, TimeRangeProvider};
use crate::mem_cache::series_data::SeriesData;
use crate::tsm::chunk::Chunk;
use crate::tsm::reader::TsmReader;
use crate::{TskvError, TskvResult};

mod batch_builder;
mod chunk;
mod column_group;
pub mod display;
pub mod filter;
mod function_register;

mod iterator;
mod memcache_reader;
mod merge;
mod metrics;
mod page;
mod paralle_merge;
mod partitioned_stream;
mod schema_alignmenter;
mod series;
mod trace;
mod utils;
mod visitor;

pub mod plan;
pub mod query_executor;
pub mod serialize;
pub mod sort_merge;
pub mod table_scan;
pub mod tag_scan;
pub mod test_util;

pub type PredicateRef = Arc<Predicate>;

#[derive(Debug)]
pub struct Predicate {
    expr: Option<Arc<dyn PhysicalExpr>>,
    schema: SchemaRef,
    limit: Option<usize>,
}

impl Predicate {
    pub fn new(
        expr: Option<Arc<dyn PhysicalExpr>>,
        schema: SchemaRef,
        limit: Option<usize>,
    ) -> Self {
        Self {
            expr,
            schema,
            limit,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn expr(&self) -> Option<Arc<dyn PhysicalExpr>> {
        self.expr.clone()
    }

    pub fn limit(&self) -> Option<usize> {
        self.limit
    }
}

#[derive(Debug, Clone)]
pub struct Projection {
    fields: Vec<ColumnId>,
    fields_with_time: Vec<ColumnId>,
}

impl Projection {
    /// Create Projection from schema.
    /// - `fields` will be the same as all columns' ids of the argument `schema`.
    /// - `fields_with_time` will be the same as schema's field_ids if `time` column is exists in schema,
    ///    otherwise, the argument `time_column_id` will be added to the end.
    fn from_schema(schema: &TskvTableSchema, time_column_id: ColumnId) -> Self {
        let column_ids = schema.columns().iter().map(|f| f.id).collect::<Vec<_>>();

        let fields_with_time = if schema.column_index(TIME_FIELD_NAME).is_none() {
            let mut new_column_ids = Vec::with_capacity(column_ids.len() + 1);
            new_column_ids.copy_from_slice(&column_ids);
            new_column_ids.push(time_column_id);
            new_column_ids
        } else {
            column_ids.clone()
        };

        Self {
            fields: column_ids,
            fields_with_time,
        }
    }

    pub fn fields(&self) -> &[ColumnId] {
        &self.fields
    }

    pub fn fields_with_time(&self) -> &[ColumnId] {
        &self.fields_with_time
    }
}

pub type SendableTskvRecordBatchStream =
    Pin<Box<dyn Stream<Item = TskvResult<RecordBatch>> + Send>>;

pub type SendableSchemableTskvRecordBatchStream =
    Pin<Box<dyn SchemableTskvRecordBatchStream<Item = TskvResult<RecordBatch>> + Send>>;
pub trait SchemableTskvRecordBatchStream: Stream<Item = TskvResult<RecordBatch>> {
    fn schema(&self) -> SchemaRef;
}

pub struct EmptySchemableTskvRecordBatchStream {
    schema: SchemaRef,
}
impl EmptySchemableTskvRecordBatchStream {
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}
impl SchemableTskvRecordBatchStream for EmptySchemableTskvRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
impl Stream for EmptySchemableTskvRecordBatchStream {
    type Item = TskvResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

pub type BatchReaderRef = Arc<dyn BatchReader>;
pub trait BatchReader {
    fn process(&self) -> TskvResult<SendableSchemableTskvRecordBatchStream>;
    fn fmt_as(&self, f: &mut fmt::Formatter) -> fmt::Result;
    fn children(&self) -> Vec<BatchReaderRef>;
}

pub struct CombinedBatchReader {
    readers: Vec<BatchReaderRef>,
}

impl CombinedBatchReader {
    pub fn new(readers: Vec<BatchReaderRef>) -> Self {
        Self { readers }
    }
}

impl BatchReader for CombinedBatchReader {
    fn process(&self) -> TskvResult<SendableSchemableTskvRecordBatchStream> {
        let streams = self
            .readers
            .iter()
            // CombinedRecordBatchStream 是倒序遍历，所以此处 rev 反转一下
            .rev()
            .map(|e| e.process())
            .collect::<TskvResult<Vec<_>>>()?;

        if let Some(s) = streams.first() {
            return Ok(Box::pin(CombinedRecordBatchStream::try_new(
                s.schema(),
                streams,
                &ExecutionPlanMetricsSet::new(),
            )?));
        }

        // 如果没有 stream，则返回一个空的 stream
        Ok(Box::pin(SchemableMemoryBatchReaderStream::new(
            Arc::new(Schema::empty()),
            vec![],
        )))
    }

    fn fmt_as(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CombinedBatchReader: size={}", self.readers.len())
    }

    fn children(&self) -> Vec<BatchReaderRef> {
        self.readers.clone()
    }
}

pub struct MemoryBatchReader {
    schema: SchemaRef,
    data: Vec<RecordBatch>,
}

impl MemoryBatchReader {
    pub fn new(schema: SchemaRef, data: Vec<RecordBatch>) -> Self {
        Self { schema, data }
    }
}

impl BatchReader for MemoryBatchReader {
    fn process(&self) -> TskvResult<SendableSchemableTskvRecordBatchStream> {
        let stream = SchemableMemoryBatchReaderStream::new(self.schema.clone(), self.data.clone());

        Ok(Box::pin(stream))
    }

    fn fmt_as(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MemoryBatchReader:")
    }

    fn children(&self) -> Vec<BatchReaderRef> {
        vec![]
    }
}

pub struct SchemableMemoryBatchReaderStream {
    schema: SchemaRef,
    stream: BoxStream<'static, RecordBatch>,
}

impl SchemableMemoryBatchReaderStream {
    pub fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        Self {
            schema,
            stream: Box::pin(futures::stream::iter(batches)),
        }
    }

    pub fn new_partitions(
        schema: SchemaRef,
        batches: Vec<Vec<RecordBatch>>,
    ) -> Vec<SendableSchemableTskvRecordBatchStream> {
        batches
            .into_iter()
            .map(|b| {
                Box::pin(SchemableMemoryBatchReaderStream::new(schema.clone(), b))
                    as SendableSchemableTskvRecordBatchStream
            })
            .collect::<Vec<_>>()
    }
}

impl SchemableTskvRecordBatchStream for SchemableMemoryBatchReaderStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for SchemableMemoryBatchReaderStream {
    type Item = TskvResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(self.stream.poll_next_unpin(cx)) {
            None => Poll::Ready(None),
            Some(r) => Poll::Ready(Some(Ok(r))),
        }
    }
}

#[derive(Clone)]
pub enum DataReference {
    Chunk(Arc<Chunk>, Arc<TsmReader>),
    Memcache(Arc<RwLock<SeriesData>>, Arc<TimeRanges>),
}

impl DataReference {
    pub fn time_range(&self) -> TimeRange {
        match self {
            DataReference::Chunk(chunk, _) => *chunk.time_range(),
            DataReference::Memcache(_, trs) => trs.max_time_range(),
        }
    }
}

impl TimeRangeProvider for DataReference {
    fn time_range(&self) -> TimeRange {
        self.time_range()
    }
}

/// A wrapper to customize partitioned file display
#[derive(Debug)]
pub struct ProjectSchemaDisplay<'a>(pub &'a SchemaRef);

impl<'a> fmt::Display for ProjectSchemaDisplay<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let parts: Vec<_> = self
            .0
            .fields()
            .iter()
            .map(|x| x.name().to_owned())
            .collect::<Vec<String>>();
        write!(f, "[{}]", parts.join(", "))
    }
}

#[async_trait::async_trait]
pub trait Cursor: Send + Sync {
    fn name(&self) -> &String;
    fn is_field(&self) -> bool {
        matches!(self.column_type(), PhysicalCType::Field(_))
    }
    fn column_type(&self) -> PhysicalCType;
    async fn next(&mut self) -> TskvResult<Option<DataType>, TskvError>;
}
