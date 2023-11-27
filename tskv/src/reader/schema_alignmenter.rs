use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::{FieldRef, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow_array::{new_null_array, RecordBatch};
use futures::{Stream, StreamExt};

use super::{
    BatchReader, BatchReaderRef, SchemableTskvRecordBatchStream,
    SendableSchemableTskvRecordBatchStream,
};
use crate::Result;

/// 对数据模式进行调整和对齐，使其与预期完整和一致
/// 输入schema与输出schema含有同名字段但类型不一致时，不做检查，使用输入schema的字段数据类型
pub struct SchemaAlignmenter {
    input: BatchReaderRef,
    schema: SchemaRef,
}

impl SchemaAlignmenter {
    pub fn new(input: BatchReaderRef, schema: SchemaRef) -> Self {
        Self { input, schema }
    }
}

impl BatchReader for SchemaAlignmenter {
    fn process(&self) -> Result<SendableSchemableTskvRecordBatchStream> {
        let input = self.input.process()?;
        let input_schema = input.schema();
        if let Some(schema_mapping) = build_schema_mapping(&self.schema, &input_schema) {
            let output_schema = schema_mapping.schema(&input_schema);

            return Ok(Box::pin(SchemaAlignmenterStream {
                schema_mapping,
                schema: output_schema,
                input,
            }));
        }

        Ok(input)
    }

    fn fmt_as(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let fields = self
            .schema
            .fields()
            .iter()
            .map(|f| f.name())
            .collect::<Vec<_>>();

        write!(f, "SchemaAlignmenter: columns={:?}", fields)
    }

    fn children(&self) -> Vec<BatchReaderRef> {
        vec![self.input.clone()]
    }
}

/// Constructs a schema mapping between `output_schema` and `input_schema`.
///
/// This function iterates through each field in the `output_schema`:
/// 1. If the field exists in the `input_schema`, it adds the field to `assignments`.
/// 2. If the field does not exist in the `input_schema`, it adds the field to `assignments`
///    with a null fill.
///
/// # Arguments
///
/// * `output_schema` - A reference to the output schema.
/// * `input_schema` - A reference to the input schema.
///
/// # Returns
///
/// If the `output_schema` is equal to the `input_schema`, returns `None`.
/// Otherwise, returns a `SchemaMapping` containing the assignments between schemas.
fn build_schema_mapping(
    output_schema: &SchemaRef,
    input_schema: &SchemaRef,
) -> Option<SchemaMapping> {
    if output_schema == input_schema {
        return None;
    }

    let assignments = output_schema
        .fields()
        .iter()
        .map(|f| {
            input_schema
                .index_of(f.name())
                .map(Assignment::Location)
                .unwrap_or_else(|_| Assignment::Fill(f.clone()))
        })
        .collect();

    Some(SchemaMapping { assignments })
}

struct SchemaAlignmenterStream {
    schema_mapping: SchemaMapping,
    schema: SchemaRef,
    input: SendableSchemableTskvRecordBatchStream,
}

impl SchemableTskvRecordBatchStream for SchemaAlignmenterStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for SchemaAlignmenterStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                match reorder_and_align_schema(&self.schema_mapping, batch) {
                    Ok(batch) => Poll::Ready(Some(Ok(batch))),
                    Err(err) => Poll::Ready(Some(Err(err).map_err(Into::into))),
                }
            }
            other => other,
        }
    }
}

/// Reorders and aligns the schema according to the provided schema mapping.
///
/// This function takes a `SchemaMapping` and a `RecordBatch`, reorders the fields and columns
/// based on the assignments provided in the `SchemaMapping`, and produces a new `RecordBatch`.
///
/// # Arguments
///
/// * `schema_mapping` - A `SchemaMapping` specifying the desired field arrangement.
/// * `batch` - A `RecordBatch` containing the data to be reordered and aligned.
///
/// # Returns
///
/// A `Result` containing a new `RecordBatch` with reordered fields and columns according to the
/// provided `SchemaMapping`. If successful, it returns `Ok(RecordBatch)`, otherwise an `ArrowError`.
///
/// # Safety
///
/// - This function assumes that the indices provided in the `Location` assignments of the
///   `schema_mapping` are valid indices within the `input_fields` and `input_columns`.
///   It uses `unsafe` code to access these indices without bounds checking.
fn reorder_and_align_schema(
    schema_mapping: &SchemaMapping,
    batch: RecordBatch,
) -> Result<RecordBatch, ArrowError> {
    let mut fields = vec![];
    let mut columns = vec![];

    let input_schema = batch.schema();
    let input_fields = input_schema.fields();
    let input_columns = batch.columns();
    for assign in &schema_mapping.assignments {
        match assign {
            Assignment::Location(idx) => {
                // 不做检查，假定 schema_mapping 中的 Location(idx) 一定是合法的
                fields.push(input_fields[*idx].clone());
                columns.push(input_columns[*idx].clone());
            }
            Assignment::Fill(f) => {
                fields.push(f.clone());
                columns.push(new_null_array(f.data_type(), batch.num_rows()));
            }
        }
    }

    let schema = Arc::new(Schema::new(fields));

    RecordBatch::try_new(schema, columns)
}

/// `SchemaMapping`结构体用于表示输入和输出schema之间的映射关系。
///
/// `assignments`字段是一个`Assignment`类型的数组，数组的下标代表输出schema的字段下标，
/// 其元素表示了是否需要用null填充或者是需要从输入schema的哪个字段获取数据。
///
/// 在给定输出schema `[time, a, b, c]` 和输入schema `[time, c, b]` 的情况下，
/// `SchemaMapping`的示例可能为 `[Location(0), Fill(a), Location(2), Location(1)]`。
///
/// - `Location(n)`: 表示从输入schema中获取数据的位置，其中`n`是输入schema字段的下标。
/// - `Fill(x)`: 表示需要使用 null 或其他指定的方式填充输出schema中的字段，其中`x`是输出schema对应的字段。
struct SchemaMapping {
    assignments: Vec<Assignment>,
}

impl SchemaMapping {
    fn schema(&self, input_schema: &SchemaRef) -> SchemaRef {
        let input_fields = input_schema.fields();
        let fields = self
            .assignments
            .iter()
            .map(|assign| match assign {
                Assignment::Location(idx) => input_fields[*idx].clone(),
                Assignment::Fill(f) => f.clone(),
            })
            .collect::<Vec<_>>();

        Arc::new(Schema::new(fields))
    }
}

/// 枚举 `Assignment` 表示 `SchemaMapping` 中的每个字段的赋值方式。
///
/// `Assignment` 可以是两种类型之一：
/// - `Location(usize)`: 表示需要从输入schema中的特定位置获取数据，`usize`代表字段的索引位置。
/// - `Fill(FieldRef)`: 表示需要使用 null 或其他指定方式填充输出schema中的字段，`FieldRef`是字段的引用。
enum Assignment {
    /// 表示字段的索引位置
    Location(usize),
    /// 表示需要用 null 填充的字段
    Fill(FieldRef),
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray, UInt64Array};
    use datafusion::assert_batches_eq;
    use futures::TryStreamExt;

    use super::SchemaAlignmenter;
    use crate::reader::{BatchReader, MemoryBatchReader};

    fn input_record_batchs() -> Vec<RecordBatch> {
        let batch = RecordBatch::try_new(
            input_schema(),
            vec![
                Arc::new(Int64Array::from(vec![-1, 2, 4, 18, 8])),
                Arc::new(StringArray::from(vec![
                    Some("z"),
                    Some("y"),
                    Some("x"),
                    Some("w"),
                    None,
                ])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 4.0, 18.0, 8.0])),
                Arc::new(UInt64Array::from(vec![1, 2, 4, 18, 8])),
            ],
        )
        .expect("create record batch");
        vec![batch]
    }

    fn input_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, true),
            Field::new("c3", DataType::Utf8, true),
            Field::new("c2", DataType::Float64, true),
            Field::new("c1", DataType::UInt64, true),
        ]))
    }

    fn output_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, true),
            Field::new("c1", DataType::UInt64, true),
            Field::new("c2", DataType::Float64, true),
            Field::new("c3", DataType::Utf8, true),
            Field::new("c4", DataType::Boolean, true),
        ]))
    }

    fn missing_column_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, true),
            Field::new("c1", DataType::UInt64, true),
        ]))
    }

    fn column_data_type_mismatch_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, true),
            Field::new("c1", DataType::Utf8, true),
        ]))
    }

    #[tokio::test]
    async fn test() {
        let reader = Arc::new(MemoryBatchReader::new(
            input_schema(),
            input_record_batchs(),
        ));

        let schema_alignmenter = SchemaAlignmenter::new(reader, output_schema());

        let stream = schema_alignmenter.process().expect("schema_alignmenter");

        let result = stream.try_collect::<Vec<_>>().await.unwrap();

        let expected = [
            "+------+----+------+----+----+",
            "| time | c1 | c2   | c3 | c4 |",
            "+------+----+------+----+----+",
            "| -1   | 1  | 1.0  | z  |    |",
            "| 2    | 2  | 2.0  | y  |    |",
            "| 4    | 4  | 4.0  | x  |    |",
            "| 18   | 18 | 18.0 | w  |    |",
            "| 8    | 8  | 8.0  |    |    |",
            "+------+----+------+----+----+",
        ];

        assert_batches_eq!(expected, &result);
    }

    #[tokio::test]
    async fn test_missing_column() {
        let reader = Arc::new(MemoryBatchReader::new(
            input_schema(),
            input_record_batchs(),
        ));

        let schema_alignmenter = SchemaAlignmenter::new(reader, missing_column_schema());

        let stream = schema_alignmenter.process().expect("schema_alignmenter");

        let result = stream.try_collect::<Vec<_>>().await.unwrap();

        let expected = [
            "+------+----+",
            "| time | c1 |",
            "+------+----+",
            "| -1   | 1  |",
            "| 2    | 2  |",
            "| 4    | 4  |",
            "| 18   | 18 |",
            "| 8    | 8  |",
            "+------+----+",
        ];

        assert_batches_eq!(expected, &result);
    }

    #[tokio::test]
    async fn test_column_data_type_mismatch() {
        let reader = Arc::new(MemoryBatchReader::new(
            input_schema(),
            input_record_batchs(),
        ));
        // c1 字段的数据类型 输出schema与输出schema不一致 不做检查 使用输出schema的字段数据类型
        let schema_alignmenter = SchemaAlignmenter::new(reader, column_data_type_mismatch_schema());

        let stream = schema_alignmenter.process().expect("schema_alignmenter");

        let result = stream.try_collect::<Vec<_>>().await.unwrap();

        let expected = [
            "+------+----+",
            "| time | c1 |",
            "+------+----+",
            "| -1   | 1  |",
            "| 2    | 2  |",
            "| 4    | 4  |",
            "| 18   | 18 |",
            "| 8    | 8  |",
            "+------+----+",
        ];

        assert_batches_eq!(expected, &result);
    }
}
