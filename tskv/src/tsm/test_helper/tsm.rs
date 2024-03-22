use std::cmp::Ordering;
use std::path::Path;
use std::sync::Arc;

use arrow_schema::TimeUnit;
use minivec::MiniVec;
use models::codec::Encoding;
use models::field_value::FieldVal;
use models::gis::data_type::GeometryType;
use models::schema::{ColumnType, PhysicalCType, TableColumn, TskvTableSchema};
use models::{ColumnId, SeriesId, SeriesKey, ValueType};

use super::{geometry, string_into_mini_vec};
use crate::tsm::writer::{Column, DataBlock, TsmWriter};
use crate::{file_utils, Error, Result};

pub async fn write_data_blocks_to_tsm(
    path: impl AsRef<Path>,
    data_blocks_iter: impl IntoIterator<Item = (SeriesId, Vec<DataBlock>)>,
) -> Result<()> {
    let tsm_seq = file_utils::get_tsm_file_id_by_path(&path)?;
    let path = path.as_ref();
    let dir = path.parent().unwrap();
    let is_delta = path.extension().is_some_and(|s| s == "delta");
    let mut tsm_writer = TsmWriter::open(&dir, tsm_seq, 0, is_delta).await?;
    for (sid, blocks) in data_blocks_iter {
        for blk in blocks {
            tsm_writer
                .write_datablock(sid, SeriesKey::default(), blk)
                .await?;
        }
    }
    tsm_writer.finish().await
}

pub async fn write_to_tsm(
    path: impl AsRef<Path>,
    data_blocks: impl Iterator<Item = (SeriesId, Vec<DataBlockSketch>)>,
) -> Result<()> {
    let tsm_seq = file_utils::get_tsm_file_id_by_path(&path)?;
    let path = path.as_ref();
    let dir = path.parent().unwrap();
    let is_delta = path.extension().is_some_and(|s| s == "delta");
    let mut tsm_writer = TsmWriter::open(&dir, tsm_seq, 0, is_delta).await?;
    for (sid, blk_sketches) in data_blocks {
        for blk_sketch in blk_sketches {
            tsm_writer
                .write_datablock(sid, SeriesKey::default(), blk_sketch.to_data_block()?)
                .await?;
        }
    }
    tsm_writer.finish().await
}

pub struct DataBlockSketch {
    pub columns: Vec<ColumnSketch>,
    pub schema: Arc<TskvTableSchema>,
}

impl DataBlockSketch {
    pub fn new(
        tenant: String,
        database: String,
        table: String,
        columns: Vec<ColumnSketch>,
    ) -> Self {
        if columns.is_empty() {
            panic!("columns is empty");
        }
        if !matches!(columns[0].col_type, ColumnType::Time(_)) {
            panic!("columns[0] must be timestamps column");
        }
        let schema_columns = columns
            .iter()
            .map(|c| TableColumn {
                id: c.id,
                name: c.name.clone(),
                column_type: c.col_type.clone(),
                encoding: Encoding::Default,
            })
            .collect();
        let schema = TskvTableSchema::new(tenant, database, table, schema_columns);

        Self {
            columns,
            schema: Arc::new(schema),
        }
    }

    pub fn to_data_block(&self) -> Result<DataBlock> {
        let ts_col_sketch = self.ts_column().expect("at least one Time column expected");
        let (ts, ts_desc) = ts_col_sketch.to_column()?;
        let mut cols = Vec::with_capacity(self.columns.len() - 1);
        let mut cols_desc = Vec::with_capacity(self.columns.len() - 1);
        for col in self.columns.iter().filter(|c| c.id != ts_col_sketch.id) {
            let (col, col_desc) = col.to_column()?;
            cols.push(col);
            cols_desc.push(col_desc);
        }
        Ok(DataBlock::new(
            self.schema.clone(),
            ts,
            ts_desc,
            cols,
            cols_desc,
        ))
    }

    fn ts_column(&self) -> Option<ColumnSketch> {
        self.columns
            .iter()
            .filter(|c| matches!(c.col_type, ColumnType::Time(_)))
            .next()
            .cloned()
    }

    fn columns(&self) -> Vec<ColumnSketch> {
        if let Some(ts_col) = self.ts_column() {
            self.columns
                .iter()
                .filter(|c| c.id != ts_col.id)
                .cloned()
                .collect()
        } else {
            self.columns.clone()
        }
    }
}

#[test]
fn test_to_data_block() {
    #[rustfmt::skip]
    let blk_sketch = DataBlockSketch::new("tn1".to_string(), "db1".to_string(), "tb1".to_string(), vec![
        ColumnSketch::new(1, "c1".to_string(), ColumnType::Time(TimeUnit::Nanosecond), 1, 10, vec![(2, 3)]),
        ColumnSketch::new(2, "c2".to_string(), ColumnType::Tag, 1, 3, vec![]),
        ColumnSketch::new(3, "c3".to_string(), ColumnType::Field(ValueType::Float), 1, 10, vec![(5, 6)]),
    ]);
    let blk = blk_sketch.to_data_block().unwrap();
    let blk_schema = blk.schema();
    #[rustfmt::skip]
    assert_eq!(blk_schema.as_ref(), &TskvTableSchema::new("tn1".to_string(), "db1".to_string(), "tb1".to_string(), vec![
        TableColumn::new(1, "c1".to_string(), ColumnType::Time(TimeUnit::Nanosecond), Encoding::Default),
        TableColumn::new(2, "c2".to_string(), ColumnType::Tag, Encoding::Default),
        TableColumn::new(3, "c3".to_string(), ColumnType::Field(ValueType::Float), Encoding::Default),
    ]));
    for col_1 in blk_sketch.ts_column() {
        let col_2 = blk.ts();
        for (i, t_1) in col_1.iter().enumerate() {
            let t_2 = col_2.get(i);
            assert_eq!(
                t_1, t_2,
                "ts data wrong, expected: {:?}, actual: {:?}",
                t_1, t_2
            );
        }
    }
    for col_1 in blk_sketch.columns() {
        let col_2 = blk
            .column(col_1.id)
            .expect(format!("column by id {} not exists in data block", col_1.id).as_str());
        for (i, v_1) in col_1.iter().enumerate() {
            let v_2 = col_2.get(i);
            assert_eq!(
                v_1, v_2,
                "column data wrong, expected: {:?}, actual: {:?}",
                v_1, v_2
            );
        }
    }
}

/// A series of data between `min` and `max`, with serial ranges of none data.
#[derive(Debug, Clone)]
pub struct ColumnSketch {
    pub id: ColumnId,
    pub name: String,
    pub col_type: ColumnType,
    pub physical_type: PhysicalCType,
    pub min: isize,
    pub max: isize,
    pub none_ranges: Vec<(isize, isize)>,
}

impl ColumnSketch {
    pub fn new(
        id: ColumnId,
        name: String,
        col_type: ColumnType,
        min: isize,
        max: isize,
        mut none_ranges: Vec<(isize, isize)>,
    ) -> Self {
        none_ranges.sort_by(|(min_1, max_1), (min_2, max_2)| match min_1.cmp(min_2) {
            Ordering::Equal => max_1.cmp(max_2),
            other => other,
        });
        let physical_type = col_type.to_physical_type();
        Self {
            id,
            name,
            col_type,
            physical_type,
            min,
            max,
            none_ranges,
        }
    }

    pub fn iter(&self) -> ColumnSketchIterator {
        ColumnSketchIterator {
            inner: self,

            cur_val: self.min,
            none_ranges_i: 0,
        }
    }

    pub fn to_column(&self) -> Result<(Column, TableColumn)> {
        self.to_column_with_encoding(Encoding::Default)
    }

    pub fn to_column_with_encoding(&self, encoding: Encoding) -> Result<(Column, TableColumn)> {
        let mut col = Column::empty(self.physical_type.clone())?;
        for val in self.iter() {
            col.push(val);
        }
        let col_desc = TableColumn {
            id: self.id,
            name: self.name.clone(),
            column_type: self.col_type.clone(),
            encoding,
        };
        Ok((col, col_desc))
    }

    pub fn check_column(&self, column: &Column, assert_msg: &str) {
        assert_eq!(&self.physical_type, column.physical_type());
        for (i, val) in self.iter().enumerate() {
            let col_val = column.get(i);
            assert_eq!(val, col_val, "{assert_msg}");
        }
    }

    fn isize_to_field_val(&self, v: isize) -> Result<FieldVal> {
        let fv = match &self.col_type {
            ColumnType::Time(_) => FieldVal::Integer(v as i64),
            ColumnType::Field(value_type) => match value_type {
                ValueType::Float => FieldVal::Float(v as f64),
                ValueType::Integer => FieldVal::Integer(v as i64),
                ValueType::Unsigned => FieldVal::Unsigned(v as u64),
                ValueType::Boolean => FieldVal::Boolean(v % 2 == 0),
                ValueType::String => {
                    let str_data = MiniVec::from(format!("str_{v}").as_bytes());
                    FieldVal::Bytes(str_data)
                }
                ValueType::Geometry(geometry) => match geometry.sub_type {
                    GeometryType::Point => {
                        let str_data = geometry::point(v, v);
                        FieldVal::Bytes(str_data)
                    }
                    GeometryType::LineString => {
                        let str_data = geometry::line_string(v, v);
                        FieldVal::Bytes(str_data)
                    }
                    GeometryType::Polygon => {
                        let str_data = geometry::polygon_square(v, v);
                        FieldVal::Bytes(str_data)
                    }
                    GeometryType::MultiPoint => {
                        let str_data = geometry::multi_point_cross(v, v);
                        FieldVal::Bytes(str_data)
                    }
                    GeometryType::MultiLineString => {
                        let str_data = geometry::multi_line_string_cross(v, v);
                        FieldVal::Bytes(str_data)
                    }
                    GeometryType::MultiPolygon => {
                        let str_data = geometry::multi_polygon_squares_vertical(v, v);
                        FieldVal::Bytes(str_data)
                    }
                    GeometryType::GeometryCollection => {
                        let str_data = geometry::geometry_collection_ok(v, v);
                        FieldVal::Bytes(str_data)
                    }
                },
                other => {
                    return Err(Error::UnsupportedDataType {
                        dt: other.to_sql_type_str().to_string(),
                    })
                }
            },
            ColumnType::Tag => FieldVal::Bytes(string_into_mini_vec(format!("{}_{v}", self.name))),
            other => {
                return Err(Error::Unimplemented {
                    msg: format!("generate {} column not implemented", other.to_string()),
                });
            }
        };
        Ok(fv)
    }
}

pub struct ColumnSketchIterator<'a> {
    inner: &'a ColumnSketch,

    cur_val: isize,
    none_ranges_i: usize,
}

impl<'a> Iterator for ColumnSketchIterator<'a> {
    type Item = Option<FieldVal>;

    fn next(&mut self) -> Option<Self::Item> {
        let v = self.cur_val;
        if v > self.inner.max {
            return None;
        }
        self.cur_val += 1;

        loop {
            if self.none_ranges_i >= self.inner.none_ranges.len() {
                break;
            }
            let (nr_min, nr_max) = self.inner.none_ranges[self.none_ranges_i];
            if v < nr_min {
                break;
            } else if v >= nr_min && v <= nr_max {
                return Some(None);
            } else {
                self.none_ranges_i += 1;
                continue;
            }
        }
        if v > self.inner.max {
            return None;
        }
        Some(Some(self.inner.isize_to_field_val(v).unwrap()))
    }
}
