use std::io;
use std::path::Path;

use io::Read;
use io::Seek;
use io::Write;

use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;

use arrow::csv::reader::Format;
use arrow::record_batch::RecordBatch;

use arrow::csv::reader::ReaderBuilder;

use parquet::arrow::ArrowWriter;

pub fn batch2parquet<I, W>(batch: I, mut wtr: ArrowWriter<W>) -> Result<W, io::Error>
where
    W: Write + Send,
    I: Iterator<Item = Result<RecordBatch, io::Error>>,
{
    for rb in batch {
        let b: RecordBatch = rb?;
        wtr.write(&b)?;
    }
    wtr.into_inner().map_err(io::Error::other)
}

pub fn batch2parquet_file<I, P, F>(
    batch: I,
    sch: SchemaRef,
    out_pq_filename: P,
    fsync: F,
) -> Result<(), io::Error>
where
    P: AsRef<Path>,
    I: Iterator<Item = Result<RecordBatch, io::Error>>,
    F: Fn(&mut std::fs::File) -> Result<(), io::Error>,
{
    let f = std::fs::File::create(out_pq_filename)?;
    let wtr = ArrowWriter::try_new(f, sch, None)?;
    let mut f = batch2parquet(batch, wtr)?;
    f.flush()?;
    fsync(&mut f)
}

pub fn fmt2schema<R>(f: &Format, rdr: R, max_records: Option<usize>) -> Result<Schema, io::Error>
where
    R: Read,
{
    f.infer_schema(rdr, max_records)
        .map(|t| t.0)
        .map_err(io::Error::other)
}

pub fn schema2batch<R>(
    sch: SchemaRef,
    f: Format,
    rdr: R,
) -> Result<impl Iterator<Item = Result<RecordBatch, io::Error>>, io::Error>
where
    R: Read,
{
    ReaderBuilder::new(sch)
        .with_format(f)
        .build(rdr)
        .map_err(io::Error::other)
        .map(|i| i.map(|r| r.map_err(io::Error::other)))
}

pub fn filename2batch2parquet<P, F>(
    fm: Format,
    max_records: Option<usize>,
    input_csv_filename: P,
    output_parquet_filename: P,
    fsync: F,
) -> Result<(), io::Error>
where
    P: AsRef<Path>,
    F: Fn(&mut std::fs::File) -> Result<(), io::Error>,
{
    let mut input_csv = std::fs::File::open(input_csv_filename)?;
    let s: Schema = fmt2schema(&fm, &input_csv, max_records)?;
    input_csv.rewind()?;
    let sr: SchemaRef = s.into();
    let batch = schema2batch(sr.clone(), fm, input_csv)?;

    batch2parquet_file(batch, sr, output_parquet_filename, fsync)
}

pub fn fsync_nop(_: &mut std::fs::File) -> Result<(), io::Error> {
    Ok(())
}
pub fn fsync_all(f: &mut std::fs::File) -> Result<(), io::Error> {
    f.sync_all()
}
