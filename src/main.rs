use std::io;
use std::process::ExitCode;

use clap::Parser;

use arrow::csv::reader::Format;

impl From<&Args> for Format {
    fn from(a: &Args) -> Self {
        let mut f = Format::default();

        f = Args::with_header(a.has_header, f);
        f = Args::with_delim(a.delimiter, f);
        if let Some(esc) = a.escape_char {
            f = Args::with_escape(esc, f);
        }
        if let Some(nl) = a.newline_char {
            f = Args::with_term(nl, f);
        }
        if let Some(cmt) = a.comment_char {
            f = Args::with_comm(cmt, f);
        }
        f = Args::with_same_column_count(a.same_column_count, f);
        f = Args::with_quote(a.quote, f);

        f
    }
}

impl Args {
    fn with_header(val: bool, f: Format) -> Format {
        f.with_header(val)
    }
    fn with_delim(val: u8, f: Format) -> Format {
        f.with_delimiter(val)
    }
    fn with_escape(val: u8, f: Format) -> Format {
        f.with_escape(val)
    }
    fn with_quote(val: u8, f: Format) -> Format {
        f.with_quote(val)
    }
    fn with_term(val: u8, f: Format) -> Format {
        f.with_terminator(val)
    }
    fn with_comm(val: u8, f: Format) -> Format {
        f.with_comment(val)
    }
    fn with_same_column_count(val: bool, f: Format) -> Format {
        f.with_truncated_rows(!val)
    }
}

impl Args {
    fn to_fsync(&self) -> impl Fn(&mut std::fs::File) -> Result<(), io::Error> {
        match self.enable_fsync {
            true => rs_csv2parquet::fsync_all,
            false => rs_csv2parquet::fsync_nop,
        }
    }
}

fn sub() -> Result<(), io::Error> {
    let args = Args::parse();

    let fsync_cb = args.to_fsync();
    let mx_rec: Option<usize> = args.infer_schema_max_record_count;
    let icsv: &str = &args.input_csv_filename;
    let opq: &str = &args.output_parquet_filename;

    rs_csv2parquet::filename2batch2parquet((&args).into(), mx_rec, icsv, opq, fsync_cb)
}

#[derive(Clone, Parser, Debug)]
struct Args {
    #[arg(short, long)]
    input_csv_filename: String,

    #[arg(short, long)]
    output_parquet_filename: String,

    #[arg(short, long, default_value_t = false)]
    enable_fsync: bool,

    #[arg(short, long)]
    infer_schema_max_record_count: Option<usize>,

    #[arg(short, long, default_value_t = true)]
    has_header: bool,

    #[arg(short, long, default_value_t = b',')]
    delimiter: u8,

    #[arg(short, long, default_value_t = true)]
    same_column_count: bool,

    #[arg(short, long)]
    escape_char: Option<u8>,

    #[arg(short, long)]
    newline_char: Option<u8>,

    #[arg(short, long)]
    comment_char: Option<u8>,

    #[arg(short, long, default_value_t = b'"')]
    quote: u8,
}

fn main() -> ExitCode {
    if let Err(e) = sub() {
        eprintln!("{e}");
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}
