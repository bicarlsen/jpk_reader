//! Real time oscilliscope data reader.
//! (`.out`)

use polars::{
    error::{ErrString, PolarsError},
    prelude::*,
};
use std::{
    fs::File,
    io::{self, BufRead, BufReader, Seek},
    path::Path,
};

pub fn load_data(uri: impl AsRef<Path>) -> PolarsResult<LazyFrame> {
    const COMMENT_PREFIX: &str = "#";
    const COLUMNS_LINE_KEY: &str = "# columns: ";
    const FIELD_SEPARATOR: char = ' ';
    const FIELD_SEPARATOR_BYTE: u8 = b' ';

    let uri: &Path = uri.as_ref();
    let mut file = File::open(uri)?;
    let reader = BufReader::new(&file);
    let mut labels = None;
    for line in reader.lines() {
        let line = line?;
        if let Some(col_line) = line.strip_prefix(COLUMNS_LINE_KEY) {
            let _ = labels.insert(
                col_line
                    .split_ascii_whitespace()
                    .map(|col| col.to_string())
                    .collect::<Vec<_>>(),
            );
            break;
        }
    }

    let Some(labels) = labels else {
        let uri_str = uri.as_os_str().to_str().unwrap();
        let reader = LazyCsvReader::new(PlPath::new(uri_str))
            .with_comment_prefix(Some(PlSmallStr::from_str(COMMENT_PREFIX)))
            .with_separator(FIELD_SEPARATOR_BYTE);

        return Ok(reader.finish().unwrap());
    };

    let mut cols = vec![vec![]; labels.len()];
    file.seek(io::SeekFrom::Start(0))?;
    let reader = BufReader::new(&file);
    for (idx, line) in reader.lines().enumerate() {
        let line = line?;
        let line = line.trim();
        if line.len() == 0 || line.starts_with(COMMENT_PREFIX) {
            continue;
        }

        dbg!(line.split(FIELD_SEPARATOR).collect::<Vec<_>>());

        let values = line
            .split(FIELD_SEPARATOR)
            .filter(|v| v.trim() != "")
            .map(|v| {
                v.parse::<f64>().map_err(|_| {
                    eprintln!("could not parse line {idx}, element {v}");
                    PolarsError::ComputeError(ErrString::new_static("could not parse value"))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if values.len() != cols.len() {
            todo!("handle unexpected number of columns");
        }

        for (idx, value) in values.into_iter().enumerate() {
            cols[idx].push(value)
        }
    }

    let cols = std::iter::zip(labels, cols)
        .map(|(label, data)| Column::new(label.into(), data))
        .collect::<Vec<_>>();

    let df = DataFrame::new(cols).unwrap();
    Ok(df.lazy())
}

#[cfg(test)]
mod test {
    use super::*;
    use std::path::PathBuf;

    const DATA_DIR: &str = "../data/scope";
    const DATA_FILE: &str = "time-current-deflection.out";

    #[test]
    fn load_data_test() {
        let data_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join(DATA_DIR)
            .join(DATA_FILE);

        let _df = load_data(data_path).unwrap();
    }
}
