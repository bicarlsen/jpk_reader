use jpk_reader::voltage_spectroscopy::{VOLTAGE_SPECTROSCOPY_FILE_EXT, v2_0 as jpk};
use std::{fs, path::PathBuf};

const DATA_FILE: &str = "../data/voltage-spectroscopy/voltage-spectroscopy.jpk-voltage-ramp";
const COLLECTION_DIR: &str = "../data/voltage-spectroscopy/collection";

#[test]
fn voltage_spectroscopy_load_data() {
    let data_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(DATA_FILE);
    let mut reader = jpk::FileReader::new(data_path).unwrap();
    let df = reader.load_data_all().unwrap();
    eprintln!("{:?}", df.head(Some(10)));
}

#[test]
fn voltage_spectroscopy_load_files() {
    let data_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(COLLECTION_DIR);
    let dir_walker = fs::read_dir(&data_path).unwrap();
    let paths = dir_walker
        .into_iter()
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let path = entry.path();
            let ext = path.extension()?.to_str()?;
            (path.is_file() && ext == VOLTAGE_SPECTROSCOPY_FILE_EXT).then_some(path)
        })
        .collect::<Vec<_>>();

    let df = jpk::load_files(&paths).unwrap();
    eprintln!("{:?}", df.head(Some(10)));
}
