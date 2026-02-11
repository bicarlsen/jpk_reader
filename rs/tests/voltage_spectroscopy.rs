use jpk_reader::voltage_spectroscopy::v2_0 as jpk;
use std::path::PathBuf;

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
fn voltage_spectroscopy_load_dir() {
    let data_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(COLLECTION_DIR);
    let reader = jpk::DirReader::new(data_path);
    let df = reader.load_data_all().unwrap();
    eprintln!("{:?}", df.head(Some(10)));
}
