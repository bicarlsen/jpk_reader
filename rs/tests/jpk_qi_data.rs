use jpk_reader::qi_map;
use std::{fs, path::PathBuf};

const DATA_DIR: &str = "../data/qi_data";
const DATA_FILE_LG: &str = "qi_data-2_0-lg.jpk-qi-data";
const DATA_FILE_SM: &str = "qi_data-sm.jpk-qi-data";

#[test]
fn qi_map_reader_query_data() {
    let data_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join(DATA_DIR)
        .join(DATA_FILE_LG);
    let file = fs::File::open(&data_path).unwrap();
    let archive = zip::ZipArchive::new(file).unwrap();
    let mut reader = qi_map::v2_0::Reader::new(archive).unwrap();
}

#[test]
fn qi_map_reader_query_metadata() {
    let data_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join(DATA_DIR)
        .join(DATA_FILE_LG);
    let file = fs::File::open(&data_path).unwrap();
    let archive = zip::ZipArchive::new(file).unwrap();
    let mut reader = qi_map::v2_0::Reader::new(archive).unwrap();
}

#[test]
fn qi_map_file_reader_query_data() {
    let data_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join(DATA_DIR)
        .join(DATA_FILE_LG);
    let mut reader = qi_map::v2_0::FileReader::new(data_path).unwrap();
}

#[test]
fn qi_map_file_reader_query_metadata() {
    let data_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join(DATA_DIR)
        .join(DATA_FILE_LG);
    let data_path = PathBuf::from(DATA_DIR).join(DATA_FILE_LG);
    let mut reader = qi_map::v2_0::FileReader::new(data_path).unwrap();
}

pub mod tmp {
    use std::{fs, io, path::Path, sync::Arc};

    pub fn open(path: impl AsRef<Path>) -> Result<(), Error> {
        let file = fs::File::open(path)?;
        let mut archive = zip::ZipArchive::new(Arc::new(file))?;

        // for i in 0..archive.len() {
        //     let file = archive.by_index(i).unwrap();
        //     println!("{i}: {:?}", file.enclosed_name());
        // }
        let mut file = archive
            .by_path("index/0/segments/0/channels/smoothedMeasuredHeight.dat")
            .unwrap();
        let outpath = match file.enclosed_name() {
            Some(path) => path,
            None => panic!(),
        };

        if let Some(p) = outpath.parent() {
            if !p.exists() {
                fs::create_dir_all(p).unwrap();
            }
        }
        let mut outfile = fs::File::create(&outpath).unwrap();
        io::copy(&mut file, &mut outfile).unwrap();
        Ok(())
    }

    #[derive(Debug, derive_more::From)]
    pub enum Error {
        Read(io::Error),
        Zip(zip::result::ZipError),
    }
}
