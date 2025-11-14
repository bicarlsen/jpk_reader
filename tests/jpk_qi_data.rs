use jpk_reader::qi_map::{self, QIMapReader};
use std::{fs, path::PathBuf};

const DATA_DIR: &str = "tests/data/qi_data";
const DATA_FILE: &str = "qi_data.jpk-qi-data";

#[tracing_test::traced_test]
#[test]
fn qi_map() {
    let data_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join(DATA_DIR)
        .join(DATA_FILE);

    let file = fs::File::open(&data_path).unwrap();
    let mut data = qi_map::Reader::new(file).unwrap();
    let m_height = data
        .get_data_index_segment_channel(0, 0, "measuredHeight")
        .unwrap();

    let sm_height = data
        .get_data_index_segment_channel(0, 0, "smoothedMeasuredHeight")
        .unwrap();

    let query = qi_map::Query {
        index: qi_map::IndexQuery::Pixel(qi_map::Pixel::new(0, 0)),
        segment: qi_map::SegmentQuery::Indices(vec![0]),
        channel: qi_map::ChannelQuery::include(vec!["measuredHeight", "smoothedMeasuredHeight"]),
    };
    let result = data.query_data(&query).unwrap();
    assert_eq!(result.len(), 2);
    let idx = qi_map::DataIndex::new(0, 0, "measuredHeight");
    let values = result.get(&idx).unwrap();
    assert_eq!(*values, m_height);

    let idx = qi_map::DataIndex::new(0, 0, "smoothedMeasuredHeight");
    let values = result.get(&idx).unwrap();
    assert_eq!(*values, sm_height);

    // long running test
    // let query = qi_map::Query::select_all();
    // let all = data.query_data(&query).unwrap();
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
