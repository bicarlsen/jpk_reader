use crate::properties::Properties;
use std::{cmp, fmt, io, path::PathBuf};

mod v2_0;

type Value = f64;
type IndexId = usize;
type SegmentId = u8;
type ChannelId = String;

const PROPERTIES_FILE_PATH: &str = "header.properties";
const PROPERTIES_FILE_FORMAT_VERSION_KEY: &str = "file-format-version";

pub trait QIMapReader {
    type Error: fmt::Debug;

    /// Get the data of a specific `(index, segment, channel)`.
    fn get_data_index_segment_channel(
        &mut self,
        index: IndexId,
        segment: SegmentId,
        channel: impl fmt::Display,
    ) -> Result<Vec<Value>, Self::Error>;

    /// Query data.
    fn query_data(&mut self, query: &Query) -> Result<Data, QueryError>;
}

pub struct Data {
    indices: Vec<DataIndex>,
    data: Vec<Vec<Value>>,
}

impl Data {
    pub fn new(indices: Vec<DataIndex>, data: Vec<Vec<Value>>) -> Result<Self, InvalidDataIndices> {
        if data.len() != indices.len() {
            return Err(InvalidDataIndices);
        }
        let mut idx_map = (0..indices.len()).collect::<Vec<_>>();
        idx_map.sort_unstable_by_key(|&idx| &indices[idx]);

        let mut data = data.into_iter().enumerate().collect::<Vec<_>>();
        let mut indices = indices.into_iter().enumerate().collect::<Vec<_>>();
        data.sort_unstable_by_key(|(idx, _)| idx_map[*idx]);
        indices.sort_unstable_by_key(|(idx, _)| idx_map[*idx]);
        let data = data.into_iter().map(|(_, value)| value).collect::<Vec<_>>();
        let indices = indices
            .into_iter()
            .map(|(_, value)| value)
            .collect::<Vec<_>>();

        Ok(Self { indices, data })
    }

    pub fn len(&self) -> usize {
        self.indices.len()
    }

    pub fn get(&self, index: &DataIndex) -> Option<&Vec<Value>> {
        let idx = self.indices.binary_search(index).ok()?;
        Some(&self.data[idx])
    }
}

/// Indices size does not match data size.
#[derive(Debug)]
pub struct InvalidDataIndices;

#[derive(Debug, PartialEq, Ord, Eq)]
pub struct DataIndex {
    pub index: IndexId,
    pub segment: SegmentId,
    pub channel: ChannelId,
}

impl DataIndex {
    pub fn new(index: IndexId, segment: SegmentId, channel: impl Into<ChannelId>) -> Self {
        Self {
            index,
            segment,
            channel: channel.into(),
        }
    }
}

impl PartialOrd for DataIndex {
    /// Order by `(index, segment, channel)`.
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        if let Some(order) = self.index.partial_cmp(&other.index) {
            if matches!(order, cmp::Ordering::Greater | cmp::Ordering::Less) {
                return Some(order);
            }
        }

        if let Some(order) = self.segment.partial_cmp(&other.segment) {
            if matches!(order, cmp::Ordering::Greater | cmp::Ordering::Less) {
                return Some(order);
            }
        }

        self.channel.partial_cmp(&other.channel)
    }
}

impl From<(IndexId, SegmentId, ChannelId)> for DataIndex {
    fn from(value: (IndexId, SegmentId, ChannelId)) -> Self {
        let (index, segment, channel) = value;
        Self {
            index,
            segment,
            channel,
        }
    }
}

pub struct Query {
    pub index: IndexQuery,
    pub segment: SegmentQuery,
    pub channel: ChannelQuery,
}

impl Query {
    pub fn select_all() -> Self {
        Self {
            index: IndexQuery::All,
            segment: SegmentQuery::All,
            channel: ChannelQuery::All,
        }
    }
}

pub enum IndexQuery {
    All,
    PixelRect(PixelRect),
    Pixel(Pixel),
}

pub struct PixelRect {
    start: Pixel,
    end: Pixel,
}

impl PixelRect {
    pub fn new(start: Pixel, end: Pixel) -> Self {
        let Pixel { x: xa, y: ya } = start;
        let Pixel { x: xb, y: yb } = end;
        let (x0, x1) = if xa < xb { (xa, xb) } else { (xb, xa) };
        let (y0, y1) = if ya < yb { (ya, yb) } else { (yb, ya) };

        let start = Pixel { x: x0, y: y0 };
        let end = Pixel { x: x1, y: y1 };
        Self { start, end }
    }

    pub fn rows(&self) -> IndexId {
        self.end.y - self.start.y + 1
    }

    pub fn cols(&self) -> IndexId {
        self.end.x - self.start.y + 1
    }

    pub fn iter(&self) -> PixelRectIter<'_> {
        PixelRectIter::new(&self)
    }
}

pub struct PixelRectIter<'a> {
    inner: &'a PixelRect,
    x: IndexId,
    y: IndexId,
}

impl<'a> PixelRectIter<'a> {
    pub fn new(inner: &'a PixelRect) -> Self {
        Self {
            inner,
            x: inner.start.x,
            y: inner.start.y,
        }
    }
}

impl<'a> std::iter::Iterator for PixelRectIter<'a> {
    type Item = Pixel;
    fn next(&mut self) -> Option<Self::Item> {
        if self.y > self.inner.end.y {
            return None;
        }

        if self.x > self.inner.end.x {
            self.x = self.inner.start.x;
            self.y += 1;
        }

        Some(Pixel {
            x: self.x,
            y: self.y,
        })
    }
}

pub enum SegmentQuery {
    All,
    Indices(Vec<SegmentId>),
}

pub enum ChannelQuery {
    All,
    Include(Vec<ChannelId>),
}

impl ChannelQuery {
    pub fn include(channels: impl IntoIterator<Item = impl Into<ChannelId>>) -> Self {
        let channels = channels.into_iter().map(|channel| channel.into()).collect();
        Self::Include(channels)
    }
}

#[derive(Debug, Clone)]
pub struct Pixel {
    x: IndexId,
    y: IndexId,
}

impl Pixel {
    pub fn new(x: IndexId, y: IndexId) -> Self {
        Self { x, y }
    }

    pub fn to_index(&self, cols: IndexId) -> IndexId {
        self.y * cols + self.x
    }
}

#[derive(Debug)]
pub enum QueryError {
    /// The pixel coordinate is invalid.
    OutOfBounds(Pixel),

    /// Error reading the zip archive.
    Zip {
        path: PathBuf,
        error: zip::result::ZipError,
    },

    InvalidFormat {
        path: PathBuf,
        cause: String,
    },

    InvalidData {
        path: PathBuf,
    },
}

pub enum FormatVersion {
    // 2.0
    V2_0,
}

impl FormatVersion {
    pub fn from_str(v: impl AsRef<str>) -> Option<FormatVersion> {
        match v.as_ref() {
            "2.0" => Some(Self::V2_0),
            _ => None,
        }
    }
}

pub struct Reader;
impl Reader {
    pub fn new<R>(reader: R) -> Result<impl QIMapReader, Error>
    where
        R: io::Read + io::Seek,
    {
        let mut archive = zip::ZipArchive::new(reader)?;
        let properties = {
            let mut properties =
                archive
                    .by_path(PROPERTIES_FILE_PATH)
                    .map_err(|error| Error::Zip {
                        path: PathBuf::from(PROPERTIES_FILE_PATH),
                        error,
                    })?;

            Properties::new(&mut properties).map_err(|_err| Error::InvalidFormat {
                path: PathBuf::from(PROPERTIES_FILE_PATH),
                cause: "invalid format".to_string(),
            })?
        };

        let Some(format_version) = properties.get(PROPERTIES_FILE_FORMAT_VERSION_KEY) else {
            return Err(Error::InvalidFormat {
                path: PathBuf::from(PROPERTIES_FILE_PATH),
                cause: format!("property `{PROPERTIES_FILE_FORMAT_VERSION_KEY}` not found"),
            });
        };
        let Some(format_version) = FormatVersion::from_str(format_version) else {
            return Err(Error::FileFormatNotSupported {
                version: format_version.clone(),
            });
        };

        match format_version {
            FormatVersion::V2_0 => v2_0::Reader::new(archive),
        }
    }
}

#[derive(derive_more::From, Debug)]
pub enum Error {
    #[from]
    OpenArchive(zip::result::ZipError),
    Zip {
        path: PathBuf,
        error: zip::result::ZipError,
    },
    InvalidFormat {
        path: PathBuf,
        cause: String,
    },
    FileFormatNotSupported {
        version: String,
    },
}
