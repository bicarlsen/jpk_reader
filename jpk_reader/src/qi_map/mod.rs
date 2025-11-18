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
    fn query_data(&mut self, query: &DataQuery) -> Result<Data, QueryError>;
    fn query_metadata(&mut self, query: &MetadataQuery) -> Result<Metadata, QueryError>;
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

    pub fn into_parts(self) -> (Vec<DataIndex>, Vec<Vec<Value>>) {
        let Data { indices, data } = self;
        (indices, data)
    }
}

pub struct Metadata {
    indices: Vec<MetadataIndex>,
    data: Vec<Properties>,
}

impl Metadata {
    pub fn new(
        indices: Vec<MetadataIndex>,
        data: Vec<Properties>,
    ) -> Result<Self, InvalidDataIndices> {
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

    pub fn get(&self, index: &MetadataIndex) -> Option<&Properties> {
        let idx = self.indices.binary_search(index).ok()?;
        Some(&self.data[idx])
    }
}

/// Indices size does not match data size.
#[derive(Debug)]
pub struct InvalidDataIndices;

#[derive(Debug, PartialEq, Ord, Eq)]
pub struct DataIndex {
    pub pixel: Pixel,
    pub segment: SegmentId,
    pub channel: ChannelId,
}

impl DataIndex {
    pub fn new(pixel: Pixel, segment: SegmentId, channel: impl Into<ChannelId>) -> Self {
        Self {
            pixel,
            segment,
            channel: channel.into(),
        }
    }
}

impl PartialOrd for DataIndex {
    /// Order by `(index, segment, channel)`.
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        if let Some(order) = self.pixel.partial_cmp(&other.pixel) {
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

impl From<(Pixel, SegmentId, ChannelId)> for DataIndex {
    fn from(value: (Pixel, SegmentId, ChannelId)) -> Self {
        let (pixel, segment, channel) = value;
        Self {
            pixel,
            segment,
            channel,
        }
    }
}

#[derive(Debug, PartialEq, Ord, Eq)]
pub enum MetadataIndex {
    Dataset,
    SharedData,
    Pixel(Pixel),
    Segment { index: IndexId, segment: SegmentId },
}

impl PartialOrd for MetadataIndex {
    /// Order hierarchically by `(dataset, shared data, index, segment)`.
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match (self, other) {
            (MetadataIndex::Dataset, MetadataIndex::Dataset)
            | (MetadataIndex::SharedData, MetadataIndex::SharedData) => Some(cmp::Ordering::Equal),

            (MetadataIndex::Dataset, MetadataIndex::SharedData)
            | (MetadataIndex::Dataset, MetadataIndex::Pixel(_))
            | (MetadataIndex::Dataset, MetadataIndex::Segment { .. })
            | (MetadataIndex::SharedData, MetadataIndex::Pixel(_))
            | (MetadataIndex::SharedData, MetadataIndex::Segment { .. })
            | (MetadataIndex::Pixel(_), MetadataIndex::Segment { .. }) => {
                Some(cmp::Ordering::Greater)
            }

            (MetadataIndex::SharedData, MetadataIndex::Dataset)
            | (MetadataIndex::Pixel(_), MetadataIndex::Dataset)
            | (MetadataIndex::Pixel(_), MetadataIndex::SharedData)
            | (MetadataIndex::Segment { .. }, MetadataIndex::Dataset)
            | (MetadataIndex::Segment { .. }, MetadataIndex::SharedData)
            | (MetadataIndex::Segment { .. }, MetadataIndex::Pixel(_)) => Some(cmp::Ordering::Less),

            (MetadataIndex::Pixel(a), MetadataIndex::Pixel(b)) => a.partial_cmp(b),

            (
                MetadataIndex::Segment {
                    index: idx_a,
                    segment: segment_a,
                },
                MetadataIndex::Segment {
                    index: idx_b,
                    segment: segment_b,
                },
            ) => {
                let mut ord = idx_a.partial_cmp(idx_b).unwrap();
                if matches!(ord, cmp::Ordering::Equal) {
                    ord = segment_a.partial_cmp(segment_b).unwrap();
                }
                Some(ord)
            }
        }
    }
}

pub struct DataQuery {
    pub index: IndexQuery,
    pub segment: SegmentQuery,
    pub channel: ChannelQuery,
}

impl DataQuery {
    pub fn select_all() -> Self {
        Self {
            index: IndexQuery::All,
            segment: SegmentQuery::All,
            channel: ChannelQuery::All,
        }
    }
}

pub enum MetadataQuery {
    All,
    /// Top level metadata.
    Dataset,
    SharedData,
    Index(IndexQuery),
    Segment {
        index: IndexQuery,
        segment: SegmentQuery,
    },
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
        let Pixel { i: xa, j: ya } = start;
        let Pixel { i: xb, j: yb } = end;
        let (x0, x1) = if xa < xb { (xa, xb) } else { (xb, xa) };
        let (y0, y1) = if ya < yb { (ya, yb) } else { (yb, ya) };

        let start = Pixel { i: x0, j: y0 };
        let end = Pixel { i: x1, j: y1 };
        Self { start, end }
    }

    pub fn rows(&self) -> IndexId {
        self.end.j - self.start.j + 1
    }

    pub fn cols(&self) -> IndexId {
        self.end.i - self.start.j + 1
    }

    pub fn iter(&self) -> PixelRectIter<'_> {
        PixelRectIter::new(&self)
    }
}

pub struct PixelRectIter<'a> {
    inner: &'a PixelRect,
    i: IndexId,
    j: IndexId,
}

impl<'a> PixelRectIter<'a> {
    pub fn new(inner: &'a PixelRect) -> Self {
        Self {
            inner,
            i: inner.start.i,
            j: inner.start.j,
        }
    }
}

impl<'a> std::iter::Iterator for PixelRectIter<'a> {
    type Item = Pixel;
    fn next(&mut self) -> Option<Self::Item> {
        if self.j > self.inner.end.j {
            return None;
        }

        if self.i > self.inner.end.i {
            self.i = self.inner.start.i;
            self.j += 1;
        }

        Some(Pixel {
            i: self.i,
            j: self.j,
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

#[derive(Debug, Clone, PartialEq, Eq, Ord)]
pub struct Pixel {
    i: IndexId,
    j: IndexId,
}

impl Pixel {
    pub fn new(i: IndexId, j: IndexId) -> Self {
        Self { i, j }
    }

    pub fn i(&self) -> IndexId {
        self.i
    }

    pub fn j(&self) -> IndexId {
        self.j
    }

    pub fn to_index(&self, cols: IndexId) -> IndexId {
        self.j * cols + self.i
    }
}

impl PartialOrd for Pixel {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        let mut ord = self.i.partial_cmp(&other.j).unwrap();
        if matches!(ord, cmp::Ordering::Equal) {
            ord = self.j.partial_cmp(&other.j).unwrap();
        }

        Some(ord)
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

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
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

#[derive(derive_more::From, Clone)]
pub enum VersionedReader<R> {
    V2_0(v2_0::Reader<R>),
}

impl<R> QIMapReader for VersionedReader<R>
where
    R: io::Read + io::Seek + Clone + Send + Sync,
{
    fn query_data(&mut self, query: &DataQuery) -> Result<Data, QueryError> {
        match self {
            VersionedReader::V2_0(reader) => reader.query_data(query),
        }
    }

    fn query_metadata(&mut self, query: &MetadataQuery) -> Result<Metadata, QueryError> {
        match self {
            VersionedReader::V2_0(reader) => reader.query_metadata(query),
        }
    }
}

pub struct Reader;
impl Reader {
    pub fn new<R>(reader: R) -> Result<impl QIMapReader, Error>
    where
        R: io::Read + io::Seek + Clone + Send + Sync,
    {
        let mut archive = zip::ZipArchive::new(reader)?;
        let format_version = Self::format_version(&mut archive)?;
        let Some(format_version) = FormatVersion::from_str(&format_version) else {
            return Err(Error::FileFormatNotSupported {
                version: format_version.clone(),
            });
        };

        match format_version {
            FormatVersion::V2_0 => v2_0::Reader::new(archive).into(),
        }
    }

    pub fn new_versioned<R>(reader: R) -> Result<VersionedReader<R>, Error>
    where
        R: io::Read + io::Seek,
    {
        let mut archive = zip::ZipArchive::new(reader)?;
        let format_version = Self::format_version(&mut archive)?;
        let Some(format_version) = FormatVersion::from_str(&format_version) else {
            return Err(Error::FileFormatNotSupported {
                version: format_version.clone(),
            });
        };

        let reader = match format_version {
            FormatVersion::V2_0 => v2_0::Reader::new(archive)?.into(),
        };
        Ok(reader)
    }

    pub fn format_version<R>(archive: &mut zip::ZipArchive<R>) -> Result<String, Error>
    where
        R: io::Read + io::Seek,
    {
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

        Ok(format_version.clone())
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
    /// A reader for the format version is not available.
    FileFormatNotSupported {
        version: String,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO: Clean up error messages.
        write!(f, "{self:?}")
    }
}
