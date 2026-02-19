use iced::{Task, widget::container};
use iced_aksel as aksel;
use jpk_reader as jpk;
use polars::prelude as pl;
use std::path::PathBuf;

mod plot;
mod voltage_spectroscopy;
mod voltage_spectroscopy_collection;

trait IsFileCollection {
    fn is_file_collection(&self) -> bool;
}

trait DefaultAxes {
    fn default_axes(&self) -> plot::Axes;
}

#[derive(Debug, Clone, derive_more::From)]
pub enum Message {
    WindowOpened(iced::window::Id),
    OpenDataTable,
    DataTableOpened(iced::window::Id),
    DataTableClosed,
    OpenFilesBrowser,
    FilesBrowserOpened(iced::window::Id),
    #[from]
    Plot(plot::Message),
    #[from]
    VoltageSpectroscopy(voltage_spectroscopy::Message),
    #[from]
    VoltageSpectroscopyCollection(voltage_spectroscopy_collection::Message),
}

#[derive(derive_more::Debug, derive_more::From)]
pub enum Reader {
    VoltageSpectroscopy(#[debug(skip)] jpk::voltage_spectroscopy::v2_0::FileReader),
    VoltageSpectroscopyCollection(#[debug(skip)] jpk::voltage_spectroscopy::v2_0::DirReader),
}

enum DatasetState {
    VoltageSpectroscopy(voltage_spectroscopy::State),
    VoltageSpectroscopyCollection(voltage_spectroscopy_collection::State),
}

impl IsFileCollection for DatasetState {
    fn is_file_collection(&self) -> bool {
        match self {
            DatasetState::VoltageSpectroscopy(state) => state.is_file_collection(),
            DatasetState::VoltageSpectroscopyCollection(state) => state.is_file_collection(),
        }
    }
}

impl DefaultAxes for DatasetState {
    fn default_axes(&self) -> plot::Axes {
        match self {
            DatasetState::VoltageSpectroscopy(state) => state.default_axes(),
            DatasetState::VoltageSpectroscopyCollection(state) => state.default_axes(),
        }
    }
}

struct Data {
    raw: pl::DataFrame,
    transforms: Vec<DataTransform>,
}

impl Data {
    pub fn new(df: pl::DataFrame) -> Self {
        Self {
            raw: df,
            transforms: vec![],
        }
    }
}

pub enum DataTransform {}

struct Children {
    data_table: Option<(iced::window::Id, DataTable)>,
    files_browser: Option<(iced::window::Id, ())>,
}

impl Default for Children {
    fn default() -> Self {
        Self {
            data_table: Default::default(),
            files_browser: Default::default(),
        }
    }
}

pub struct Dataset {
    path: PathBuf,
    window_id: iced::window::Id,
    reader: Reader,
    data: Data,
    state: DatasetState,
    plot: plot::State,
    children: Children,
}

impl Dataset {
    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn kind(&self) -> jpk::dataset::DatasetType {
        match &self.reader {
            Reader::VoltageSpectroscopy(_) => jpk::dataset::DatasetType::VoltageSpectroscopy,
            Reader::VoltageSpectroscopyCollection(_) => {
                jpk::dataset::DatasetType::VoltageSpectroscopyCollection
            }
        }
    }
}

impl Dataset {
    pub fn new(
        path: impl Into<PathBuf>,
        reader: Reader,
        dataframe: pl::DataFrame,
    ) -> (Self, iced::Task<Message>) {
        let state = match &reader {
            Reader::VoltageSpectroscopy(_) => DatasetState::VoltageSpectroscopy(
                voltage_spectroscopy::State::new(dataframe.clone()),
            ),
            Reader::VoltageSpectroscopyCollection(_) => {
                DatasetState::VoltageSpectroscopyCollection(
                    voltage_spectroscopy_collection::State::new(dataframe.clone()),
                )
            }
        };

        let plot = plot::State::new(dataframe.clone(), state.default_axes());

        let (window_id, open) = iced::window::open(iced::window::Settings::default());

        (
            Self {
                path: path.into(),
                window_id,
                reader,
                data: Data::new(dataframe),
                state,
                plot,
                children: Children::default(),
            },
            open.map(Message::WindowOpened),
        )
    }

    pub fn update(&mut self, message: Message) -> iced::Task<Message> {
        match message {
            Message::WindowOpened(_) => unreachable!("should be intercepted by parent"),
            Message::OpenDataTable => {
                if let Some((id, _)) = &self.children.data_table {
                    iced::window::gain_focus(id.clone())
                } else {
                    let (_, open) = iced::window::open(iced::window::Settings::default());
                    open.map(Message::DataTableOpened)
                }
            }
            Message::DataTableOpened(id) => {
                assert!(
                    self.children.data_table.is_none(),
                    "data table already exists"
                );
                let data_table = DataTable::new(self.data.raw.clone());
                let _ = self.children.data_table.insert((id.clone(), data_table));
                Task::done(Message::WindowOpened(id))
            }
            Message::DataTableClosed => {
                assert!(self.children.data_table.is_some());
                let _ = self.children.data_table.take();
                Task::none()
            }
            Message::OpenFilesBrowser => {
                todo!()
            }
            Message::FilesBrowserOpened(id) => todo!(),
            Message::Plot(message) => self.plot.update(message).map(Into::into),
            Message::VoltageSpectroscopy(message) => {
                let DatasetState::VoltageSpectroscopy(state) = &mut self.state else {
                    panic!("invalid message state")
                };

                state.update(message).map(Into::into)
            }
            Message::VoltageSpectroscopyCollection(message) => {
                let DatasetState::VoltageSpectroscopyCollection(state) = &mut self.state else {
                    panic!("invalid message state")
                };

                state.update(message).map(Into::into)
            }
        }
    }

    pub fn view(&self, window: &iced::window::Id) -> iced::Element<'_, Message> {
        if self.window_id == *window {
            let plot_container = iced::widget::container(self.plot.view().map(Message::Plot));

            let btn_open_data_table =
                iced::widget::button("Data table").on_press(Message::OpenDataTable);
            let mut controls = iced::widget::column![btn_open_data_table];
            if self.state.is_file_collection() {
                let btn_open_files_browser =
                    iced::widget::button("Files browser").on_press(Message::OpenFilesBrowser);
                controls = controls.push(btn_open_files_browser);
            }

            let controls_container = container(controls);

            return iced::widget::row![plot_container, controls_container].into();
        }
        if let Some((id, data_table)) = &self.children.data_table {
            if id == window {
                return data_table.view().into();
            }
        }
        if let Some((id, files_browser)) = &self.children.files_browser {
            if id == window {
                todo!();
            }
        }

        panic!("invalid window id")
    }
}

struct DataTable {
    df: pl::DataFrame,
}

impl DataTable {
    fn new(df: pl::DataFrame) -> Self {
        Self { df }
    }

    fn view(&self) -> iced::Element<'_, Message> {
        use iced::widget::text;

        // TODO: Headers should be sticky
        // TODO: Columns fit to data instead of header title causing overflow
        let columns = self.df.schema().iter().map(|(name, _dtype)| {
            iced::widget::table::column(name.as_str(), |idx: usize| {
                let col = self.df.column(name.as_str()).unwrap();
                match col.get(idx).unwrap() {
                    pl::AnyValue::Null => text(""),
                    pl::AnyValue::Boolean(value) => {
                        if value {
                            text("true")
                        } else {
                            text("false")
                        }
                    }
                    pl::AnyValue::Float64(value) => text(format!("{value:?}")),
                    pl::AnyValue::String(value) => text(value),
                    pl::AnyValue::UInt8(value) => text(format!("{value:?}")),
                    _ => todo!(),
                }
            })
        });

        let table = iced::widget::table::Table::new(columns, 0..self.df.height());
        iced::widget::scrollable(table).into()
    }
}
