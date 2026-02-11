use iced_aksel as aksel;
use jpk_reader as jpk;
use polars::prelude as pl;
use polars::prelude::*;
use std::{iter, path::PathBuf};

pub const X_AXIS_ID: &str = "x_id";
pub const Y_AXIS_ID: &str = "y_id";

#[derive(Debug, Clone)]
pub enum Message {
    WindowOpened(iced::window::Id),
    Temp,
}

#[derive(derive_more::Debug, derive_more::From)]
pub enum Reader {
    VoltageSpectroscopy(#[debug(skip)] jpk::voltage_spectroscopy::v2_0::FileReader),
    VoltageSpectroscopyCollection(#[debug(skip)] jpk::voltage_spectroscopy::v2_0::DirReader),
}

pub struct Dataset {
    path: PathBuf,
    reader: Reader,
    dataframe: pl::DataFrame,
    plot_state: aksel::State<&'static str, f64>,
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
        const DEFAULT_X_COL: &str = "cafmBias";
        const DEFAULT_Y_COL: &str = "cafmCurrent";

        let xy_bounds = dataframe
            .clone()
            .lazy()
            .select([
                col(DEFAULT_X_COL).min().alias("xmin"),
                col(DEFAULT_Y_COL).min().alias("ymin"),
                col(DEFAULT_X_COL).max().alias("xmax"),
                col(DEFAULT_Y_COL).max().alias("ymax"),
            ])
            .collect()
            .unwrap();
        let x_min = match xy_bounds.column("xmin").unwrap().get(0).unwrap() {
            AnyValue::Float64(value) => value,
            _ => panic!("invalid datatype"),
        };
        let x_max = match xy_bounds.column("xmax").unwrap().get(0).unwrap() {
            AnyValue::Float64(value) => value,
            _ => panic!("invalid datatype"),
        };
        let y_min = match xy_bounds.column("ymin").unwrap().get(0).unwrap() {
            AnyValue::Float64(value) => value,
            _ => panic!("invalid datatype"),
        };
        let y_max = match xy_bounds.column("ymax").unwrap().get(0).unwrap() {
            AnyValue::Float64(value) => value,
            _ => panic!("invalid datatype"),
        };

        let mut plot_state = aksel::State::new();
        plot_state.set_axis(
            X_AXIS_ID,
            aksel::Axis::new(
                aksel::scale::Linear::new(x_min, x_max),
                aksel::axis::Position::Bottom,
            ),
        );
        plot_state.set_axis(
            Y_AXIS_ID,
            aksel::Axis::new(
                aksel::scale::Linear::new(y_min, y_max),
                aksel::axis::Position::Left,
            ),
        );

        let (_, open) = iced::window::open(iced::window::Settings::default());

        (
            Self {
                path: path.into(),
                reader,
                dataframe,
                plot_state,
            },
            open.map(Message::WindowOpened),
        )
    }

    pub fn update(&mut self, message: Message) -> iced::Task<Message> {
        todo!()
    }

    pub fn view(&self) -> iced::Element<'_, Message> {
        aksel::Chart::new(&self.plot_state)
            .plot_data(self, X_AXIS_ID, Y_AXIS_ID)
            .into()
    }
}

impl aksel::PlotData<f64> for Dataset {
    fn draw(&self, plot: &mut aksel::Plot<f64>, theme: &iced::advanced::graphics::core::Theme) {
        let x = self.dataframe.column("cafmBias").unwrap().f64().unwrap();
        let y = self.dataframe.column("cafmCurrent").unwrap().f64().unwrap();
        let points = iter::zip(x.into_no_null_iter(), y.into_no_null_iter());
        for (x, y) in points {
            plot.add_shape(
                aksel::shape::Ellipse::new(
                    aksel::PlotPoint::new(x, y),
                    aksel::Measure::Screen(5.0),
                    aksel::Measure::Screen(5.0),
                )
                .fill(theme.palette().primary),
            );
        }
    }
}
