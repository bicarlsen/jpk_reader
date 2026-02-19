//! Plot

use iced_aksel as aksel;
use polars::prelude::{self as pl};

pub type YAxisId = u8;
type TraceIdx = usize;

pub const X_AXIS_ID: &str = "x";
pub const Y_AXIS_IDS: [&str; 1] = ["y0"];
pub const LOG_AXIS_BASE: f64 = 10.0;

#[derive(Debug, Clone, Copy)]
pub enum AxisScale {
    Linear,
    Log,
}

impl Default for AxisScale {
    fn default() -> Self {
        Self::Linear
    }
}

#[derive(Clone, Debug)]
pub enum XAxisValues {
    /// Use dataframe index as axis values.
    Index,
    /// Use a column from the dataframe as axis values.
    /// Value is the name of the column.
    Series(String),
}

impl Default for XAxisValues {
    fn default() -> Self {
        Self::Index
    }
}

#[derive(Default)]
struct XAxis {
    scale: AxisScale,
    values: XAxisValues,
}

// TODO: Add marker option.
pub struct Trace {
    /// Column name in the dataframe.
    column: String,
    color: Option<iced::Color>,
    /// Aplha (opacity) channel.
    /// Between 0 (transparent) and 1 (opaque).
    alpha: f64,
}

struct YAxis {
    id: YAxisId,
    scale: AxisScale,
    position: aksel::axis::Position,
    traces: Vec<Trace>,
}

impl YAxis {
    pub fn new(id: YAxisId) -> Self {
        Self {
            id,
            scale: Default::default(),
            position: aksel::axis::Position::Left,
            traces: Default::default(),
        }
    }

    pub fn aksel_id(&self) -> &'static str {
        let idx = self.id as usize;
        Y_AXIS_IDS[idx].clone()
    }

    pub fn position_left(&mut self) {
        self.position = aksel::axis::Position::Left;
    }

    pub fn position_right(&mut self) {
        self.position = aksel::axis::Position::Right;
    }

    pub fn scale_linear(&mut self) {
        self.scale = AxisScale::Linear;
    }

    pub fn scale_log(&mut self) {
        self.scale = AxisScale::Log;
    }

    pub fn add_trace(&mut self, trace: Trace) {
        self.traces.push(trace);
    }

    pub fn remove_trace(&mut self, idx: TraceIdx) {
        self.traces.remove(idx);
    }

    pub fn minmax_f64(&self, dataframe: &pl::DataFrame) -> (f64, f64) {
        let mut min = f64::MAX;
        let mut max = f64::MIN;
        for trace in self.traces.iter() {
            let column = dataframe.column(&trace.column).unwrap();
            let (cmin, cmax) = column_minmax_f64(column);
            if cmin < min {
                min = cmin;
            }
            if cmax > max {
                max = cmax;
            }
        }

        (min, max)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Axis {
    X,
    Y(YAxisId),
}

impl Trace {
    pub fn new(column: impl Into<String>) -> Self {
        Self {
            column: column.into(),
            color: None,
            alpha: 1.0,
        }
    }
}

#[derive(Debug, Clone)]
pub enum Message {
    SetTitle(String),
    UpdateXAxisValues(XAxisValues),
    UpdateTrace {
        axis: YAxisId,
        trace: TraceIdx,
        column: String,
    },
}

pub struct Options {
    x_axis: XAxis,
    y_axes: Vec<YAxis>,
}

impl Options {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn x_axis(&mut self, x_axis: impl Into<String>) -> &mut Self {
        self.x_axis.values = XAxisValues::Series(x_axis.into());
        self
    }

    pub fn x_axis_log(&mut self) -> &mut Self {
        self.x_axis.scale = AxisScale::Log;
        self
    }

    pub fn new_y_axis(&mut self) -> (&mut Self, YAxisId) {
        let id = self.y_axes.iter().map(|ax| ax.id).max().unwrap() + 1;
        self.y_axes.push(YAxis::new(id));
        (self, id)
    }

    pub fn add_trace(&mut self, y_axis: YAxisId, trace: Trace) -> &mut Self {
        let ax = self.y_axes.iter_mut().find(|ax| ax.id == y_axis).unwrap();
        ax.add_trace(trace);
        self
    }
}

impl Default for Options {
    fn default() -> Self {
        Self {
            x_axis: Default::default(),
            y_axes: vec![YAxis::new(0)],
        }
    }
}

pub struct State {
    chart: aksel::State<&'static str, f64>,
    df: pl::DataFrame,
    title: String,
    x_axis: XAxis,
    y_axes: Vec<YAxis>,
}

impl State {
    pub fn new(dataframe: pl::DataFrame, options: Options) -> Result<Self, ()> {
        let Options { x_axis, y_axes } = options;

        let mut chart = aksel::State::new();
        chart.set_axis(X_AXIS_ID, x_axis_to_aksel(&x_axis, &dataframe));

        for axis in y_axes.iter() {
            let id = axis.aksel_id();
            let axis = y_axis_to_aksel(axis, &dataframe);
            chart.set_axis(id, axis);
        }

        Ok(Self {
            chart,
            df: dataframe,
            title: "".to_string(),
            x_axis,
            y_axes,
        })
    }

    pub fn update(&mut self, message: Message) -> iced::Task<Message> {
        match message {
            Message::SetTitle(_) => todo!(),
            Message::UpdateXAxisValues(value) => self.update_x_axis_values(value),
            Message::UpdateTrace {
                axis,
                trace,
                column,
            } => self.update_trace(axis, trace, column),
        }
    }
    fn update_x_axis_values(&mut self, values: XAxisValues) -> iced::Task<Message> {
        // TODO: account for logarithmic scale
        self.x_axis.values = values;
        self.chart
            .set_axis(X_AXIS_ID, x_axis_to_aksel(&self.x_axis, &self.df));

        iced::Task::none()
    }

    fn update_trace(
        &mut self,
        axis: YAxisId,
        trace: TraceIdx,
        column: String,
    ) -> iced::Task<Message> {
        let axis = self.y_axes.iter_mut().find(|ax| ax.id == axis).unwrap();
        let trace = &mut axis.traces[trace];
        trace.column = column;

        iced::Task::none()
    }
}

impl State {
    pub fn view(&self) -> iced::Element<'_, Message> {
        let plot = aksel::Chart::new(&self.chart).plot_data(self, X_AXIS_ID, Y_AXIS_IDS[0]);

        let axes_controls = self.axes_controls();
        iced::widget::column![plot, axes_controls].into()
    }

    fn axes_controls(&self) -> iced::Element<'_, Message> {
        let columns = self
            .df
            .schema()
            .iter()
            .map(|(name, _)| name.to_string())
            .collect::<Vec<_>>();

        let pl_yaxis = iced::widget::pick_list(
            columns.clone(),
            Some(self.y_axes[0].traces[0].column.clone()),
            |selection| Message::UpdateTrace {
                axis: 0,
                trace: 0,
                column: selection,
            },
        );
        let pl_yaxis = iced::widget::row![iced::widget::text("y-axis"), pl_yaxis];

        let columns = std::iter::once("".to_string())
            .chain(columns)
            .collect::<Vec<_>>();
        let pl_xaxis = iced::widget::pick_list(
            columns.clone(),
            match &self.x_axis.values {
                XAxisValues::Index => None,
                XAxisValues::Series(column) => Some(column.clone()),
            },
            |selection| {
                let values = if selection.is_empty() {
                    XAxisValues::Index
                } else {
                    XAxisValues::Series(selection)
                };

                Message::UpdateXAxisValues(values)
            },
        )
        .placeholder("<index>");
        let pl_xaxis = iced::widget::row![iced::widget::text("x-axis"), pl_xaxis];

        iced::widget::row![pl_yaxis, pl_xaxis].into()
    }
}

impl aksel::PlotData<f64> for State {
    fn draw(&self, plot: &mut aksel::Plot<f64>, theme: &iced::advanced::graphics::core::Theme) {
        let y = self.df.column(&self.y_axes[0].traces[0].column).unwrap();
        let y = column_to_values_f64(y);

        let x = match &self.x_axis.values {
            XAxisValues::Series(column) => {
                let x = self.df.column(column).unwrap();
                column_to_values_f64(x)
            }
            XAxisValues::Index => (0..self.df.height()).map(|x| x as f64).collect::<Vec<_>>(),
        };

        let points = std::iter::zip(x, y);
        for (x, y) in points {
            plot.add_shape(
                aksel::shape::Ellipse::new(
                    aksel::PlotPoint::new(x, y),
                    aksel::Measure::Screen(1.0),
                    aksel::Measure::Screen(1.0),
                )
                .fill(theme.palette().primary),
            );
        }
    }
}

fn column_minmax_f64(column: &pl::Column) -> (f64, f64) {
    let values = column.as_series().unwrap();
    match column.dtype() {
        pl::DataType::UInt8 => {
            let min = values.min::<u8>().unwrap().unwrap();
            let max = values.max::<u8>().unwrap().unwrap();
            (min as f64, max as f64)
        }
        pl::DataType::Float64 => {
            let min = values.min::<f64>().unwrap().unwrap();
            let max = values.max::<f64>().unwrap().unwrap();
            (min, max)
        }
        _ => todo!(),
    }
}

fn x_axis_to_aksel(axis: &XAxis, df: &pl::DataFrame) -> aksel::Axis<f64> {
    const POSITION: aksel::axis::Position = aksel::axis::Position::Bottom;

    match &axis.values {
        XAxisValues::Index => {
            aksel::Axis::new(aksel::scale::Linear::new(0.0, df.height() as f64), POSITION)
        }
        XAxisValues::Series(name) => {
            let column = df.column(name).unwrap();
            let (min, max) = column_minmax_f64(column);
            match axis.scale {
                AxisScale::Linear => {
                    aksel::Axis::new(aksel::scale::Linear::new(min, max), POSITION)
                }
                AxisScale::Log => aksel::Axis::new(aksel::scale::Linear::new(min, max), POSITION),
            }
        }
    }
}

fn y_axis_to_aksel(axis: &YAxis, df: &pl::DataFrame) -> aksel::Axis<f64> {
    let (min, max) = axis.minmax_f64(df);
    match axis.scale {
        AxisScale::Linear => aksel::Axis::new(aksel::scale::Linear::new(min, max), axis.position),
        AxisScale::Log => aksel::Axis::new(aksel::scale::Linear::new(min, max), axis.position),
    }
}

fn column_to_values_f64(column: &pl::Column) -> Vec<f64> {
    match column.dtype() {
        pl::DataType::Float64 => column
            .f64()
            .unwrap()
            .into_no_null_iter()
            .collect::<Vec<_>>(),
        pl::DataType::UInt8 => column
            .u8()
            .unwrap()
            .into_no_null_iter()
            .map(|v| v as f64)
            .collect::<Vec<_>>(),
        _ => todo!(),
    }
}
