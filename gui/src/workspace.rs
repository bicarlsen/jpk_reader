//! Workspace.
use crate::icon;
use iced::advanced::graphics::core::window;
use iced::widget::{button, column, row, text};
use jpk_reader as jpk;
use std::path::PathBuf;

pub enum DatasetState {
    Ok,
    Err(String),
    Loading,
}
struct Dataset {
    path: PathBuf,
    data: DatasetState,
    kind: Option<jpk::dataset::DatasetType>,
    window: Option<iced::window::Id>,
}

#[derive(Debug, Clone)]
pub enum Message {
    /// The workspace was opened.
    WorkspaceOpened(iced::window::Id),
    WorkspaceClosed(iced::window::Id),
    /// Open a dataset file chooser.
    PromptOpenDatasetFile,
    /// Open a dataset directory chooser.
    PromptOpenDatasetDir,
    /// The user selected a datset file path to try to open.
    DatasetFilePathSelected(PathBuf),
    /// The user selected a datset directory path to try to open.
    DatasetDirPathSelected(PathBuf),
    /// A dataset is loading.
    DatasetLoading {
        path: PathBuf,
    },
    /// A dataset loaded successfully.
    DatasetLoaded {
        path: PathBuf,
        kind: jpk::dataset::DatasetType,
    },
    /// A dataset window opened
    DatasetWindowOpened {
        path: PathBuf,
        window: iced::window::Id,
    },
    /// An error occurred while loading the dataset.
    DatasetError {
        path: PathBuf,
        error: String,
    },
    DatasetClosed {
        path: PathBuf,
    },
}

pub(crate) struct Workspace {
    _title: String,
    datasets: Vec<Dataset>,
}

impl Default for Workspace {
    fn default() -> Self {
        Self {
            _title: "jpk reader".into(),
            datasets: Default::default(),
        }
    }
}

impl Workspace {
    pub fn title(&self, window: iced::window::Id) -> String {
        self._title.clone()
    }
}

impl Workspace {
    pub fn new() -> (Self, iced::Task<Message>) {
        let (_, open) = iced::window::open(window::Settings::default());

        (Self::default(), open.map(Message::WorkspaceOpened))
    }

    pub fn update(&mut self, message: Message) -> iced::Task<Message> {
        match message {
            Message::WorkspaceOpened(id) => iced::Task::none(),
            Message::WorkspaceClosed(id) => self.workspace_closed(id),
            Message::PromptOpenDatasetFile => self.prompt_open_dataset_file(),
            Message::PromptOpenDatasetDir => self.prompt_open_dataset_dir(),
            Message::DatasetFilePathSelected(_) => iced::Task::done(message),
            Message::DatasetDirPathSelected(_) => iced::Task::done(message),
            Message::DatasetLoading { path } => self.dataset_loading(path),
            Message::DatasetLoaded { path, kind } => self.dataset_loaded(path, kind),
            Message::DatasetWindowOpened { path, window } => {
                self.dataset_window_opened(path, window)
            }
            Message::DatasetError { path, error } => self.dataset_error(path, error),
            Message::DatasetClosed { path } => self.dataset_closed(path),
        }
    }

    pub fn view(&self) -> iced::Element<'_, Message> {
        let btn_open_dataset_file = button(icon::file()).on_press(Message::PromptOpenDatasetFile);
        let btn_open_dataset_dir = button(icon::opendir()).on_press(Message::PromptOpenDatasetDir);
        let dataset_commands = column![row![btn_open_dataset_file, btn_open_dataset_dir]];

        let mut dataset_list = column![];
        for dataset in self.datasets.iter() {
            dataset_list = dataset_list.push(text(dataset.path.to_string_lossy()));
        }

        let lo_main = column![dataset_commands, dataset_list];
        lo_main.into()
    }

    pub fn subscription(&self) -> iced::Subscription<Message> {
        iced::window::close_events().map(Message::WorkspaceClosed)
    }
}

impl Workspace {
    fn workspace_closed(&mut self, window: iced::window::Id) -> iced::Task<Message> {
        iced::Task::none()
    }

    fn prompt_open_dataset_file(&mut self) -> iced::Task<Message> {
        rfd::FileDialog::new()
            .set_title("Open dataset file")
            .pick_file()
            .map(|path| iced::Task::done(Message::DatasetFilePathSelected(path)))
            .unwrap_or(iced::Task::none())
    }

    fn prompt_open_dataset_dir(&mut self) -> iced::Task<Message> {
        rfd::FileDialog::new()
            .set_title("Open dataset folder")
            .pick_folder()
            .map(|path| iced::Task::done(Message::DatasetDirPathSelected(path)))
            .unwrap_or(iced::Task::none())
    }

    fn dataset_loading(&mut self, path: PathBuf) -> iced::Task<Message> {
        if let Some(dataset) = self
            .datasets
            .iter_mut()
            .find(|dataset| dataset.path == path)
        {
            dataset.data = DatasetState::Loading;
        } else {
            self.datasets.push(Dataset {
                path,
                data: DatasetState::Loading,
                kind: None,
                window: None,
            });
        }

        iced::Task::none()
    }

    fn dataset_loaded(
        &mut self,
        path: PathBuf,
        kind: jpk::dataset::DatasetType,
    ) -> iced::Task<Message> {
        let Some(dataset) = self
            .datasets
            .iter_mut()
            .find(|dataset| dataset.path == path)
        else {
            todo!("dataset loaded, but doesn't exist");
        };

        dataset.data = DatasetState::Ok;
        let _ = dataset.kind.insert(kind);
        iced::Task::none()
    }

    fn dataset_window_opened(
        &mut self,
        path: PathBuf,
        window: iced::window::Id,
    ) -> iced::Task<Message> {
        let Some(dataset) = self
            .datasets
            .iter_mut()
            .find(|dataset| dataset.path == path)
        else {
            todo!("dataset loaded, but doesn't exist");
        };

        let _ = dataset.window.insert(window);
        iced::Task::none()
    }

    fn dataset_error(&mut self, path: PathBuf, error: String) -> iced::Task<Message> {
        todo!()
    }

    fn dataset_closed(&mut self, path: PathBuf) -> iced::Task<Message> {
        self.datasets.retain(|dataset| dataset.path != path);
        iced::Task::none()
    }
}
