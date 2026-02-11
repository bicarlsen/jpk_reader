//! JPK reader GUI.

use iced::{
    Element,
    widget::{button, text},
};

fn main() -> iced::Result {
    iced::run(update, view)
}

fn update(counter: &mut u64, message: Message) {
    match message {
        Message::Increment => *counter += 1,
    }
}

fn view(counter: &u64) -> Element<'_, Message> {
    button(text(counter)).on_press(Message::Increment).into()
}

#[derive(Debug, Clone)]
enum Message {
    Increment,
}
