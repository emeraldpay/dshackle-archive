use lazy_static::lazy_static;

lazy_static! {
    pub static ref SHUTDOWN: shutdown::Shutdown = shutdown::Shutdown::new().expect("Failed to create a shutdown hook");
}

pub fn get_shutdown() -> shutdown::Shutdown {
    SHUTDOWN.clone()
}
