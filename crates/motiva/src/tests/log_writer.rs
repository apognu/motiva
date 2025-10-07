use std::{
  io,
  sync::{Arc, Mutex, mpsc},
};

#[derive(Clone, Debug)]
pub(super) struct VecLogWriter {
  buffer: Arc<Mutex<Vec<u8>>>,
  lines: Arc<Mutex<Vec<String>>>,
  done: mpsc::Sender<()>,
}

impl VecLogWriter {
  pub(super) fn new(lines: Arc<Mutex<Vec<String>>>) -> (Self, mpsc::Receiver<()>) {
    let (tx, rx) = mpsc::channel();

    (
      Self {
        buffer: Arc::new(Mutex::new(Vec::new())),
        lines,
        done: tx,
      },
      rx,
    )
  }
}

impl io::Write for VecLogWriter {
  fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
    let mut buffer = self.buffer.lock().unwrap();

    buffer.extend_from_slice(buf);

    while let Some(i) = buffer.iter().position(|&b| b == b'\n') {
      let bytes = buffer.drain(..=i).collect::<Vec<u8>>();
      let line = String::from_utf8(bytes).unwrap().trim_end().to_string();

      println!("{}", line);

      self.lines.lock().unwrap().push(line);
      self.done.send(()).unwrap();
    }

    Ok(buf.len())
  }

  fn flush(&mut self) -> io::Result<()> {
    Ok(())
  }
}
