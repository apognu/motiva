use std::{
  io,
  sync::{Arc, Mutex},
};

#[derive(Clone, Debug, Default)]
pub(super) struct VecLogWriter {
  buffer: Arc<Mutex<Vec<u8>>>,
  lines: Arc<Mutex<Vec<String>>>,
}

impl VecLogWriter {
  pub(super) fn new(lines: Arc<Mutex<Vec<String>>>) -> Self {
    Self {
      buffer: Arc::new(Mutex::new(Vec::new())),
      lines,
    }
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
    }

    Ok(buf.len())
  }

  fn flush(&mut self) -> io::Result<()> {
    Ok(())
  }
}
