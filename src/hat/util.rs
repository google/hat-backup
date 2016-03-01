use std::io;
use std::str;


pub struct InfoWriter;

impl io::Write for InfoWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        info!("{}", str::from_utf8(&buf[..]).unwrap());
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
