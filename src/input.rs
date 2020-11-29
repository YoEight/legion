use std::io::{self, Stdin, Stdout, Write};
use termion::cursor::DetectCursorPos;
use termion::event::Key;
use termion::input::{Keys, TermRead};
use termion::raw::IntoRawMode;

pub enum Input {
    String(String),
    Command(String),
    Exit,
}

pub struct Inputs {
    buffer: String,
    offset: u16,
    start_pos: u16,
    keys: Keys<Stdin>,
}

impl Inputs {
    pub fn new() -> Self {
        let keys = io::stdin().keys();

        Inputs {
            keys,
            buffer: String::new(),
            offset: 0,
            start_pos: 3,
        }
    }

    pub fn await_input(&mut self, stdout: &mut Stdout) -> io::Result<Input> {
        let mut stdout = stdout.into_raw_mode()?;

        let (_, y) = stdout.cursor_pos()?;
        write!(stdout, "\n{}λ ", termion::cursor::Goto(1, y + 1))?;
        stdout.flush()?;

        while let Some(c) = self.keys.next().transpose()? {
            let (_, y) = stdout.cursor_pos()?;

            match c {
                Key::Ctrl('c') => {
                    println!();
                    return Ok(Input::Exit);
                }

                Key::Backspace if self.offset > 0 => {
                    self.offset -= 1;
                    self.buffer.remove(self.offset as usize);
                    write!(
                        stdout,
                        "{}{}λ {}{}",
                        termion::cursor::Goto(1, y),
                        termion::clear::CurrentLine,
                        self.buffer,
                        termion::cursor::Goto(self.start_pos + self.offset, y)
                    )?;
                }

                Key::Left if self.offset > 0 => {
                    self.offset -= 1;
                    write!(
                        stdout,
                        "{}{}λ {}{}",
                        termion::cursor::Goto(1, y),
                        termion::clear::CurrentLine,
                        self.buffer,
                        termion::cursor::Goto(self.start_pos + self.offset, y)
                    )?;
                }

                Key::Right if self.offset < self.buffer.len() as u16 => {
                    self.offset += 1;
                    write!(
                        stdout,
                        "{}{}λ {}{}",
                        termion::cursor::Goto(1, y),
                        termion::clear::CurrentLine,
                        self.buffer,
                        termion::cursor::Goto(self.start_pos + self.offset, y)
                    )?;
                }

                Key::Char('\n') => {
                    let line = std::mem::replace(&mut self.buffer, String::new());
                    let line = line.as_str().trim();

                    if line.is_empty() {
                        write!(stdout, "\n{}λ ", termion::cursor::Goto(1, y + 1))?;
                        stdout.flush()?;
                        continue;
                    }

                    self.offset = 0;

                    if let Some(cmd) = line.strip_prefix(":") {
                        if cmd == "exit" {
                            return Ok(Input::Exit);
                        }

                        if cmd.is_empty() {
                            continue;
                        }

                        return Ok(Input::Command(cmd.to_string()));
                    }

                    return Ok(Input::String(line.to_string()));
                }

                Key::Char(c) => {
                    self.offset += 1;

                    if self.offset < (self.buffer.len() + 1) as u16 {
                        self.buffer.insert((self.offset as usize) - 1, c);
                        write!(
                            stdout,
                            "{}{}λ {}{}",
                            termion::cursor::Goto(1, y),
                            termion::clear::CurrentLine,
                            self.buffer,
                            termion::cursor::Goto(self.start_pos + self.offset, y)
                        )?;
                    } else {
                        self.buffer.push(c);
                        write!(
                            stdout,
                            "{}{}λ {}",
                            termion::cursor::Goto(1, y),
                            termion::clear::CurrentLine,
                            self.buffer
                        )?;
                    }
                }
                _ => {}
            }
            stdout.flush()?;
        }

        Ok(Input::Exit)
    }
}
