use std::ffi::OsStr;
use std::io::{self, Result};
use std::iter::once;
use std::os::windows::ffi::OsStrExt;
use std::os::windows::io::OwnedHandle;
use std::sync::Arc;
use std::sync::mpsc::TryRecvError;

use crate::event::{OnResize, WindowSize};
use crate::tty::windows::child::ChildExitWatcher;
use crate::tty::{ChildEvent, EventedPty, EventedReadWrite, Options, Shell};

mod blocking;
mod child;
mod conpty;

use blocking::{UnblockedReader, UnblockedWriter};
use conpty::Conpty as Backend;
use miow::pipe::{AnonRead, AnonWrite};
use polling::{Event, Poller};

pub const PTY_CHILD_EVENT_TOKEN: usize = 1;
pub const PTY_READ_WRITE_TOKEN: usize = 2;

type ReadPipe = UnblockedReader<AnonRead>;
type WritePipe = UnblockedWriter<AnonWrite>;

pub struct Pty {
    // XXX: Backend is required to be the first field, to ensure correct drop order. Dropping
    // `conout` before `backend` will cause a deadlock (with Conpty).
    backend: Backend,
    conout: ReadPipe,
    conin: WritePipe,
    child_watcher: ChildExitWatcher,
    // Dropped after the watcher, which reads it until its wait is gone.
    _child: OwnedHandle,
}

pub fn new(config: &Options, window_size: WindowSize, _window_id: u64) -> Result<Pty> {
    conpty::new(config, window_size)
}

impl Pty {
    fn new(
        backend: impl Into<Backend>,
        conout: impl Into<ReadPipe>,
        conin: impl Into<WritePipe>,
        child_watcher: ChildExitWatcher,
        child: OwnedHandle,
    ) -> Self {
        Self {
            backend: backend.into(),
            conout: conout.into(),
            conin: conin.into(),
            child_watcher,
            _child: child,
        }
    }

    pub fn child_watcher(&self) -> &ChildExitWatcher {
        &self.child_watcher
    }
}

fn with_key(mut event: Event, key: usize) -> Event {
    event.key = key;
    event
}

impl EventedReadWrite for Pty {
    type Reader = ReadPipe;
    type Writer = WritePipe;

    #[inline]
    unsafe fn register(
        &mut self,
        poll: &Arc<Poller>,
        interest: polling::Event,
        poll_opts: polling::PollMode,
    ) -> io::Result<()> {
        self.conin.register(poll, with_key(interest, PTY_READ_WRITE_TOKEN), poll_opts);
        self.conout.register(poll, with_key(interest, PTY_READ_WRITE_TOKEN), poll_opts);
        self.child_watcher.register(poll, with_key(interest, PTY_CHILD_EVENT_TOKEN));

        Ok(())
    }

    #[inline]
    fn reregister(
        &mut self,
        poll: &Arc<Poller>,
        interest: polling::Event,
        poll_opts: polling::PollMode,
    ) -> io::Result<()> {
        self.conin.register(poll, with_key(interest, PTY_READ_WRITE_TOKEN), poll_opts);
        self.conout.register(poll, with_key(interest, PTY_READ_WRITE_TOKEN), poll_opts);
        self.child_watcher.register(poll, with_key(interest, PTY_CHILD_EVENT_TOKEN));

        Ok(())
    }

    #[inline]
    fn deregister(&mut self, _poll: &Arc<Poller>) -> io::Result<()> {
        self.conin.deregister();
        self.conout.deregister();
        self.child_watcher.deregister();

        Ok(())
    }

    #[inline]
    fn reader(&mut self) -> &mut Self::Reader {
        &mut self.conout
    }

    #[inline]
    fn writer(&mut self) -> &mut Self::Writer {
        &mut self.conin
    }
}

impl EventedPty for Pty {
    fn next_child_event(&mut self) -> Option<ChildEvent> {
        match self.child_watcher.event_rx().try_recv() {
            Ok(ev) => Some(ev),
            Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Disconnected) => Some(ChildEvent::Exited(None)),
        }
    }
}

impl OnResize for Pty {
    fn on_resize(&mut self, window_size: WindowSize) {
        self.backend.on_resize(window_size)
    }
}

// Modified per stdlib implementation.
// https://github.com/rust-lang/rust/blob/6707bf0f59485cf054ac1095725df43220e4be20/library/std/src/sys/args/windows.rs#L174
fn push_escaped_arg(cmd: &mut String, arg: &str) {
    let arg_bytes = arg.as_bytes();
    let quote = arg_bytes.iter().any(|c| *c == b' ' || *c == b'\t') || arg_bytes.is_empty();
    if quote {
        cmd.push('"');
    }

    let mut backslashes: usize = 0;
    for x in arg.chars() {
        if x == '\\' {
            backslashes += 1;
        } else {
            if x == '"' {
                // Add n+1 backslashes to total 2n+1 before internal '"'.
                cmd.extend((0..=backslashes).map(|_| '\\'));
            }
            backslashes = 0;
        }
        cmd.push(x);
    }

    if quote {
        // Add n backslashes to total 2n before ending '"'.
        cmd.extend((0..backslashes).map(|_| '\\'));
        cmd.push('"');
    }
}

fn cmdline(config: &Options) -> String {
    let default_shell = Shell::new("powershell".to_owned(), Vec::new());
    let shell = config.shell.as_ref().unwrap_or(&default_shell);

    let mut cmd = String::new();
    // With no application name, CreateProcessW takes an unquoted program path
    // up to each space in turn and runs the first file it finds, so
    // `C:\p9 dir\child.exe` runs a planted `C:\p9.exe`. A path holds no quote
    // marks, so wrapping it is all it needs.
    if config.escape_args && shell.program.contains([' ', '\t']) {
        cmd.push('"');
        cmd.push_str(&shell.program);
        cmd.push('"');
    } else {
        cmd.push_str(&shell.program);
    }

    for arg in &shell.args {
        cmd.push(' ');
        if config.escape_args {
            push_escaped_arg(&mut cmd, arg);
        } else {
            cmd.push_str(arg)
        }
    }
    cmd
}

/// Converts the string slice into a Windows-standard representation for "W"-
/// suffixed function variants, which accept UTF-16 encoded string values.
pub fn win32_string<S: AsRef<OsStr> + ?Sized>(value: &S) -> Vec<u16> {
    OsStr::new(value).encode_wide().chain(once(0)).collect()
}

#[cfg(test)]
mod test {
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    use windows_sys::Win32::Foundation::CloseHandle;
    use windows_sys::Win32::System::Threading::{
        GetCurrentProcess, GetProcessHandleCount, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
        QueryFullProcessImageNameW, TerminateProcess,
    };

    use crate::event::WindowSize;
    use crate::tty::windows::{cmdline, new, push_escaped_arg};
    use crate::tty::{ChildEvent, Options, Shell};

    const SIZE: WindowSize =
        WindowSize { num_lines: 24, num_cols: 80, cell_width: 8, cell_height: 16 };

    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("{name}_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn image_of(pid: u32) -> PathBuf {
        let mut name = vec![0u16; 1024];
        let mut len = name.len() as u32;
        unsafe {
            let process = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
            assert!(!process.is_null(), "pid {pid} is gone");
            let ok = QueryFullProcessImageNameW(process, 0, name.as_mut_ptr(), &mut len);
            CloseHandle(process);
            assert_ne!(ok, 0);
        }
        PathBuf::from(String::from_utf16_lossy(&name[..len as usize]))
    }

    fn handle_count() -> u32 {
        let mut count = 0;
        unsafe { GetProcessHandleCount(GetCurrentProcess(), &mut count) };
        count
    }

    // With a space in the program's path and a writable folder above it, an
    // unquoted command line ran a planted program instead.
    #[test]
    fn a_spaced_program_path_runs_that_program() {
        let dir = scratch("alacritty_spaced");
        let cmd = Path::new(&std::env::var_os("SystemRoot").unwrap()).join(r"System32\cmd.exe");
        let spaced = dir.join("p9 dir");
        std::fs::create_dir_all(&spaced).unwrap();
        let real = spaced.join("child.exe");
        std::fs::copy(&cmd, &real).unwrap();
        std::fs::copy(&cmd, dir.join("p9.exe")).unwrap();

        let options = Options {
            shell: Some(Shell::new(real.to_string_lossy().into_owned(), vec!["/k".into()])),
            escape_args: true,
            ..Default::default()
        };
        let pty = new(&options, SIZE, 0).unwrap();
        let watcher = pty.child_watcher();
        let ran = image_of(watcher.pid().unwrap().get());
        unsafe { TerminateProcess(watcher.raw_handle(), 0) };
        watcher.event_rx().recv_timeout(Duration::from_secs(10)).unwrap();
        drop(pty);
        assert!(ran.ends_with(r"p9 dir\child.exe"), "ran {}", ran.display());
        std::fs::remove_dir_all(&dir).ok();
    }

    // Each pane kept the child's process and thread handles and its own two
    // ends of the pseudo console's pipes, for as long as the terminal ran.
    #[test]
    fn a_closed_pane_gives_back_its_handles() {
        let open_and_close = || {
            let options = Options {
                shell: Some(Shell::new("cmd.exe".into(), vec!["/c".into(), "exit".into()])),
                escape_args: true,
                ..Default::default()
            };
            let pty = new(&options, SIZE, 0).unwrap();
            let exited = pty.child_watcher().event_rx().recv_timeout(Duration::from_secs(10));
            assert!(matches!(exited, Ok(ChildEvent::Exited(_))));
            drop(pty);
        };
        // the first few load the console host's libraries and start the pools
        for _ in 0..5 {
            open_and_close();
        }
        let before = handle_count();
        for _ in 0..50 {
            open_and_close();
        }
        let kept = handle_count().saturating_sub(before);
        // other tests run beside this one, so allow some noise; a leak is 200
        assert!(kept < 40, "50 closed panes kept {kept} handles");
    }

    #[test]
    fn test_escape() {
        let test_set = vec![
            // Basic cases - no escaping needed
            ("abc", "abc"),
            // Cases requiring quotes (space/tab)
            ("", "\"\""),
            (" ", "\" \""),
            ("ab c", "\"ab c\""),
            ("ab\tc", "\"ab\tc\""),
            // Cases with backslashes only (no spaces, no quotes) - no quotes added
            ("ab\\c", "ab\\c"),
            // Cases with quotes only (no spaces) - quotes escaped but no outer quotes
            ("ab\"c", "ab\\\"c"),
            ("\"", "\\\""),
            ("a\"b\"c", "a\\\"b\\\"c"),
            // Cases requiring both quotes and escaping (contains spaces)
            ("ab \"c", "\"ab \\\"c\""),
            ("a \"b\" c", "\"a \\\"b\\\" c\""),
            // Complex real-world cases
            ("C:\\Program Files\\", "\"C:\\Program Files\\\\\""),
            ("C:\\Program Files\\a.txt", "\"C:\\Program Files\\a.txt\""),
            (
                r#"sh -c "cd /home/user; ARG='abc' \""'${SHELL:-sh}" -i -c '"'echo hello'""#,
                r#""sh -c \"cd /home/user; ARG='abc' \\\"\"'${SHELL:-sh}\" -i -c '\"'echo hello'\"""#,
            ),
        ];

        for (input, expected) in test_set {
            let mut escaped_arg = String::new();
            push_escaped_arg(&mut escaped_arg, input);
            assert_eq!(escaped_arg, expected, "Failed for input: {}", input);
        }
    }

    #[test]
    fn test_cmdline() {
        let mut options = Options {
            shell: Some(Shell {
                program: "echo".to_string(),
                args: vec!["hello world".to_string()],
            }),
            working_directory: None,
            drain_on_exit: true,
            env: Default::default(),
            escape_args: false,
        };
        assert_eq!(cmdline(&options), "echo hello world");

        options.escape_args = true;
        assert_eq!(cmdline(&options), "echo \"hello world\"");
    }

    #[test]
    fn test_cmdline_spaced_program() {
        let mut options = Options {
            shell: Some(Shell {
                program: r"C:\p9 dir\child.exe".to_string(),
                args: vec!["--cd".to_string(), r"C:\Users\Jo Smith\Documents".to_string()],
            }),
            escape_args: true,
            ..Default::default()
        };
        assert_eq!(
            cmdline(&options),
            r#""C:\p9 dir\child.exe" --cd "C:\Users\Jo Smith\Documents""#
        );

        // unchanged without escaping, for configs that put arguments in the program
        options.escape_args = false;
        assert_eq!(cmdline(&options), r"C:\p9 dir\child.exe --cd C:\Users\Jo Smith\Documents");
    }
}
