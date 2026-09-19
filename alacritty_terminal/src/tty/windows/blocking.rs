//! Code for running a reader/writer on another thread while driving it through `polling`.

use std::io::prelude::*;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::{io, thread};

use piper::{Reader, Writer, pipe};
use polling::os::iocp::{CompletionPacket, PollerIocpExt};
use polling::{Event, PollMode, Poller};

use crate::thread::spawn_named;

struct Registration {
    interest: Mutex<Option<Interest>>,
    end: PipeEnd,
}

#[derive(Copy, Clone)]
enum PipeEnd {
    Reader,
    Writer,
}

struct Interest {
    /// The event to send about completion.
    event: Event,

    /// The poller to send the event to.
    poller: Arc<Poller>,

    /// The mode that we are in.
    mode: PollMode,
}

/// Poll a reader in another thread.
pub struct UnblockedReader<R> {
    /// The event to send about completion.
    interest: Arc<Registration>,

    /// The pipe that we are reading from.
    pipe: Reader,

    /// Is this the first time registering?
    first_register: bool,

    /// We logically own the reader, but we don't actually use it.
    _reader: PhantomData<R>,
}

impl<R: Read + Send + 'static> UnblockedReader<R> {
    /// Spawn a new unblocked reader.
    pub fn new(mut source: R, pipe_capacity: usize) -> Self {
        // Create a new pipe.
        let (reader, mut writer) = pipe(pipe_capacity);
        let interest = Arc::new(Registration {
            interest: Mutex::<Option<Interest>>::new(None),
            end: PipeEnd::Reader,
        });

        // Spawn the reader thread.
        let notify = interest.clone();
        spawn_named("alacritty-tty-reader-thread", move || {
            let waker = Waker::from(Arc::new(ThreadWaker(thread::current())));
            let mut context = Context::from_waker(&waker);

            loop {
                // Read from the reader into the pipe.
                match writer.poll_fill(&mut context, &mut source) {
                    Poll::Ready(Ok(0)) => {
                        // Either the pipe is closed or the reader is at its EOF.
                        // In any case, we are done.
                        return;
                    },

                    Poll::Ready(Ok(_)) => {
                        // Tell the consumer that data is available.
                        //
                        // The waker installed by `poll_drain_bytes` is only armed when
                        // the consumer drained the pipe empty, but `EventLoop::pty_read`
                        // deliberately stops after `MAX_LOCKED_READ` bytes so it does not
                        // hold the terminal lock for too long. It therefore returns to the
                        // poller regularly with data still buffered and no waker armed.
                        // Once the pipe fills after that, this thread parks and no further
                        // write can happen, so nothing remains that could wake the
                        // consumer: it waits for a notification that only a write could
                        // produce, the write waits for space that only the consumer could
                        // free, and the PTY blocks forever.
                        Wake::wake_by_ref(&notify);
                        continue;
                    },

                    Poll::Ready(Err(e)) if e.kind() == io::ErrorKind::Interrupted => {
                        // We were interrupted; continue.
                        continue;
                    },

                    Poll::Ready(Err(e)) => {
                        log::error!("error writing to pipe: {}", e);
                        return;
                    },

                    Poll::Pending => {
                        // We are now waiting on the other end to advance. Park the
                        // thread until they do.
                        thread::park();
                    },
                }
            }
        });

        Self { interest, pipe: reader, first_register: true, _reader: PhantomData }
    }

    /// Register interest in the reader.
    pub fn register(&mut self, poller: &Arc<Poller>, event: Event, mode: PollMode) {
        let mut interest = self.interest.interest.lock().unwrap();
        *interest = Some(Interest { event, poller: poller.clone(), mode });

        // Send the event to start off with if we have any data.
        if (!self.pipe.is_empty() && event.readable) || self.first_register {
            self.first_register = false;
            poller.post(CompletionPacket::new(event)).ok();
        }
    }

    /// Deregister interest in the reader.
    pub fn deregister(&self) {
        let mut interest = self.interest.interest.lock().unwrap();
        *interest = None;
    }

    /// Try to read from the reader.
    pub fn try_read(&mut self, buf: &mut [u8]) -> usize {
        let waker = Waker::from(self.interest.clone());

        match self.pipe.poll_drain_bytes(&mut Context::from_waker(&waker), buf) {
            Poll::Pending => 0,
            Poll::Ready(n) => n,
        }
    }
}

impl<R: Read + Send + 'static> Read for UnblockedReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        Ok(self.try_read(buf))
    }
}

/// Poll a writer in another thread.
pub struct UnblockedWriter<W> {
    /// The interest to send about completion.
    interest: Arc<Registration>,

    /// The pipe that we are writing to.
    pipe: Writer,

    /// We logically own the writer, but we don't actually use it.
    _reader: PhantomData<W>,
}

impl<W: Write + Send + 'static> UnblockedWriter<W> {
    /// Spawn a new unblocked writer.
    pub fn new(mut sink: W, pipe_capacity: usize) -> Self {
        // Create a new pipe.
        let (mut reader, writer) = pipe(pipe_capacity);
        let interest = Arc::new(Registration {
            interest: Mutex::<Option<Interest>>::new(None),
            end: PipeEnd::Writer,
        });

        // Spawn the writer thread.
        spawn_named("alacritty-tty-writer-thread", move || {
            let waker = Waker::from(Arc::new(ThreadWaker(thread::current())));
            let mut context = Context::from_waker(&waker);

            loop {
                // Write from the pipe into the writer.
                match reader.poll_drain(&mut context, &mut sink) {
                    Poll::Ready(Ok(0)) => {
                        // Either the pipe is closed or the writer is full.
                        // In any case, we are done.
                        return;
                    },

                    Poll::Ready(Ok(_)) => {
                        // Keep writing.
                        continue;
                    },

                    Poll::Ready(Err(e)) if e.kind() == io::ErrorKind::Interrupted => {
                        // We were interrupted; continue.
                        continue;
                    },

                    Poll::Ready(Err(e)) => {
                        log::error!("error writing to pipe: {}", e);
                        return;
                    },

                    Poll::Pending => {
                        // We are now waiting on the other end to advance. Park the
                        // thread until they do.
                        thread::park();
                    },
                }
            }
        });

        Self { interest, pipe: writer, _reader: PhantomData }
    }

    /// Register interest in the writer.
    pub fn register(&self, poller: &Arc<Poller>, event: Event, mode: PollMode) {
        let mut interest = self.interest.interest.lock().unwrap();
        *interest = Some(Interest { event, poller: poller.clone(), mode });

        // Send the event to start off with if we have room for data.
        if !self.pipe.is_full() && event.writable {
            poller.post(CompletionPacket::new(event)).ok();
        }
    }

    /// Deregister interest in the writer.
    pub fn deregister(&self) {
        let mut interest = self.interest.interest.lock().unwrap();
        *interest = None;
    }

    /// Try to write to the writer.
    pub fn try_write(&mut self, buf: &[u8]) -> usize {
        let waker = Waker::from(self.interest.clone());

        match self.pipe.poll_fill_bytes(&mut Context::from_waker(&waker), buf) {
            Poll::Pending => 0,
            Poll::Ready(n) => n,
        }
    }
}

impl<W: Write + Send + 'static> Write for UnblockedWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        Ok(self.try_write(buf))
    }

    fn flush(&mut self) -> io::Result<()> {
        // Nothing to flush.
        Ok(())
    }
}

struct ThreadWaker(thread::Thread);

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

impl Wake for Registration {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let mut interest_lock = self.interest.lock().unwrap();
        if let Some(interest) = interest_lock.as_ref() {
            // Send the event to the poller.
            let send_event = match self.end {
                PipeEnd::Reader => interest.event.readable,
                PipeEnd::Writer => interest.event.writable,
            };

            if send_event {
                interest.poller.post(CompletionPacket::new(interest.event)).ok();

                // Clear the event if we're in oneshot mode.
                if matches!(interest.mode, PollMode::Oneshot | PollMode::EdgeOneshot) {
                    *interest_lock = None;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    struct Endless;

    impl Read for Endless {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            buf.fill(b'x');
            Ok(buf.len())
        }
    }

    // A read as big as the pipe empties it without ever finding it empty, so
    // the pipe's own waker is never armed, which is how `EventLoop::pty_read`
    // ends a round. Only the reader thread's word after each fill wakes the
    // consumer again. Without it a long run of output froze the pane for good.
    #[test]
    fn a_drained_pipe_says_when_it_fills_again() {
        const CAPACITY: usize = 1 << 20;
        let poller = Arc::new(Poller::new().unwrap());
        let mut reader = UnblockedReader::new(Endless, CAPACITY);
        reader.register(&poller, Event::readable(7), PollMode::Level);
        let mut buf = vec![0u8; CAPACITY];
        let mut events = polling::Events::new();
        let mut total = 0;
        for round in 0..64 {
            events.clear();
            poller.wait(&mut events, Some(Duration::from_secs(3))).unwrap();
            assert!(!events.is_empty(), "no notice for 3 s after {total} bytes in {round} rounds");
            if round % 2 == 1 {
                thread::sleep(Duration::from_millis(5));
            }
            total += reader.try_read(&mut buf);
        }
    }
}
