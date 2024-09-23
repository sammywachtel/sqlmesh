import io
import pty
import sys
import threading

import pytest
import os

from pytest_mock import MockerFixture

from sqlmesh.core import constants


# Mock for sys.stdout without fileno (e.g., LoggingProxy)
class MockNoFileno:
    def write(self, text):
        pass

    def flush(self):
        pass


# Mock for sys.stdout with fileno (returning a pseudo-terminal)

class MockTerminal(io.IOBase):
    def __init__(self):
        # Create a pseudo-terminal
        self.master_fd, self.slave_fd = pty.openpty()
        self.collected_text_buffer = ''

        # Event to synchronize reading and flushing
        self.read_event = threading.Event()

        # set up a thread to continuously flush the pseudo-terminal
        self.reader_thread = threading.Thread(target=self._read_from_slave)
        self.reader_thread.daemon = True
        self.reader_thread.start()

    def fileno(self):
        return self.slave_fd  # Return valid pseudo-terminal descriptor

    def isatty(self):
        # Returns True if it's a terminal
        return os.isatty(self.slave_fd)

    def writable(self):
        # Terminals are writable
        return True

    def readable(self):
        # In this mock terminal, we only simulate writing, so not readable
        return False

    def write(self, text):
        # Clear the event before writing new data, so it doesn't assume the previous data is done
        self.read_event.clear()
        # Simulate writing to the master side of the pseudo-term
        os.write(self.master_fd, text.encode())
        return len(text)  # Return the number of bytes written

    def flush(self):
        # Ensure all data is read before flushing
        self.read_event.wait()  # Wait until the read thread signals completion
        sys.__stdout__.write(self.collected_text_buffer + "\n")

    def close(self):
        # Close the file descriptors for the pseudo-terminal
        os.close(self.master_fd)
        os.close(self.slave_fd)

    def _read_from_slave(self):
        # Continuously read from the slave to avoid buffer overflow
        while True:
            data = os.read(self.slave_fd, 1024)
            if not data:
                break
            self.collected_text_buffer += data.decode()

            # Signal that data has been read
            self.read_event.set()


# Mock for sys.stdout with fileno but not a TTY (simulating headless which might be a daemon)
class MockTTYWithInvalidDescriptor(MockTerminal):
    def fileno(self):
        return -1


# Mock for sys.stdout with fileno but not a TTY (simulating headless which might be a daemon)
class MockNonTTY(MockTerminal):
    def fileno(self):
        # Create a pipe to simulate non-TTY stdout
        read_fd, write_fd = os.pipe()
        return write_fd  # Return the write end of the pipe (simulating piped output)


# Mock for sys.stdout with broken fileno
class MockBrokenTerminal(MockTerminal):
    def fileno(self):
        raise OSError("Broken file descriptor")


# Test cases

# Case 1: sys.stdout has fileno and is a TTY (not a daemon)
def test_stdout_is_tty(mocker: MockerFixture):
    #
    mocker.patch("os.getppid", return_value=2)
    mocker.patch("os.getsid", return_value=3)
    mocker.patch("os.getpid", return_value=4)
    mock_stdout = mocker.patch("sys.stdout", new_callable=lambda: MockTerminal())

    assert not constants.is_daemon_process()  # Should return False (not a daemon)
    mock_stdout.flush()


# Case 2: sys.stdout has fileno but is not a TTY (daemon)
def test_stdout_is_not_tty(mocker: MockerFixture):
    # this should look like a daemon
    mock_stdout = mocker.patch("sys.stdout", new_callable=lambda: MockNonTTY())

    # ensure there's the sid and pid don't match so they don't indicate a daemon
    mocker.patch("os.getsid", return_value=2)
    mocker.patch("os.getpid", return_value=4)

    assert constants.is_daemon_process()  # Should return True (daemon)
    mock_stdout.flush()

# Case 3: sys.stdout does not have fileno (LoggingProxy example) (is daemon)
def test_stdout_no_fileno_ppid_is_1(mocker: MockerFixture):
    mock_stdout = mocker.patch("sys.stdout", new_callable=lambda: MockNoFileno())
    mocker.patch("os.getppid", return_value=1)  # Parent proc ID is 1 "re-parented" to init (typical for daemons)
    mocker.patch("os.getsid", return_value=2)
    mocker.patch("os.getpid", return_value=4)  # Would match sid, but ppid is 1, so should be ignored

    assert not constants.is_daemon_process()  # Should return False (not daemon)
    mock_stdout.flush()

# Case 4: sys.stdout does not have fileno and PPID is not 1, but session ID matches process ID (is daemon)
def test_stdout_no_fileno_sid_matches_pid(mocker: MockerFixture):
    mocker.patch("sys.stdout", new_callable=lambda: MockNoFileno())
    mocker.patch("os.getppid", return_value=1234)  # Parent process is not init (not a daemon)
    mocker.patch("os.getsid", side_effect=lambda _: os.getpid())  # Session ID equals process ID

    assert constants.is_daemon_process()  # Should return True (daemon)


# Case 5: sys.stdout does not have fileno and PPID is not 1, session ID does not match (is not daemon)
def test_stdout_no_fileno_not_daemon(mocker: MockerFixture):
    mocker.patch("sys.stdout", new_callable=lambda: MockNoFileno())
    mocker.patch("os.getppid", return_value=1234)  # Not a daemon
    mocker.patch("os.getsid", return_value=1234)  # Session ID doesn't match process ID

    assert not constants.is_daemon_process()  # Should return False (not a daemon)


# Case 6: sys.stdout has fileno, but os.isatty raises an exception
def test_stdout_isatty_raises_exception(mocker: MockerFixture):
    mocker.patch("sys.stdout", new_callable=lambda: MockBrokenTerminal())

    with pytest.raises(OSError):  # Expect an OSError
        constants.is_daemon_process()


# Case 7: sys.stdout is None (edge case) (is not daemon)
def test_stdout_is_none(mocker: MockerFixture):
    mocker.patch("sys.stdout", None)  # Simulate sys.stdout being None
    mocker.patch("os.getppid", return_value=1234)  # Not a daemon
    mocker.patch("os.getsid", return_value=1234)  # Session ID doesn't match process ID

    assert not constants.is_daemon_process()  # Should return False (not a daemon)


# Case 8: sys.stdout.fileno() returns an invalid file descriptor with ppid==1
def test_stdout_invalid_fileno_ppid_is_1(mocker: MockerFixture):
    mocker.patch("sys.stdout", new_callable=lambda: MockTerminal())
    mocker.patch("os.getppid", return_value=1)  # Parent proc ID is 1 "re-parented" to init (typical for daemons)
    mocker.patch("os.getsid", return_value=2)
    mocker.patch("os.getpid", return_value=4)  # Would match sid, but ppid is 1, so should be ignored

    assert constants.is_daemon_process()


# Case 9: sys.stdout.fileno() returns an invalid file descriptor where process and session ids do not indicate daemon
def test_stdout_invalid_fileno_not_daemon(mocker: MockerFixture):
    mocker.patch("sys.stdout", new_callable=lambda: MockTTYWithInvalidDescriptor())
    mocker.patch("os.getppid", return_value=5)
    mocker.patch("os.getsid", return_value=2)
    mocker.patch("os.getpid", return_value=4)

    assert not constants.is_daemon_process()

# Case 10. sys.stdout redirected to a file (non-tty)
def test_stdout_redirected_to_file(mocker: MockerFixture, tmp_path):
    # Redirect stdout to a file (non-TTY environment)
    file_path = tmp_path / "test_output.log"
    with open(file_path, 'w') as f:
        mocker.patch("sys.stdout", f)
        assert constants.is_daemon_process()  # Should return True (daemon-like)