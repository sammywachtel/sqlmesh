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
        self.read_event = threading.Event()
        self.reader_thread = threading.Thread(target=self._read_from_slave)
        self.reader_thread.daemon = True
        self.reader_thread.start()

    def fileno(self):
        return self.slave_fd  # Return valid pseudo-terminal descriptor

    def isatty(self):
        return os.isatty(self.slave_fd)

    def write(self, text):
        self.read_event.clear()
        os.write(self.master_fd, text.encode())
        return len(text)

    def flush(self):
        self.read_event.wait()
        sys.__stdout__.write(self.collected_text_buffer + "\n")

    def close(self):
        os.close(self.master_fd)
        os.close(self.slave_fd)

    def _read_from_slave(self):
        while True:
            data = os.read(self.slave_fd, 1024)
            if not data:
                break
            self.collected_text_buffer += data.decode()
            self.read_event.set()


# Mock for sys.stdout with invalid descriptor (simulating a broken fileno)
class MockBrokenTerminal(MockTerminal):
    def fileno(self):
        raise OSError("Broken file descriptor")


# Mock for sys.stdout with a pipe (non-TTY)
class MockNonTTY(MockTerminal):
    def fileno(self):
        read_fd, write_fd = os.pipe()
        return write_fd  # Simulate piped output (non-TTY)


# Mock for sys.stdout with invalid descriptor (not a TTY, but has fileno)
class MockTTYWithInvalidDescriptor(MockTerminal):
    def fileno(self):
        return -1  # Simulate an invalid file descriptor


# Test cases

# Case 1: sys.stdout has fileno and is a TTY (not a daemon)
def test_stdout_is_tty(mocker: MockerFixture):
    mocker.patch("os.getppid", return_value=2)
    mocker.patch("os.getsid", return_value=3)
    mocker.patch("os.getpid", return_value=4)
    mock_stdout = mocker.patch("sys.stdout", new_callable=lambda: MockTerminal())
    assert not constants.is_daemon_process()  # Should return False (not a daemon)
    mock_stdout.flush()


# Case 2: sys.stdout has fileno but is not a TTY (daemon-like)
def test_stdout_is_not_tty(mocker: MockerFixture):
    mock_stdout = mocker.patch("sys.stdout", new_callable=lambda: MockNonTTY())
    mocker.patch("os.getsid", return_value=2)
    mocker.patch("os.getpid", return_value=4)
    assert constants.is_daemon_process()  # Should return True (daemon-like)
    mock_stdout.flush()


# Case 3: sys.stdout does not have fileno (LoggingProxy example) (daemon-like)
def test_stdout_no_fileno_ppid_is_1(mocker: MockerFixture):
    mock_stdout = mocker.patch("sys.stdout", new_callable=lambda: MockNoFileno())
    mocker.patch("os.getppid", return_value=1)  # Parent proc ID is 1 (daemon)
    mocker.patch("os.getsid", return_value=2)
    mocker.patch("os.getpid", return_value=4)
    assert constants.is_daemon_process()  # Should return True (daemon-like)
    mock_stdout.flush()


# Case 4: sys.stdout does not have fileno and PPID is not 1, but session ID matches process ID (daemon)
def test_stdout_no_fileno_sid_matches_pid(mocker: MockerFixture):
    mocker.patch("sys.stdout", new_callable=lambda: MockNoFileno())
    mocker.patch("os.getppid", return_value=1234)
    mocker.patch("os.getsid", side_effect=lambda _: os.getpid())  # Session ID equals process ID
    assert constants.is_daemon_process()  # Should return True (daemon)


# Case 5: sys.stdout does not have fileno, PPID is not 1, and session ID does not match (assume daemon)
def test_stdout_no_fileno_daemon(mocker: MockerFixture):
    mocker.patch("sys.stdout", new_callable=lambda: MockNoFileno())
    mocker.patch("os.getppid", return_value=1234)
    mocker.patch("os.getsid", return_value=1234)  # Session ID doesn't match process ID
    assert constants.is_daemon_process()  # Should return False (assumed daemon)


# Case 6: sys.stdout has fileno, but os.isatty raises an exception (broken fileno)
def test_stdout_isatty_raises_exception(mocker: MockerFixture):
    mock_stdout = mocker.patch("sys.stdout", new_callable=lambda: MockBrokenTerminal())
    with pytest.raises(OSError):  # Expect an OSError
        constants.is_daemon_process()


# Case 7: sys.stdout is null (edge case), PPID is not 1, and session ID does not match (assume daemon)
def test_stdout_is_none(mocker: MockerFixture):
    mocker.patch("sys.stdout", None)
    mocker.patch("os.getppid", return_value=1234)
    mocker.patch("os.getsid", return_value=1234)
    assert constants.is_daemon_process()  # Should return False (not a daemon)

# Case 8: sys.stdout is None (edge case) with ppid == 1 (daemon)
def test_stdout_is_none_ppid_id_1(mocker: MockerFixture):
    mocker.patch("sys.stdout", None)
    mocker.patch("os.getppid", return_value=1)
    mocker.patch("os.getsid", return_value=1234)
    assert constants.is_daemon_process()  # Should return False (daemon)


# Case 9: sys.stdout.fileno() returns an invalid file descriptor with ppid == 1 (daemon)
def test_stdout_invalid_fileno_ppid_is_1(mocker: MockerFixture):
    mock_stdout = mocker.patch("sys.stdout", new_callable=lambda: MockTTYWithInvalidDescriptor())
    mocker.patch("os.getppid", return_value=1)
    mocker.patch("os.getsid", return_value=2)
    mocker.patch("os.getpid", return_value=4)
    assert constants.is_daemon_process()  # Should return True (daemon)


# Case 10: sys.stdout.fileno() returns an invalid file descriptor where process and session IDs do not indicate daemon
#          While it is reasonable to assume that this case would NOT indicate a daemon given the IDs, it is safer
#          to assume a daemon and prevent forking.
def test_stdout_invalid_fileno_not_daemon(mocker: MockerFixture):
    mock_stdout = mocker.patch("sys.stdout", new_callable=lambda: MockTTYWithInvalidDescriptor())
    mocker.patch("os.getppid", return_value=5)
    mocker.patch("os.getsid", return_value=2)
    mocker.patch("os.getpid", return_value=4)
    assert constants.is_daemon_process()  # Should return False (not a daemon)


# Case 11: sys.stdout redirected to a file (non-tty, daemon-like)
def test_stdout_redirected_to_file(mocker: MockerFixture, tmp_path):
    file_path = tmp_path / "test_output.log"
    with open(file_path, 'w') as f:
        mock_stdout = mocker.patch("sys.stdout", f)
        assert constants.is_daemon_process()  # Should return True (daemon-like)