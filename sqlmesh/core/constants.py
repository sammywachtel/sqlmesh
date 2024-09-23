from __future__ import annotations

import datetime
import os
import typing as t
from pathlib import Path

from sqlmesh.utils.system import is_daemon_process

SQLMESH = "sqlmesh"
SQLMESH_PATH = Path.home() / ".sqlmesh"

PROD = "prod"
"""Prod"""
DEV = "dev"
"""Dev"""

SNAPSHOTS_PATH = "snapshots"
"""Snapshots path"""
DEFAULT_SNAPSHOT_TTL = "in 1 week"
"""Default snapshot TTL"""
DEFAULT_ENVIRONMENT_TTL = "in 1 week"
"""Default environment TTL"""
IGNORE_PATTERNS = [
    ".ipynb_checkpoints/*",
]
"""Ignore patterns"""
DATA_VERSION_LIMIT = 10
"""Data version limit"""
DEFAULT_TIME_COLUMN_FORMAT = "%Y-%m-%d"
"""Default time column format"""
MAX_MODEL_DEFINITION_SIZE = 10000
"""Maximum number of characters in a model definition"""


def is_daemon_process() -> bool:
    """
    Determines if the current process is running as a daemon.

    Returns:
        bool:
            - True: if the process has traits common to daemons, such as:
                - Being re-parented to `init` (PPID == 1).
                - Being the leader of its own session (session ID == process ID).
                - Not being connected to a terminal (for cases where `sys.stdout` has a fileno).
            - False: if the process is running interactively or in non-interactive environments
              (e.g., CI/CD, background jobs, or pipelines), but is not a daemon.

    Logic:
        1. **Parent Process Check (PPID)**:
            - If the process has been re-parented to `init` (PPID == 1), it is likely a daemon.
        2. **Session ID Check**:
            - If the process is the leader of its own session (`os.getsid(0) == os.getpid()`),
              it is likely a daemon. NOTE: Pass zero to getsid(0) to get the session id of the current process.
        3. **TTY Check**:
            - If `sys.stdout` has a `fileno()` method, check whether the process is connected
              to a terminal using `os.isatty()`. If not connected to a terminal, it might be a daemon.

    Example:
        if is_daemon_process():
            print("Running as daemon")
        else:
            print("Not a daemon")

    Edge cases:
        - Accounts for non-interactive environments like background jobs, pipelines, or containers
          without falsely classifying them as daemons.
        - May still return True for non-daemon session leaders that exhibit similar traits to daemons.
    """
    # Print the type of sys.stdout
    print(f"DEBUGGING: Type of sys.stdout: {type(sys.stdout)}")

    # Print the class of sys.stdout
    print(f"DEBUGGING: Class of sys.stdout: {sys.stdout.__class__}")

    # Print all available methods and attributes of sys.stdout
    print(f"DEBUGGING: Attributes and methods of sys.stdout: {dir(sys.stdout)}")

    # Check if sys.stdout has a fileno method
    has_fileno = hasattr(sys.stdout, 'fileno')
    print(f"DEBUGGING: Does sys.stdout have a fileno method: {has_fileno}")

    # If fileno method exists, print the fileno value
    if has_fileno:
        print(f"DEBUGGING: sys.stdout.fileno(): {sys.stdout.fileno()}")

    # Check if the process has been re-parented to init (PPID == 1) or has started its own session
    print(f"DEBUGGING: os.getppid(): {os.getppid()}")
    print(f"DEBUGGING: os.getsid(): {os.getsid(0)}")
    print(f"DEBUGGING: os.getpid(): {os.getpid()}")
    if os.getppid() == 1 or os.getsid(0) == os.getpid():
        # print("DEBUGGING: os.getppid() == 1 OR os.getsid() and os.getsid() match")
        return True

    # If sys.stdout has a fileno, check for interactive terminal connection
    if hasattr(sys.stdout, "fileno"):
        try:
            return not os.isatty(sys.stdout.fileno())
        except OSError:
            # If we can't determine fileno, assume daemon (could be logging proxy or similar)
            return True

    # Non-daemon, likely running in a non-interactive, piped, or background job environment
    return False


if hasattr(os, "fork") and not is_daemon_process():
    try:
        MAX_FORK_WORKERS: t.Optional[int] = int(os.getenv("MAX_FORK_WORKERS"))  # type: ignore
        print(f"DEBUGGING: can fork -- Maximum number of fork workers: {MAX_FORK_WORKERS}")
    except TypeError:
        MAX_FORK_WORKERS = (
            len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None  # type: ignore
        )
        print(f"DEBUGGING: in except -- Maximum number of fork workers: {MAX_FORK_WORKERS}")
else:
    MAX_FORK_WORKERS = 1
    print("DEBUGGING: in a daemon, can not fork -- Maximum number of fork workers: 1")

EPOCH = datetime.date(1970, 1, 1)

DEFAULT_MAX_LIMIT = 1000
"""The default maximum row limit that is used when evaluating a model."""

DEFAULT_LOG_LIMIT = 20
"""The default number of logs to keep."""

DEFAULT_LOG_FILE_DIR = "logs"
"""The default directory for log files."""

AUDITS = "audits"
CACHE = ".cache"
EXTERNAL_MODELS = "external_models"
MACROS = "macros"
MATERIALIZATIONS = "materializations"
METRICS = "metrics"
MODELS = "models"
SEEDS = "seeds"
SIGNALS = "signals"
TESTS = "tests"

EXTERNAL_MODELS_YAML = "external_models.yaml"
EXTERNAL_MODELS_DEPRECATED_YAML = "schema.yaml"

DEFAULT_SCHEMA = "default"

SQLMESH_VARS = "__sqlmesh__vars__"
VAR = "var"
GATEWAY = "gateway"

SQLMESH_MACRO = "__sqlmesh__macro__"
SQLMESH_BUILTIN = "__sqlmesh__builtin__"
SQLMESH_METADATA = "__sqlmesh__metadata__"


BUILTIN = "builtin"
AIRFLOW = "airflow"
DBT = "dbt"
NATIVE = "native"
HYBRID = "hybrid"
