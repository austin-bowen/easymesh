import signal
from dataclasses import dataclass
from subprocess import Popen, TimeoutExpired
from typing import Any


class ProcessManager:
    processes: list[Popen]
    """Processes managed by this manager."""

    timeout: float | None
    """Default timeout for stopping processes."""

    def __init__(
            self,
            python_exe: str = 'python',
            options: dict[str, Any] = None,
            timeout: float | None = 10.,
    ):
        """
        ProcessManager makes it easy to start and stop numerous processes
        in a coordinated fashion.

        Example:

            with ProcessManager() as pm:
                # Run: ls -l
                pm.popen(['ls', '-l'])

                pm.start_python('my_script.py', arg1='value1')
                pm.start_python('my_other_script.py', arg2='value2')

        Args:
            python_exe:
                Path to the Python executable to use for starting Python processes.
            options:
                Default kwargs to pass to Popen.
            timeout:
                Default timeout for stopping processes.
        """

        self.python_exe = python_exe
        self.timeout = timeout

        self.processes: list[Popen] = []
        self._options: dict[str, Any] = options or {}

    @property
    def options(self) -> dict[str, Any]:
        return dict(self._options)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def using_options(self, **options) -> '_OptionsContext':
        """
        Use with a ``with`` statement to temporarily use different default
        options for Popen. Useful when you want to run a bunch of processes
        with a different set of options.

        Example:

            with ProcessManager(shell=False) as pm:
                pm.popen(['ls', '-l'])

                with pm.using_options(shell=True):
                    pm.popen('ls -l')
                    pm.popen('cp foo bar')
                    pm.popen('cat bar')

        Args:
            **options:
                New default options to use for Popen.

        Returns: A context manager.
        """

        return _OptionsContext(self, options)

    def popen(self, args, **kwargs) -> Popen:
        """
        Start a new process.

        Args:
            args: Command arguments passed to Popen.
            **kwargs: Keyword arguments passed to Popen.

        Returns: The Popen object for the new process.
        """

        kwargs = {**self._options, **kwargs}
        return self.add(Popen(args, **kwargs))

    def add(self, process: Popen) -> Popen:
        """Add an existing process to the manager."""
        self.processes.append(process)
        return process

    def start_python(self, args, **kwargs) -> Popen:
        """
        Start a new Python process.

        Args:
            args: Command arguments appended to the Python exe and passed to Popen.
            **kwargs: Keyword arguments passed to Popen.

        Returns: The Popen object for the new process.
        """

        if isinstance(args, str):
            args = f'"{self.python_exe}" {args}'
        else:
            args = [self.python_exe] + list(args)

        return self.popen(args, **kwargs)

    def stop(
            self,
            process: Popen | None = None,
            timeout: float | None = None,
    ):
        """
        Stop one or all processes managed by this manager.

        First it sends a `SIGTERM` signal to the process, and then waits for it to
        finish. If the process does not finish within the timeout, it sends a
        `SIGKILL` signal to forcefully terminate the process.

        Args:
            process:
                Optional process to stop. By default, all processes will be stopped.
            timeout:
                Optional timeout override. Defaults to `self.timeout`.
        """

        if process is None:
            processes = list(self.processes)
            self.processes.clear()
        else:
            processes = [process]
            self.processes.remove(process)

        if timeout is None:
            timeout = self.timeout

        print(f'Stopping {len(processes)} processes...')
        for process in processes:
            process.send_signal(signal.SIGTERM)

        procs_to_kill = []
        for process in processes:
            try:
                process.wait(timeout)
            except TimeoutExpired:
                print(f'Process {process.pid} did not stop in time.')
                procs_to_kill.append(process)

        for process in procs_to_kill:
            print(f'Killing process {process.pid}')
            process.kill()

        for process in procs_to_kill:
            try:
                process.wait(timeout)
            except TimeoutExpired:
                print(f'Failed to kill process {process.pid}.')

    def wait(self, timeout: float = None) -> None:
        """
        Wait for all processes to finish.

        Args:
            timeout: Optional wait timeout.
        """

        for process in list(self.processes):
            process.wait(timeout)
            self.processes.remove(process)


@dataclass
class _OptionsContext:
    procman: ProcessManager
    options: dict[str, Any]
    prev_options: dict[str, Any] = None

    def __enter__(self):
        self.prev_options = self.procman._options
        self.procman._options = self.options
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.procman._options = self.prev_options
