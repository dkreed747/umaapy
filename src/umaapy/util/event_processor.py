import threading
import time
import heapq
import uuid
import os
import logging
from dataclasses import dataclass, field
from typing import Callable, Any, Dict, Union, Tuple
from abc import ABC, abstractmethod
from concurrent.futures import Future


class Command(ABC):
    """
    Base class for commands to be executed by the EventProcessor.

    Subclasses must implement the `execute` method.
    """

    @abstractmethod
    def execute(self, *args: Any, **kwargs: Any) -> Any:
        """
        Perform the command's action and return an optional result.

        :param args: Positional arguments passed through to the command.
        :param kwargs: Keyword arguments passed through to the command.
        :return: The result of executing the command.
        """
        pass


# Priority levels (lower is higher priority)
HIGH = 0
MEDIUM = 1
LOW = 2
_EXIT = 3  # internal worker-exit signal


@dataclass(order=True)
class Task:
    """
    Represents a one-off task scheduled on the EventProcessor.

    :param priority: Execution priority (lower value runs first).
    :param sequence: Insertion sequence number for tie-breaking.
    :param enqueued_time: Monotonic timestamp when enqueued.
    :param task_id: Unique identifier for tracking and cancellation.
    :param fn: The callable or Command to execute.
    :param future: Future representing the task's eventual result.
    :param args: Positional arguments for `fn`.
    :param kwargs: Keyword arguments for `fn`.
    """

    priority: int
    sequence: int
    enqueued_time: float = field(compare=False)
    task_id: str = field(compare=False)
    fn: Union[Callable[..., Any], Command] = field(compare=False)
    future: Future = field(compare=False)
    args: Tuple[Any, ...] = field(default_factory=tuple, compare=False)
    kwargs: Dict[str, Any] = field(default_factory=dict, compare=False)


@dataclass(order=True)
class RecurringTask:
    """
    Represents a recurring task schedule on the EventProcessor.

    :param next_run_time: Monotonic timestamp for next execution.
    :param interval_ms: Interval in milliseconds between runs.
    :param priority: Execution priority (lower value runs first).
    :param sequence: Insertion sequence number for tie-breaking.
    :param task_id: Unique identifier for cancellation.
    :param fn: The callable or Command to execute.
    :param args: Positional arguments for `fn`.
    :param kwargs: Keyword arguments for `fn`.
    """

    next_run_time: float
    interval_ms: int = field(compare=False)
    priority: int = field(compare=False)
    sequence: int = field(compare=False)
    task_id: str = field(compare=False)
    fn: Union[Callable[..., Any], Command] = field(compare=False)
    args: Tuple[Any, ...] = field(default_factory=tuple, compare=False)
    kwargs: Dict[str, Any] = field(default_factory=dict, compare=False)


class EventProcessor:
    """
    Singleton thread-pool and scheduler for prioritized one-off and recurring tasks.

    - One-off tasks are submitted via `submit`.
    - Recurring tasks are scheduled via `submit_recurring`.
    - Tasks return a Future for result or cancellation.
    - Background threads automatically start on initialization.
    """

    _instance = None
    _instance_lock = threading.Lock()

    def __new__(cls, *args: Any, **kwargs: Any):
        """
        Enforce singleton: subsequent calls return the same EventProcessor instance.
        """
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """
        Initialize queues, thread pool parameters, and start worker & scheduler threads.

        Reads configuration from environment variables:
          - THREAD_POOL_SIZE (default 4)
          - MAX_THREADS (default 2x pool size)
          - SCHEDULER_RESOLUTION_MS (default 1 ms)
          - ENABLE_AUTO_SCALING
          - SCALE_CHECK_INTERVAL_SEC (default 1 s)
        """
        if getattr(self, "_initialized", False):
            return
        self._initialized = True
        self._running = False
        self._start_stop_lock = threading.Lock()

        # Thread pool configuration
        self.initial_workers = int(os.getenv("THREAD_POOL_SIZE", "4"))
        self.min_workers = self.initial_workers
        self.max_workers = int(os.getenv("MAX_THREADS", str(self.min_workers * 2)))
        self.scheduler_resolution = float(os.getenv("SCHEDULER_RESOLUTION_MS", "1")) / 1000.0
        self.enable_auto_scale = os.getenv("ENABLE_AUTO_SCALING", "false").lower() in ("1", "true")
        self.scale_check_interval = float(os.getenv("SCALE_CHECK_INTERVAL_SEC", "1"))

        # Synchronization primitives
        self._queue_lock = threading.Lock()
        self._queue_cond = threading.Condition(self._queue_lock)
        self._rec_lock = threading.Lock()
        self._rec_cond = threading.Condition(self._rec_lock)

        # Task and recurring queues
        self._task_queue: list[Task] = []
        self._recurring_queue: list[RecurringTask] = []
        self._sequence = 0
        self._tasks: Dict[str, str] = {}  # task_id -> 'oneoff'|'recurring'
        self._futures: Dict[str, Future] = {}
        self._cancelled: set[str] = set()

        # Worker and scheduler threads
        self._workers: list[threading.Thread] = []
        self._scheduler_thread: threading.Thread

        self.logger = logging.getLogger(__name__)
        self.start()

    def __del__(self):
        """
        Ensure clean shutdown of threads if the processor is deleted.
        """
        self.stop(wait=False)

    def start(self) -> None:
        """
        Start worker threads and scheduler. Safe to call multiple times.
        """
        with self._start_stop_lock:
            if self._running:
                return
            self._running = True
            # Launch initial worker threads
            for _ in range(self.initial_workers):
                self._start_worker()
            # Launch scheduler thread
            self._scheduler_thread = threading.Thread(target=self._scheduler_loop, name="EventScheduler", daemon=True)
            self._scheduler_thread.start()
            self.logger.debug("EventProcessor started")

    def stop(self, wait: bool = True) -> None:
        """
        Stop accepting new tasks, signal threads to exit, and optionally wait for completion.

        :param wait: If True, block until all worker and scheduler threads finish.
        """
        with self._start_stop_lock:
            if not self._running:
                return
            self._running = False
            # Wake any waiting threads
            with self._queue_cond:
                self._queue_cond.notify_all()
            with self._rec_cond:
                self._rec_cond.notify_all()
            # Insert exit tasks for each worker
            with self._queue_cond:
                for _ in self._workers:
                    self._sequence += 1
                    exit_future = Future()
                    exit_task = Task(_EXIT, self._sequence, time.monotonic(), "", None, exit_future)
                    heapq.heappush(self._task_queue, exit_task)
                self._queue_cond.notify_all()
            if wait:
                self._scheduler_thread.join()
                for w in self._workers:
                    w.join()
            self.logger.debug("EventProcessor stopped")

    def running(self) -> bool:
        """
        Check if the processor is currently running.

        :return: True if running, False otherwise.
        """
        with self._start_stop_lock:
            return self._running

    def get_pending_task_count(self) -> int:
        """
        Get the number of one-off tasks waiting to be executed.

        :return: Count of pending tasks.
        """
        with self._queue_lock:
            return len(self._task_queue)

    def get_recurring_task_count(self) -> int:
        """
        Get the number of recurring tasks scheduled.

        :return: Count of recurring tasks.
        """
        with self._rec_lock:
            return len(self._recurring_queue)

    def submit(
        self, fn: Union[Callable[..., Any], Command], *args: Any, priority: int = MEDIUM, **kwargs: Any
    ) -> Future:
        """
        Submit a one-off task for execution.

        :param fn: Callable or Command instance to execute.
        :param args: Positional arguments for `fn`.
        :param priority: Priority of the task (LOW/MEDIUM/HIGH).
        :param kwargs: Keyword arguments for `fn`.
        :return: Future that will hold the result.
        :raises RuntimeError: If the processor is not running.
        """
        if not self.running():
            raise RuntimeError("EventProcessor not running. Call start() first.")
        task_id = uuid.uuid4().hex
        future: Future = Future()
        future._task_id = task_id
        self._futures[task_id] = future
        with self._queue_cond:
            self._sequence += 1
            task = Task(priority, self._sequence, time.monotonic(), task_id, fn, future, args, kwargs)
            heapq.heappush(self._task_queue, task)
            self._tasks[task_id] = "oneoff"
            self._queue_cond.notify()
        return future

    def submit_recurring(
        self,
        fn: Union[Callable[..., Any], Command],
        interval_ms: int,
        *args: Any,
        priority: int = MEDIUM,
        **kwargs: Any,
    ) -> str:
        """
        Schedule a recurring task at a fixed interval.

        :param fn: Callable or Command to execute each interval.
        :param interval_ms: Time between executions, in milliseconds.
        :param args: Positional arguments for `fn`.
        :param priority: Priority for the recurring invocation.
        :param kwargs: Keyword arguments for `fn`.
        :return: A task_id string for cancellation.
        :raises RuntimeError: If the processor is not running.
        """
        if not self.running():
            raise RuntimeError("EventProcessor not running. Call start() first.")
        task_id = uuid.uuid4().hex
        with self._rec_cond:
            self._sequence += 1
            next_run = time.monotonic() + interval_ms / 1000.0
            rt = RecurringTask(next_run, interval_ms, priority, self._sequence, task_id, fn, args, kwargs)
            heapq.heappush(self._recurring_queue, rt)
            self._tasks[task_id] = "recurring"
            self._rec_cond.notify()
        return task_id

    def cancel(self, task_id: str) -> bool:
        """
        Cancel a scheduled one-off or recurring task.

        :param task_id: Identifier returned from submit or submit_recurring.
        :return: True if cancellation was registered, False otherwise.
        """
        if task_id not in self._tasks:
            return False
        self._cancelled.add(task_id)
        future = self._futures.get(task_id)
        if future:
            future.cancel()
        return True

    def _start_worker(self) -> None:
        """
        Spawn a new worker thread to process tasks from the queue.
        """
        worker = threading.Thread(target=self._worker_loop, name=f"Worker-{len(self._workers)+1}", daemon=True)
        self._workers.append(worker)
        worker.start()
        self.logger.debug(f"Started worker {worker.name}")

    def _worker_loop(self) -> None:
        """
        Worker thread loop: deque tasks, run them or exit on exit signal.
        """
        while True:
            with self._queue_cond:
                while not self._task_queue and self.running():
                    self._queue_cond.wait()
                if not self.running():
                    break
                task = heapq.heappop(self._task_queue)
            if task.priority == _EXIT:
                self.logger.debug("Worker exit signal received")
                break
            if task.future.cancelled() or task.task_id in self._cancelled:
                continue
            try:
                if task.future.set_running_or_notify_cancel():
                    result = (
                        task.fn.execute(*task.args, **task.kwargs)
                        if isinstance(task.fn, Command)
                        else task.fn(*task.args, **task.kwargs)
                    )
                    task.future.set_result(result)
            except Exception:
                self.logger.exception(f"Error in task {task.task_id}")
                task.future.set_exception(Exception)

    def _scheduler_loop(self) -> None:
        """
        Scheduler thread loop: promote due recurring tasks and handle auto-scaling.
        """
        last_scale = time.monotonic()
        while self.running():
            now = time.monotonic()
            with self._rec_cond:
                # Schedule all recurring tasks whose time has arrived
                while self._recurring_queue and self._recurring_queue[0].next_run_time <= now:
                    rt = heapq.heappop(self._recurring_queue)
                    if rt.task_id not in self._cancelled:
                        with self._queue_cond:
                            self._sequence += 1
                            future = Future()
                            t = Task(rt.priority, self._sequence, now, rt.task_id, rt.fn, future, rt.args, rt.kwargs)
                            heapq.heappush(self._task_queue, t)
                            self._queue_cond.notify()
                        # Reschedule for next interval
                        rt.next_run_time = now + rt.interval_ms / 1000.0
                        heapq.heappush(self._recurring_queue, rt)
                # Compute wait time until next recurring task or resolution
                timeout = self.scheduler_resolution
                if self._recurring_queue:
                    next_due = self._recurring_queue[0].next_run_time
                    timeout = max(0.0, min(timeout, next_due - now))
                self._rec_cond.wait(timeout)
            # Auto-scale worker threads if enabled
            if self.enable_auto_scale and now - last_scale >= self.scale_check_interval:
                last_scale = now
                with self._queue_lock:
                    qlen = len(self._task_queue)
                wcount = len(self._workers)
                # Scale up
                if qlen > wcount and wcount < self.max_workers:
                    self._start_worker()
                # Scale down
                elif qlen < wcount - 1 and wcount > self.min_workers:
                    with self._queue_cond:
                        self._sequence += 1
                        exit_future = Future()
                        exit_t = Task(_EXIT, self._sequence, time.monotonic(), "", None, exit_future)
                        heapq.heappush(self._task_queue, exit_t)
                        self._queue_cond.notify()
                # Remove dead workers from list
                self._workers = [w for w in self._workers if w.is_alive()]

    @classmethod
    def reset(cls) -> None:
        """
        Tear down the current EventProcessor singleton so that the next
        EventProcessor() call returns a fresh instance.

        Stops threads, clears initialization, and resets the singleton.
        """
        with cls._instance_lock:
            inst = cls._instance
            if not inst:
                return
            try:
                inst.stop(wait=True)
            except Exception as e:
                inst.logger.warning("EventProcessor.reset: error stopping processor: %s", e)
            finally:
                if hasattr(inst, "_initialized"):
                    delattr(inst, "_initialized")
                cls._instance = None
