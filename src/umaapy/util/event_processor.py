import threading
import time
import heapq
import uuid
import os
import logging
from dataclasses import dataclass, field
from typing import Callable, Any, Dict, Union
from abc import ABC, abstractmethod
from concurrent.futures import Future


# Command interface
class Command(ABC):
    """Base class for commands to be executed by the EventProcessor."""

    @abstractmethod
    def execute(self) -> Any:
        """Perform the command's action and return an optional result."""
        pass


# Priority levels
HIGH = 0
MEDIUM = 1
LOW = 2
_EXIT = 3  # internal worker-exit signal


@dataclass(order=True)
class Task:
    priority: int
    sequence: int
    enqueued_time: float = field(compare=False)
    task_id: str = field(compare=False)
    fn: Union[Callable[..., Any], Command] = field(compare=False)
    future: Future = field(compare=False)


@dataclass(order=True)
class RecurringTask:
    next_run_time: float
    interval_ms: int = field(compare=False)
    priority: int = field(compare=False)
    sequence: int = field(compare=False)
    task_id: str = field(compare=False)
    fn: Union[Callable[..., Any], Command] = field(compare=False)


class EventProcessor:
    """
    Singleton thread-pool and scheduler for prioritized one-off and recurring tasks.
    """

    _instance = None
    _instance_lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        # Enforce singleton design - subsequent calls to EventProcessor() will just return the same instance :)
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if getattr(self, "_initialized", False):
            return
        self._initialized = True
        self._running = False
        self._start_stop_lock = threading.Lock()

        # Configuration
        self.initial_workers = int(os.getenv("THREAD_POOL_SIZE", str(4)))
        self.min_workers = self.initial_workers
        self.max_workers = int(os.getenv("MAX_THREADS", str(self.min_workers * 2)))
        self.scheduler_resolution = float(os.getenv("SCHEDULER_RESOLUTION_MS", "1")) / 1000.0
        self.enable_auto_scale = os.getenv("ENABLE_AUTO_SCALING", "false").lower() in ("1", "true")
        self.scale_check_interval = float(os.getenv("SCALE_CHECK_INTERVAL_SEC", "1"))

        # Locks & conditions
        self._queue_lock = threading.Lock()
        self._queue_cond = threading.Condition(self._queue_lock)
        self._rec_lock = threading.Lock()
        self._rec_cond = threading.Condition(self._rec_lock)

        # Task queues
        self._task_queue: list[Task] = []
        self._recurring_queue: list[RecurringTask] = []
        self._sequence = 0
        self._tasks: Dict[str, str] = {}  # task_id -> 'oneoff'|'recurring'
        self._futures: Dict[str, Future] = {}
        self._cancelled: set[str] = set()

        # Threads
        self._workers: list[threading.Thread] = []
        self._scheduler_thread: threading.Thread

        self.logger = logging.getLogger(__name__)
        self.start()

    def __del__(self):
        # Just in case
        self.stop(wait=False)

    def start(self):
        """Start the worker pool and scheduler. Idempotent."""
        with self._start_stop_lock:
            if self._running:
                return
            self._running = True
            # Start workers
            for _ in range(self.initial_workers):
                self._start_worker()
            # Start scheduler
            self._scheduler_thread = threading.Thread(target=self._scheduler_loop, name="EventScheduler", daemon=True)
            self._scheduler_thread.start()
            self.logger.debug("EventProcessor started")

    def stop(self, wait: bool = True):
        """Stop processing: no new tasks, signal threads to exit, and optionally wait."""
        with self._start_stop_lock:
            if not self._running:
                return
            self._running = False
            # Wake up waiting threads
            with self._queue_cond:
                self._queue_cond.notify_all()
            with self._rec_cond:
                self._rec_cond.notify_all()
            # Send exit signals to workers
            with self._queue_cond:
                for _ in self._workers:
                    self._sequence += 1
                    exit_future = Future()
                    exit_task = Task(_EXIT, self._sequence, time.monotonic(), "", None, exit_future)
                    heapq.heappush(self._task_queue, exit_task)
                self._queue_cond.notify_all()
            if wait:
                # Wait for scheduler
                self._scheduler_thread.join()
                # Wait for workers
                for w in self._workers:
                    w.join()
            self.logger.debug("EventProcessor stopped")

    def running(self) -> bool:
        """Return True if the processor is currently running."""
        with self._start_stop_lock:
            return self._running

    def get_pending_task_count(self) -> int:
        """Return the current number of pending one-off tasks in the queue."""
        with self._queue_lock:
            return len(self._task_queue)

    def get_recurring_task_count(self) -> int:
        """Return the current number of scheduled recurring tasks."""
        with self._rec_lock:
            return len(self._recurring_queue)

    def submit(self, fn: Union[Callable[..., Any], Command], priority: int = MEDIUM) -> Future:
        """
        Submit a one-off task. Returns a Future.
        Must be running.
        """
        if not self.running():
            raise RuntimeError("EventProcessor not running. Call start() first.")
        task_id = uuid.uuid4().hex
        future: Future = Future()
        future._task_id = task_id
        self._futures[task_id] = future
        with self._queue_cond:
            self._sequence += 1
            task = Task(priority, self._sequence, time.monotonic(), task_id, fn, future)
            heapq.heappush(self._task_queue, task)
            self._tasks[task_id] = "oneoff"
            self._queue_cond.notify()
        return future

    def submit_recurring(self, fn: Union[Callable[..., Any], Command], interval_ms: int, priority: int = MEDIUM) -> str:
        """
        Schedule a recurring task. Returns a task_id for cancellation.
        Must be running.
        """
        if not self.running():
            raise RuntimeError("EventProcessor not running. Call start() first.")
        task_id = uuid.uuid4().hex
        with self._rec_cond:
            self._sequence += 1
            next_run = time.monotonic() + interval_ms / 1000.0
            rt = RecurringTask(next_run, interval_ms, priority, self._sequence, task_id, fn)
            heapq.heappush(self._recurring_queue, rt)
            self._tasks[task_id] = "recurring"
            self._rec_cond.notify()
        return task_id

    def cancel(self, task_id: str) -> bool:
        """
        Cancel a one-off or recurring task. Cancels future for one-offs.
        """
        if task_id not in self._tasks:
            return False
        self._cancelled.add(task_id)
        future = self._futures.get(task_id)
        if future:
            future.cancel()
        return True

    def _start_worker(self):
        worker = threading.Thread(target=self._worker_loop, name=f"Worker-{len(self._workers)+1}", daemon=True)
        self._workers.append(worker)
        worker.start()
        self.logger.debug(f"Started worker {worker.name}")

    def _worker_loop(self):
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
                    result = task.fn.execute() if isinstance(task.fn, Command) else task.fn()
                    task.future.set_result(result)
            except Exception:
                self.logger.exception(f"Error in task {task.task_id}")
                task.future.set_exception(Exception)

    def _scheduler_loop(self):
        last_scale = time.monotonic()
        while self.running():
            now = time.monotonic()
            with self._rec_cond:
                while self._recurring_queue and self._recurring_queue[0].next_run_time <= now:
                    rt = heapq.heappop(self._recurring_queue)
                    if rt.task_id not in self._cancelled:
                        with self._queue_cond:
                            self._sequence += 1
                            future = Future()
                            t = Task(rt.priority, self._sequence, now, rt.task_id, rt.fn, future)
                            heapq.heappush(self._task_queue, t)
                            self._queue_cond.notify()
                        rt.next_run_time = now + rt.interval_ms / 1000.0
                        heapq.heappush(self._recurring_queue, rt)
                timeout = self.scheduler_resolution
                if self._recurring_queue:
                    next_due = self._recurring_queue[0].next_run_time
                    timeout = max(0.0, min(timeout, next_due - now))
                self._rec_cond.wait(timeout)
            # Auto-scaling if enabled
            if self.enable_auto_scale and now - last_scale >= self.scale_check_interval:
                last_scale = now
                with self._queue_lock:
                    qlen = len(self._task_queue)
                wcount = len(self._workers)
                if qlen > wcount and wcount < self.max_workers:
                    self._start_worker()
                elif qlen < wcount - 1 and wcount > self.min_workers:
                    with self._queue_cond:
                        self._sequence += 1
                        exit_future = Future()
                        exit_t = Task(_EXIT, self._sequence, time.monotonic(), "", None, exit_future)
                        heapq.heappush(self._task_queue, exit_t)
                        self._queue_cond.notify()
                self._workers = [w for w in self._workers if w.is_alive()]
