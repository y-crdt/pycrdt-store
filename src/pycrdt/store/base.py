from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Awaitable
from contextlib import AsyncExitStack
from functools import partial
from inspect import isawaitable
from logging import Logger
from typing import Callable, cast

from anyio import TASK_STATUS_IGNORED, Event, Lock, create_task_group
from anyio.abc import TaskGroup, TaskStatus

from pycrdt import Doc


class YDocNotFound(Exception):
    pass


class BaseYStore(ABC):
    metadata_callback: Callable[[], Awaitable[bytes] | bytes] | None = None
    version = 2
    _started: Event | None = None
    _stopped: Event | None = None
    _task_group: TaskGroup | None = None
    _public_start_lock: Lock | None = None
    _private_start_lock: Lock | None = None

    @abstractmethod
    def __init__(
        self,
        path: str,
        metadata_callback: Callable[[], Awaitable[bytes] | bytes] | None = None,
        log: Logger | None = None,
    ): ...

    @abstractmethod
    async def write(self, data: bytes) -> None: ...

    @abstractmethod
    async def read(self) -> AsyncIterator[tuple[bytes, bytes, float]]:
        if False:
            yield

    @property
    def started(self) -> Event:
        if self._started is None:
            self._started = Event()
        return self._started

    @property
    def stopped(self) -> Event:
        if self._stopped is None:
            self._stopped = Event()
        return self._stopped

    @property
    def start_lock(self) -> Lock:
        if self._public_start_lock is None:
            self._public_start_lock = Lock()
        return self._public_start_lock

    @property
    def _start_lock(self) -> Lock:
        if self._private_start_lock is None:
            self._private_start_lock = Lock()
        return self._private_start_lock

    async def __aenter__(self) -> BaseYStore:
        async with self._start_lock:
            if self._task_group is not None:
                raise RuntimeError("YStore already running")

            async with AsyncExitStack() as exit_stack:
                tg = create_task_group()
                self._task_group = await exit_stack.enter_async_context(tg)
                self._exit_stack = exit_stack.pop_all()
                await tg.start(partial(self.start, from_context_manager=True))

        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        await self.stop()
        return await self._exit_stack.__aexit__(exc_type, exc_value, exc_tb)

    async def start(
        self,
        *,
        task_status: TaskStatus[None] = TASK_STATUS_IGNORED,
        from_context_manager: bool = False,
    ):
        """Start the store.

        Arguments:
            task_status: The status to set when the task has started.
        """
        if from_context_manager:
            task_status.started()
            self.started.set()
            return

        async with self._start_lock:
            if self._task_group is not None:
                raise RuntimeError("YStore already running")

            async with create_task_group() as self._task_group:
                task_status.started()
                self.started.set()
                await self.stopped.wait()

    async def stop(self) -> None:
        """Stop the store."""
        if self._task_group is None:
            raise RuntimeError("YStore not running")

        self.stopped.set()
        self._task_group.cancel_scope.cancel()
        self._task_group = None

    async def get_metadata(self) -> bytes:
        """
        Returns:
            The metadata.
        """
        if self.metadata_callback is None:
            return b""

        metadata = self.metadata_callback()
        if isawaitable(metadata):
            metadata = await metadata
        metadata = cast(bytes, metadata)
        return metadata

    async def encode_state_as_update(self, ydoc: Doc) -> None:
        """Store a YDoc state.

        Arguments:
            ydoc: The YDoc from which to store the state.
        """
        update = ydoc.get_update()
        await self.write(update)

    async def apply_updates(self, ydoc: Doc) -> None:
        """Apply all stored updates to the YDoc.

        Arguments:
            ydoc: The YDoc on which to apply the updates.
        """
        async for update, *rest in self.read():
            ydoc.apply_update(update)
