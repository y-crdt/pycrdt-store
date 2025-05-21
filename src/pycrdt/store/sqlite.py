from __future__ import annotations

import time
from collections.abc import AsyncIterator, Awaitable
from logging import Logger, getLogger
from typing import Callable

import anyio
from anyio import TASK_STATUS_IGNORED, Event, Lock, create_task_group
from anyio.abc import TaskStatus
from sqlite_anyio import Connection, connect, exception_logger

from pycrdt import Doc

from .base import BaseYStore, YDocNotFound
from .utils import get_new_path


class SQLiteYStore(BaseYStore):
    """A YStore which uses an SQLite database.
    Unlike file-based YStores, the Y updates of all documents are stored in the same database.

    Subclass to point to your database file:

    ```py
    class MySQLiteYStore(SQLiteYStore):
        db_path = "path/to/my_ystore.db"
    ```
    """

    db_path: str = "ystore.db"
    # Determines the "time to live" for all documents, i.e. how recent the
    # latest update of a document must be before purging document history.
    # Defaults to never purging document history (None).
    document_ttl: int | None = None
    path: str
    lock: Lock
    db_initialized: Event | None
    _db: Connection
    # Optional callbacks for compressing and decompressing data, default: no compression
    _compress: Callable[[bytes], bytes] | None = None
    _decompress: Callable[[bytes], bytes] | None = None

    def __init__(
        self,
        path: str,
        metadata_callback: Callable[[], Awaitable[bytes] | bytes] | None = None,
        log: Logger | None = None,
    ) -> None:
        """Initialize the object.

        Arguments:
            path: The file path used to store the updates.
            metadata_callback: An optional callback to call to get the metadata.
            log: An optional logger.
        """
        self.path = path
        self.metadata_callback = metadata_callback
        self.log = log or getLogger(__name__)
        self.lock = Lock()
        self.db_initialized = None

    async def start(
        self,
        *,
        task_status: TaskStatus[None] = TASK_STATUS_IGNORED,
        from_context_manager: bool = False,
    ):
        """Start the SQLiteYStore.

        Arguments:
            task_status: The status to set when the task has started.
        """
        self.db_initialized = Event()
        if from_context_manager:
            assert self._task_group is not None
            self._task_group.start_soon(self._init_db)
            task_status.started()
            self.started.set()
            return

        async with self._start_lock:
            if self._task_group is not None:
                raise RuntimeError("YStore already running")
            async with create_task_group() as self._task_group:
                self._task_group.start_soon(self._init_db)
                task_status.started()
                self.started.set()
                await self.stopped.wait()

    async def stop(self) -> None:
        """Stop the store."""
        if self.db_initialized is not None and self.db_initialized.is_set():
            await self._db.close()
        await super().stop()

    async def _init_db(self):
        create_db = False
        move_db = False
        if not await anyio.Path(self.db_path).exists():
            create_db = True
        else:
            async with self.lock:
                db = await connect(
                    self.db_path,
                    exception_handler=exception_logger,
                    log=self.log,
                )
                async with db:
                    cursor = await db.cursor()
                    await cursor.execute(
                        "SELECT count(name) FROM sqlite_master "
                        "WHERE type='table' and name='yupdates'"
                    )
                    table_exists = (await cursor.fetchone())[0]
                    if table_exists:
                        await cursor.execute("pragma user_version")
                        version = (await cursor.fetchone())[0]
                        if version != self.version:
                            move_db = True
                            create_db = True
                    else:
                        create_db = True
                await db.close()
        if move_db:
            new_path = await get_new_path(self.db_path)
            self.log.warning("YStore version mismatch, moving %s to %s", self.db_path, new_path)
            await anyio.Path(self.db_path).rename(new_path)
        if create_db:
            async with self.lock:
                db = await connect(
                    self.db_path,
                    exception_handler=exception_logger,
                    log=self.log,
                )
                async with db:
                    cursor = await db.cursor()
                    # enable full auto-vacuum & rebuild now
                    await cursor.execute("PRAGMA auto_vacuum = FULL")
                    await cursor.execute("VACUUM")
                    await cursor.execute(
                        "CREATE TABLE yupdates (path TEXT NOT NULL, yupdate BLOB, "
                        "metadata BLOB, timestamp REAL NOT NULL)"
                    )
                    await cursor.execute(
                        "CREATE INDEX idx_yupdates_path_timestamp ON yupdates (path, timestamp)"
                    )
                    await cursor.execute(f"PRAGMA user_version = {self.version}")
                await db.close()
        self._db = await connect(
            self.db_path,
            exception_handler=exception_logger,
            log=self.log,
        )
        assert self.db_initialized is not None
        self.db_initialized.set()

    def register_compression_callbacks(
        self, compress: Callable[[bytes], bytes], decompress: Callable[[bytes], bytes]
    ) -> None:
        if not callable(compress) or not callable(decompress):
            raise TypeError("Both compress and decompress must be callable.")
        self._compress = compress
        self._decompress = decompress

    async def read(self) -> AsyncIterator[tuple[bytes, bytes, float]]:
        """Async iterator for reading the store content.

        Returns:
            A tuple of (update, metadata, timestamp) for each update.
        """
        if self.db_initialized is None:
            raise RuntimeError("YStore not started")
        await self.db_initialized.wait()
        try:
            async with self.lock:
                found = False
                async with self._db:
                    cursor = await self._db.cursor()
                    await cursor.execute(
                        "SELECT yupdate, metadata, timestamp FROM yupdates WHERE path = ?",
                        (self.path,),
                    )
                    for update, metadata, timestamp in await cursor.fetchall():
                        if self._decompress:
                            try:
                                update = self._decompress(update)
                            except Exception:
                                pass
                        found = True
                        yield update, metadata, timestamp
                if not found:
                    raise YDocNotFound
        except Exception:
            raise YDocNotFound

    async def write(self, data: bytes) -> None:
        """Store an update.

        Arguments:
            data: The update to store.
        """
        if self.db_initialized is None:
            raise RuntimeError("YStore not started")
        await self.db_initialized.wait()
        async with self.lock:
            async with self._db:
                # first, determine time elapsed since last update
                cursor = await self._db.cursor()
                await cursor.execute(
                    "SELECT timestamp FROM yupdates WHERE path = ? "
                    "ORDER BY timestamp DESC LIMIT 1",
                    (self.path,),
                )
                row = await cursor.fetchone()
                diff = (time.time() - row[0]) if row else 0

                if self.document_ttl is not None and diff > self.document_ttl:
                    # squash updates
                    ydoc: Doc = Doc()
                    await cursor.execute(
                        "SELECT yupdate FROM yupdates WHERE path = ?",
                        (self.path,),
                    )
                    for (update,) in await cursor.fetchall():
                        ydoc.apply_update(update)
                    # delete history
                    await cursor.execute("DELETE FROM yupdates WHERE path = ?", (self.path,))
                    # insert squashed updates
                    squashed_update = ydoc.get_update()
                    compressed_update = (
                        self._compress(squashed_update) if self._compress else squashed_update
                    )
                    metadata = await self.get_metadata()
                    await cursor.execute(
                        "INSERT INTO yupdates VALUES (?, ?, ?, ?)",
                        (self.path, compressed_update, metadata, time.time()),
                    )

                # finally, write this update to the DB
                metadata = await self.get_metadata()
                compressed_data = self._compress(data) if self._compress else data
                await cursor.execute(
                    "INSERT INTO yupdates VALUES (?, ?, ?, ?)",
                    (self.path, compressed_data, metadata, time.time()),
                )
