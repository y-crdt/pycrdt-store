from __future__ import annotations

import time
import warnings
from collections.abc import AsyncIterator, Awaitable
from logging import Logger, getLogger
from typing import Callable, Literal

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
    squash_after_inactivity_of: int | None = None
    # Deprecated: retained for backward compatibility
    document_ttl: int | None = None
    # The maximum length of the history of the documents in seconds that is kept.
    squash_history_older_than: int | None = None
    # The minimum interval in seconds between history cleanup operations.
    squash_no_more_often_than: int = 60
    _cleaned_timestamp: float | None = None
    # Interval at which checkpoints are created for efficient document loading
    checkpoint_interval: int | None = 100
    # Counter to keep track of updates since the last checkpoint
    _update_counter = 0
    # Cleanup when database size exceeds this threshold (in MB)
    cleanup_when_db_size_above: float | None = None
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

    async def _get_database_size_mb(self) -> float:
        """Get current database size in MB."""
        if self.db_path == ":memory:":
            # For in-memory databases, query the page count
            cursor = await self._db.cursor()
            await cursor.execute("PRAGMA page_count")
            row = await cursor.fetchone()
            page_count = row[0] if row is not None else 0
            await cursor.execute("PRAGMA page_size")
            row = await cursor.fetchone()
            page_size = row[0] if row is not None else 0
            return (page_count * page_size) / (1024 * 1024)
        else:
            # For file-based databases, get actual file size
            if await anyio.Path(self.db_path).exists():
                return (await anyio.Path(self.db_path).stat()).st_size / (1024 * 1024)
            return 0.0

    async def _cleanup_if_needed(self) -> bool:
        """Check if cleanup is needed and perform it if necessary.

        Returns:
            True if cleanup was performed, False otherwise.
        """
        if not self.cleanup_when_db_size_above:
            return False

        db_size = await self._get_database_size_mb()
        if db_size <= self.cleanup_when_db_size_above:
            return False

        # Perform cleanup
        cursor = await self._db.cursor()

        # Delete all checkpoints
        try:
            await cursor.execute("DELETE FROM ycheckpoints")
            await self._db.commit()
        except Exception as exc:
            self.log.exception("Failed to delete checkpoints during cleanup: %s", exc)
            return False

        db_size = await self._get_database_size_mb()
        # Add 10% buffer to avoid constant cleanup cycles
        if db_size <= self.cleanup_when_db_size_above * 0.9:
            return True

        # Get all documents ordered by oldest activity
        try:
            await cursor.execute("""
                SELECT path, last_activity, update_count FROM (
                    SELECT path,
                        MAX(timestamp) AS last_activity,
                        COUNT(*) AS update_count
                    FROM yupdates
                    GROUP BY path
                )
                ORDER BY last_activity ASC
            """)
            documents = await cursor.fetchall()
        except Exception as exc:
            self.log.exception("Unexpected error when querying yupdates for cleanup: %s", exc)
            return False

        # Squash documents from oldest to newest
        for path, last_activity, update_count in documents:
            if update_count <= 1:
                continue  # Already squashed

            try:
                await self._squash_document_history(cursor, path)
                await self._db.commit()
            except Exception as exc:
                self.log.exception("Error squashing history for path=%s: %s", path, exc)
                # Continue to next document
                continue

            db_size = await self._get_database_size_mb()
            if db_size <= self.cleanup_when_db_size_above * 0.9:
                return True

        # Finished cleanup attempts but didn't reduce size enough
        return False

    async def _squash_document_history(self, cursor, path: str) -> None:
        """Squash all updates for a given document path into a single update."""
        # Get all updates for this path
        await cursor.execute(
            "SELECT yupdate, timestamp FROM yupdates WHERE path = ? ORDER BY timestamp ASC",
            (path,),
        )
        updates = await cursor.fetchall()
        if not updates:
            return

        ydoc: Doc = Doc()
        for update, _ in updates:
            if self._decompress:
                update = self._decompress(update)
            ydoc.apply_update(update)

        squashed_update = ydoc.get_update()
        compressed_update = self._compress(squashed_update) if self._compress else squashed_update
        latest_timestamp = updates[-1][1]

        await cursor.execute("DELETE FROM yupdates WHERE path = ?", (path,))
        await cursor.execute(
            "INSERT INTO yupdates VALUES (?, ?, ?, ?)",
            (path, compressed_update, None, latest_timestamp),
        )

    async def apply_updates(self, ydoc: Doc) -> None:
        """Apply all stored updates to the YDoc.

        Arguments:
            ydoc: The YDoc on which to apply the updates.
        """
        await self._apply_checkpointed_updates(ydoc)

    async def _apply_checkpointed_updates(self, ydoc: Doc) -> None:
        """Apply the latest checkpoint (if any) and then all subsequent updates to the YDoc."""
        if self.db_initialized is None:
            raise RuntimeError("YStore not started")
        await self.db_initialized.wait()

        found_any = False
        async with self.lock:
            async with self._db:
                cursor = await self._db.cursor()

                # 1) Load latest checkpoint, if present
                await cursor.execute(
                    "SELECT checkpoint, timestamp FROM ycheckpoints WHERE path = ?",
                    (self.path,),
                )
                row = await cursor.fetchone()
                if row:
                    ckpt_blob_comp, last_ts = row
                    ckpt_blob = (
                        self._decompress(ckpt_blob_comp) if self._decompress else ckpt_blob_comp
                    )
                    ydoc.apply_update(ckpt_blob)
                    found_any = True
                else:
                    last_ts = 0.0

                # 2) Apply all updates after the checkpoint timestamp
                await cursor.execute(
                    "SELECT yupdate, metadata, timestamp "
                    "FROM yupdates "
                    "WHERE path = ? AND timestamp >= ? "
                    "ORDER BY timestamp ASC",
                    (self.path, last_ts),
                )
                for update, metadata, timestamp in await cursor.fetchall():
                    ydoc.apply_update(update)
                    found_any = True

        if not found_any:
            # no checkpoint and no updates ⇒ document doesn't exist
            raise YDocNotFound

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
        if self.squash_after_inactivity_of is None and self.document_ttl is not None:
            warnings.warn(
                "`document_ttl` is deprecated. Use `squash_after_inactivity_of` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            self.squash_after_inactivity_of = self.document_ttl
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
                            # if ycheckpoints is missing, we need a fresh schema
                            await cursor.execute(
                                "SELECT count(name) FROM sqlite_master "
                                "WHERE type='table' AND name='ycheckpoints'"
                            )
                            ckpt_exists = (await cursor.fetchone())[0]
                            if not ckpt_exists:
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
                    # checkpoint table
                    await cursor.execute(
                        "CREATE TABLE ycheckpoints ("
                        "path TEXT NOT NULL, "
                        "checkpoint BLOB NOT NULL, "
                        "timestamp REAL NOT NULL, "
                        "PRIMARY KEY(path)"
                        ")"
                    )
                    await cursor.execute(f"PRAGMA user_version = {self.version}")
                self._db = db
        else:
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
                cursor = await self._db.cursor()
                await self._cleanup_if_needed()

                # Determine which time differentials we need to query
                if self.squash_after_inactivity_of is None and self.document_ttl is not None:
                    self.squash_after_inactivity_of = self.document_ttl
                need_newest = self.squash_after_inactivity_of is not None
                need_oldest = self.squash_history_older_than is not None

                newest_diff = None
                oldest_diff = None

                if need_newest or need_oldest:
                    if need_newest and need_oldest:
                        # One query for both
                        await cursor.execute(
                            "SELECT MIN(timestamp), MAX(timestamp) FROM yupdates WHERE path = ?",
                            (self.path,),
                        )
                        row = await cursor.fetchone()
                        if row and row[0] is not None and row[1] is not None:
                            now_time = time.time()
                            oldest_diff = now_time - row[0]  # time since oldest entry
                            newest_diff = now_time - row[1]  # time since newest entry
                        else:
                            newest_diff = oldest_diff = 0
                    elif need_newest:
                        # Only TTL is enabled - query newest only
                        newest_diff = await self._get_time_differential_to_entry(
                            cursor, direction="DESC"
                        )
                    elif need_oldest:
                        # Only history length is enabled - query oldest only
                        oldest_diff = await self._get_time_differential_to_entry(
                            cursor, direction="ASC"
                        )
                # If neither is enabled, do nothing (newest_diff and oldest_diff remain None)

                ttl_exceeded = (
                    self.squash_after_inactivity_of is not None
                    and newest_diff is not None
                    and newest_diff > self.squash_after_inactivity_of
                )
                history_exceeded = (
                    self.squash_history_older_than is not None
                    and oldest_diff is not None
                    and oldest_diff > self.squash_history_older_than
                )

                now = time.time()
                if (self._cleaned_timestamp is None and (ttl_exceeded or history_exceeded)) or (
                    self._cleaned_timestamp is not None
                    and (now - self._cleaned_timestamp) > self.squash_no_more_often_than
                    and (ttl_exceeded or history_exceeded)
                ):
                    self._cleaned_timestamp = now
                    # squash updates
                    ydoc: Doc = Doc()
                    # pick cutoff: "now" = squash everything; "now - history" = prune only old tail
                    if ttl_exceeded:
                        older_than = now
                    else:  # history_exceeded
                        assert self.squash_history_older_than is not None
                        older_than = now - self.squash_history_older_than

                    await cursor.execute(
                        "SELECT yupdate FROM yupdates WHERE path = ? AND timestamp <= ?"
                        "ORDER BY timestamp ASC",
                        (self.path, older_than),
                    )
                    for (update,) in await cursor.fetchall():
                        if self._decompress:
                            update = self._decompress(update)
                            ydoc.apply_update(update)
                        # delete history
                    await cursor.execute(
                        "DELETE FROM yupdates WHERE path = ? AND timestamp <= ?",
                        (self.path, older_than),
                    )
                    # insert squashed updates
                    squashed_update = ydoc.get_update()
                    compressed_update = (
                        self._compress(squashed_update) if self._compress else squashed_update
                    )
                    metadata = await self.get_metadata()
                    await cursor.execute(
                        "INSERT INTO yupdates VALUES (?, ?, ?, ?)",
                        (self.path, compressed_update, metadata, older_than),
                    )
                    self._cleaned_timestamp = now

                # storing checkpoints
                self._update_counter += 1
                if self.checkpoint_interval and self._update_counter >= self.checkpoint_interval:
                    # load or init checkpoint
                    await cursor.execute(
                        "SELECT checkpoint, timestamp FROM ycheckpoints WHERE path = ?",
                        (self.path,),
                    )
                    row = await cursor.fetchone()
                    ydoc = Doc()
                    last_ts = 0.0
                    if row:
                        blob, last_ts = row
                        ydoc.apply_update(self._decompress(blob) if self._decompress else blob)

                    # apply all updates after last_ts
                    await cursor.execute(
                        "SELECT yupdate FROM yupdates "
                        "WHERE path = ? AND timestamp >= ? ORDER BY timestamp ASC",
                        (self.path, last_ts),
                    )
                    for (upd,) in await cursor.fetchall():
                        ydoc.apply_update(self._decompress(upd) if self._decompress else upd)

                    # write back the new checkpoint
                    new_ckpt = ydoc.get_update()
                    new_ckpt_compressed = self._compress(new_ckpt) if self._compress else new_ckpt
                    now = time.time()
                    await cursor.execute(
                        "INSERT OR REPLACE INTO ycheckpoints (path, checkpoint, timestamp) "
                        "VALUES (?, ?, ?)",
                        (self.path, new_ckpt_compressed, now),
                    )
                    self._update_counter = 0

                # finally, write this update to the DB
                metadata = await self.get_metadata()
                compressed_data = self._compress(data) if self._compress else data
                await cursor.execute(
                    "INSERT INTO yupdates VALUES (?, ?, ?, ?)",
                    (self.path, compressed_data, metadata, now),
                )

    async def _get_time_differential_to_entry(
        self, cursor, direction: Literal["ASC", "DESC"] = "DESC"
    ) -> float:
        """Get the time differential to the newest (DESC) or oldest (ASC) entry in the database."""
        await cursor.execute(
            (
                "SELECT timestamp FROM yupdates WHERE path = ? "
                f"ORDER BY timestamp {direction} LIMIT 1"
            ),
            (self.path,),
        )
        row = await cursor.fetchone()
        return (time.time() - row[0]) if row else 0
