import platform
import tempfile
import time
import zlib
from pathlib import Path
from unittest.mock import patch

import pytest
from anyio import create_task_group, sleep
from sqlite_anyio import connect
from utils import StartStopContextManager, YDocTest

from pycrdt.store import SQLiteYStore, TempFileYStore

pytestmark = pytest.mark.anyio

IS_MAC = platform.system() == "Darwin"
MY_SQLITE_YSTORE_DB_PATH = str(Path(tempfile.mkdtemp(prefix="test_sql_")) / "ystore.db")


class MetadataCallback:
    def __init__(self):
        self.i = 0

    async def __call__(self):
        res = str(self.i).encode()
        self.i += 1
        return res


class MyTempFileYStore(TempFileYStore):
    prefix_dir = "test_temp_"

    def __init__(self, *args, delete=False, **kwargs):
        super().__init__(*args, **kwargs)
        if delete:
            Path(self.path).unlink(missing_ok=True)


class MySQLiteYStore(SQLiteYStore):
    db_path = MY_SQLITE_YSTORE_DB_PATH

    def __init__(
        self,
        *args,
        delete=False,
        checkpoint_interval=None,
        squash_after_inactivity_of=1000,
        squash_history_older_than=None,
        squash_no_more_often_than=None,
        cleanup_when_db_size_above=None,
        **kwargs,
    ):
        self.checkpoint_interval = checkpoint_interval
        self.squash_after_inactivity_of = squash_after_inactivity_of
        if delete:
            Path(self.db_path).unlink(missing_ok=True)
        if squash_history_older_than:
            self.squash_history_older_than = squash_history_older_than
        if squash_no_more_often_than:
            self.squash_no_more_often_than = squash_no_more_often_than
        if cleanup_when_db_size_above:
            self.cleanup_when_db_size_above = cleanup_when_db_size_above
        super().__init__(*args, **kwargs)


@pytest.mark.parametrize("YStore", (MyTempFileYStore, MySQLiteYStore))
@pytest.mark.parametrize("ystore_api", ("ystore_context_manager", "ystore_start_stop"))
async def test_ystore(YStore, ystore_api):
    async with create_task_group() as tg:
        store_name = f"my_store_with_api_{ystore_api}"
        ystore = YStore(store_name, metadata_callback=MetadataCallback(), delete=True)
        if ystore_api == "ystore_start_stop":
            ystore = StartStopContextManager(ystore, tg)

        async with ystore as ystore:
            data = [b"foo", b"bar", b"baz"]
            for d in data:
                await ystore.write(d)

            if YStore == MyTempFileYStore:
                assert (Path(MyTempFileYStore.base_dir) / store_name).exists()
            elif YStore == MySQLiteYStore:
                assert Path(MySQLiteYStore.db_path).exists()
            i = 0
            async for d, m, t in ystore.read():
                assert d == data[i]  # data
                assert m == str(i).encode()  # metadata
                i += 1

            assert i == len(data)


@pytest.mark.parametrize("ystore_api", ("ystore_context_manager", "ystore_start_stop"))
async def test_squash_after_inactivity_of_sqlite_ystore(ystore_api):
    async with create_task_group() as tg:
        test_ydoc = YDocTest()
        store_name = f"my_store_with_api_{ystore_api}"
        ystore = MySQLiteYStore(store_name, delete=True)
        if ystore_api == "ystore_start_stop":
            ystore = StartStopContextManager(ystore, tg)

        async with ystore as ystore:
            now = time.time()
            db = await connect(ystore.db_path)
            cursor = await db.cursor()

            for i in range(3):
                # assert that adding a record before document TTL doesn't delete document history
                with patch("time.time") as mock_time:
                    mock_time.return_value = now
                    await ystore.write(test_ydoc.update())
                    assert (
                        await (await cursor.execute("SELECT count(*) FROM yupdates")).fetchone()
                    )[0] == i + 1

            # assert that adding a record after document TTL deletes previous document history
            with patch("time.time") as mock_time:
                mock_time.return_value = now + ystore.squash_after_inactivity_of + 1
                await ystore.write(test_ydoc.update())
                # two updates in DB: one squashed update and the new update
                assert (await (await cursor.execute("SELECT count(*) FROM yupdates")).fetchone())[
                    0
                ] == 2

            await db.close()


@pytest.mark.parametrize("ystore_api", ("ystore_context_manager", "ystore_start_stop"))
async def test_squash_after_inactivity_of_reduces_file_size(ystore_api):
    async with create_task_group() as tg:
        test_ydoc = YDocTest()
        store_name = f"size_test_store_{ystore_api}"
        ystore = MySQLiteYStore(store_name, delete=True)
        if ystore_api == "ystore_start_stop":
            ystore = StartStopContextManager(ystore, tg)

        async with ystore as ystore:
            now = time.time()
            db_path = ystore.db_path
            # 1) tweak page size to 512 Bytes so the file grows in small increments
            db = await connect(db_path)
            async with db:
                cursor = await db.cursor()
                await cursor.execute("PRAGMA page_size = 512;")
                await cursor.execute("VACUUM;")
                await db.commit()

            for _ in range(10):
                with patch("time.time") as mock_time:
                    mock_time.return_value = now
                    await ystore.write(test_ydoc.update())
            size_before = Path(db_path).stat().st_size

            with patch("time.time") as mock_time:
                mock_time.return_value = now + ystore.squash_after_inactivity_of + 1
                await ystore.write(test_ydoc.update())

            # Allow some time for vacuum to complete
            await sleep(0.1)

            size_after = Path(db_path).stat().st_size

            assert size_after < size_before, (
                f"Expected size_after < size_before but got {size_before} -> {size_after}"
            )

            await db.close()


@pytest.mark.parametrize("ystore_api", ("ystore_context_manager", "ystore_start_stop"))
async def test_history_pruning_with_cleanup_interval(ystore_api):
    async with create_task_group() as tg:
        test_ydoc = YDocTest()
        store_name = f"store_{ystore_api}_prune_interval"
        # squash_history_older_than = 3s, squash_no_more_often_than = 1s
        ystore = MySQLiteYStore(
            store_name,
            delete=True,
            squash_history_older_than=3,
            squash_no_more_often_than=1,
        )
        if ystore_api == "ystore_start_stop":
            ystore = StartStopContextManager(ystore, tg)

        async with ystore as ystore:
            db = await connect(ystore.db_path)
            cursor = await db.cursor()

            base = time.time()

            # Write at t = base and t = base + 1 & + 2
            for offset in (0, 1, 2):
                with patch("time.time", return_value=base + offset):
                    await ystore.write(test_ydoc.update())

            # All entries are still within squash_history_older_than window
            count = (await (await cursor.execute("SELECT count(*) FROM yupdates")).fetchone())[0]
            assert count == 3

            # Now advance to t = base + 6
            # oldest_diff = 6s > squash_history_older_than i.e 3s → triggers prune
            with patch("time.time", return_value=base + 6):
                await ystore.write(test_ydoc.update())

            # After pruning, only entries ≥ (current_time − squash_history_older_than)
            # i.e., timestamps ≥ (6 − 3) = 3 remain.
            # We had writes at t=0,1,2 (all < 3), t=6 and squashed update survives
            final_count = (
                await (await cursor.execute("SELECT count(*) FROM yupdates")).fetchone()
            )[0]
            assert final_count == 2, f"Expected 2 entry after pruning, got {final_count}"

            await db.close()


@pytest.mark.parametrize("YStore", (MyTempFileYStore, MySQLiteYStore))
@pytest.mark.parametrize("ystore_api", ("ystore_context_manager", "ystore_start_stop"))
async def test_version(YStore, ystore_api, caplog):
    async with create_task_group() as tg:
        store_name = f"my_store_with_api_{ystore_api}"
        prev_version = YStore.version
        YStore.version = -1
        ystore = YStore(store_name)
        if ystore_api == "ystore_start_stop":
            ystore = StartStopContextManager(ystore, tg)

        async with ystore as ystore:
            await ystore.write(b"foo")
            assert "YStore version mismatch" in caplog.text

        YStore.version = prev_version
        async with ystore as ystore:
            await ystore.write(b"bar")


@pytest.mark.parametrize("ystore_api", ("ystore_context_manager", "ystore_start_stop"))
async def test_in_memory_sqlite_ystore_persistence(ystore_api):
    """
    Test that an in-memory SQLiteYStore properly persists tables and data
    throughout its lifetime.
    """

    class InMemorySQLiteYStore(SQLiteYStore):
        db_path = ":memory:"  # Use in-memory database
        squash_after_inactivity_of = None

    async with create_task_group() as tg:
        store_name = f"in_memory_test_store_with_api_{ystore_api}"
        ystore = InMemorySQLiteYStore(store_name)
        if ystore_api == "ystore_start_stop":
            ystore = StartStopContextManager(ystore, tg)

        async with ystore as ystore:
            test_data = [b"data1", b"data2", b"data3"]
            for data in test_data:
                await ystore.write(data)

            read_data = []
            async for update, _, _ in ystore.read():
                read_data.append(update)

            # Assert that all data we wrote is present
            assert read_data == test_data


@pytest.mark.parametrize("ystore_api", ("ystore_context_manager", "ystore_start_stop"))
async def test_compression_callbacks_zlib(ystore_api):
    """
    Verify that registering zlib.compress as a compression callback
    correctly round-trips data through the SQLiteYStore.
    """
    async with create_task_group() as tg:
        store_name = f"compress_test_with_api_{ystore_api}"
        ystore = MySQLiteYStore(store_name, metadata_callback=MetadataCallback(), delete=True)
        if ystore_api == "ystore_start_stop":
            ystore = StartStopContextManager(ystore, tg)

        async with ystore as ystore:
            # register zlib compression and no-op decompression
            ystore.register_compression_callbacks(zlib.compress, lambda x: x)

            data = [b"alpha", b"beta", b"gamma"]
            # write compressed
            for d in data:
                await ystore.write(d)

            assert Path(MySQLiteYStore.db_path).exists()

            # read back and ensure correct decompression
            i = 0
            async for d_read, m, t in ystore.read():
                assert zlib.decompress(d_read) == data[i]
                assert m == str(i).encode()
                i += 1

            assert i == len(data)


# Mac runners are so much faster on CI for some reason...
_MUL = 10 if IS_MAC else 1


@pytest.mark.parametrize(
    "test_case",
    (
        # expect it to be no slower than no checkpointing for small number of updates
        pytest.param(
            dict(
                number_of_updates=100 * _MUL,
                read_speedup=1,
                write_speedup=1,
                checkpointing_interval=10,
            ),
            id="non-inferiority-for-small-sizes",
        ),
        # expect it to be at least twice as fast for a larger number of updates
        pytest.param(
            dict(
                number_of_updates=1000 * _MUL,
                read_speedup=2,
                write_speedup=1,
                checkpointing_interval=100,
            ),
            id="superiority-for-larger-sizes",
        ),
    ),
)
@pytest.mark.parametrize("ystore_api", ("ystore_context_manager", "ystore_start_stop"))
@pytest.mark.flaky(reruns=2)
async def test_sqlite_ystore_checkpoint_loading(ystore_api, test_case):
    store_name = "checkpoint_test_store"
    number_of_updates = test_case["number_of_updates"]

    # measure with checkpointing
    ystore = MySQLiteYStore(
        store_name,
        delete=True,
        checkpoint_interval=test_case["checkpointing_interval"],
        squash_after_inactivity_of=None,
    )
    ydoc = YDocTest()
    async with create_task_group() as tg:
        if ystore_api == "ystore_start_stop":
            ystore = StartStopContextManager(ystore, tg)

        async with ystore as ystore:
            t0 = time.perf_counter()
            for _ in range(number_of_updates):
                update = ydoc.update()
                await ystore.write(update)
            t1 = time.perf_counter()
            write_time_checkpointed = t1 - t0

            # Restore document
            ydoc_checkpointed = YDocTest()
            t0 = time.perf_counter()
            await ystore.apply_updates(ydoc_checkpointed.ydoc)
            t1 = time.perf_counter()
            read_time_checkpointed = t1 - t0

    # measure without checkpointing
    ystore = MySQLiteYStore(
        store_name, delete=True, checkpoint_interval=None, squash_after_inactivity_of=None
    )
    ydoc = YDocTest()
    async with create_task_group() as tg:
        if ystore_api == "ystore_start_stop":
            ystore = StartStopContextManager(ystore, tg)

        async with ystore as ystore:
            t0 = time.perf_counter()
            for _ in range(number_of_updates):
                update = ydoc.update()
                await ystore.write(update)
            t1 = time.perf_counter()
            write_time = t1 - t0

            # Restore document
            ydoc_manual = YDocTest()
            t0 = time.perf_counter()
            await ystore.apply_updates(ydoc_manual.ydoc)
            t1 = time.perf_counter()
            read_time = t1 - t0

    assert ydoc_checkpointed.array.to_py() == [float(i) for i in range(number_of_updates)]
    assert ydoc_checkpointed.array.to_py() == ydoc_manual.array.to_py()
    checkpointed_read_faster_times = read_time / read_time_checkpointed
    checkpointed_write_faster_times = write_time / write_time_checkpointed
    assert round(checkpointed_read_faster_times) >= test_case["read_speedup"]
    assert round(checkpointed_write_faster_times) >= test_case["write_speedup"]


@pytest.mark.parametrize("db_path", (":memory:", "cleanup_test.db"))
@pytest.mark.parametrize("ystore_api", ("ystore_context_manager", "ystore_start_stop"))
async def test_cleanup_triggers_when_db_size_exceeds_limit(ystore_api, db_path):
    async with create_task_group() as tg:
        test_ydoc = YDocTest()
        store_name = "cleanup_test_store"

        if db_path == ":memory:":

            class InMemoryYStore(MySQLiteYStore):
                db_path = ":memory:"

            ystore_class = InMemoryYStore
        else:
            if ystore_api == "ystore_start_stop":
                unique_db_path = MY_SQLITE_YSTORE_DB_PATH.replace("ystore", "unique_ystore")

                class UniqueDBYStore(MySQLiteYStore):
                    db_path = unique_db_path

                ystore_class = UniqueDBYStore
            else:
                ystore_class = MySQLiteYStore

        DB_SIZE_LIMIT = 0.031
        ystore = ystore_class(
            store_name,
            delete=(db_path != ":memory:"),
            squash_after_inactivity_of=None,
            cleanup_when_db_size_above=DB_SIZE_LIMIT,
        )

        if ystore_api == "ystore_start_stop":
            ystore = StartStopContextManager(ystore, tg)

        async with ystore as ystore:
            await ystore._init_db()
            if db_path == ":memory:":
                db = ystore._db
            else:
                db = await connect(ystore.db_path)
            cursor = await db.cursor()

            # Get initial size
            initial_size = await ystore._get_database_size_mb()
            assert initial_size < DB_SIZE_LIMIT, (
                f"Initial size {initial_size}MB should be below limit"
            )

            # Patch the cleanup method to track calls
            cleanup_called = {"count": 0, "when_size": []}
            original_cleanup = ystore._cleanup_if_needed

            async def tracked_cleanup():
                current_size = await ystore._get_database_size_mb()
                cleanup_called["when_size"].append(current_size)
                result = await original_cleanup()
                if result:
                    cleanup_called["count"] += 1
                return result

            ystore._cleanup_if_needed = tracked_cleanup

            # Write updates until we exceed the limit
            WRITE_COUNT = 80
            for i in range(WRITE_COUNT):
                await ystore.write(test_ydoc.update())

            # Verify cleanup was called
            assert cleanup_called["count"] > 0, "Cleanup should have been triggered"

            # Verify cleanup was called only after exceeding the limit
            sizes_when_cleanup_triggered = [
                s for s in cleanup_called["when_size"] if s > DB_SIZE_LIMIT
            ]
            assert len(sizes_when_cleanup_triggered) > 0, (
                "Cleanup should have been triggered when size exceeded limit"
            )

            # Verify database size was reduced after cleanup
            final_size = await ystore._get_database_size_mb()
            assert final_size < DB_SIZE_LIMIT, (
                f"Final size {final_size}MB should be near limit after cleanup"
            )

            # Verify updates were squashed
            count = (await (await cursor.execute("SELECT count(*) FROM yupdates")).fetchone())[0]
            assert count < WRITE_COUNT, f"Expected cleanup to squash updates, got {count}"

            await db.close()
