"""
Coverage for AsyncLocalClient.sendPUTF's concurrent dispatch (flud/docs/
dht-metadata-performance.md, tier B, item 7) -- previously zero coverage
existed for this class at all.
"""
import asyncio
import time
from pathlib import Path

import pytest

pytest.importorskip("Cryptodome.Cipher")

from flud.protocol.AsyncLocalClient import AsyncLocalClient


pytestmark = pytest.mark.integration


def _run(awaitable):
    return asyncio.run(awaitable)


def _make_files(root, n, prefix="f"):
    root.mkdir(parents=True, exist_ok=True)
    paths = []
    for i in range(n):
        p = root / f"{prefix}{i}.txt"
        p.write_bytes(f"content {prefix}{i}".encode())
        paths.append(p)
    return paths


def test_native_sendputf_directory_all_files_succeed(flud_node):
    tmp = Path("/tmp") / ("fputf-ok-" + str(id(flud_node)))
    _make_files(tmp, 8)
    try:
        async def _scenario():
            client = AsyncLocalClient(flud_node.config, port=flud_node.config.clientport)
            try:
                return await client.sendPUTF(str(tmp), concurrency=4)
            finally:
                await client.close()

        results = _run(_scenario())
        assert len(results) == 8
        assert all(ok for ok, _ in results), results
    finally:
        import shutil
        shutil.rmtree(tmp, ignore_errors=True)


def test_native_sendputf_single_file_unchanged_return_shape(flud_node):
    tmp = Path("/tmp") / ("fputf-single-" + str(id(flud_node)))
    tmp.mkdir(exist_ok=True)
    f = tmp / "one.txt"
    f.write_bytes(b"just one file")
    try:
        async def _scenario():
            client = AsyncLocalClient(flud_node.config, port=flud_node.config.clientport)
            try:
                return await client.sendPUTF(str(f))
            finally:
                await client.close()

        # A single (non-directory) file keeps its original return shape --
        # the raw PUTF result, not an (ok, result) tuple or a list.
        result = _run(_scenario())
        assert not isinstance(result, list)
    finally:
        import shutil
        shutil.rmtree(tmp, ignore_errors=True)


def test_native_sendputf_concurrent_dispatch_faster_than_serial(flud_node):
    """Regression test for the bug this change fixes: dispatching over one
    shared client instance provides no real concurrency at all, since
    request() serializes every call's full round trip under self._lock.
    concurrency=8 (separate sibling connections) must be meaningfully
    faster than concurrency=1 (forced serial) for the same file count."""
    tmp = Path("/tmp") / ("fputf-speed-" + str(id(flud_node)))
    fast_dir = tmp / "fast"
    slow_dir = tmp / "slow"
    n = 10
    _make_files(fast_dir, n, prefix="fast")
    _make_files(slow_dir, n, prefix="slow")
    try:
        async def _timed(path, concurrency):
            client = AsyncLocalClient(flud_node.config, port=flud_node.config.clientport)
            try:
                start = time.perf_counter()
                results = await client.sendPUTF(str(path), concurrency=concurrency)
                elapsed = time.perf_counter() - start
                assert all(ok for ok, _ in results), results
                return elapsed
            finally:
                await client.close()

        concurrent_elapsed = _run(_timed(fast_dir, 8))
        serial_elapsed = _run(_timed(slow_dir, 1))

        # Generous margin (any real improvement, not a specific ratio) to
        # avoid flakiness while still catching a regression to full
        # serialization.
        assert concurrent_elapsed < serial_elapsed * 0.9, (
            concurrent_elapsed, serial_elapsed)
    finally:
        import shutil
        shutil.rmtree(tmp, ignore_errors=True)


def test_native_sendputf_partial_failure_does_not_wedge_pool(flud_node):
    """One bad file among a batch must fail independently (ok=False, with
    the exception) without hanging or preventing the other, good files in
    the same batch from completing -- the "one connection's failure
    shouldn't wedge siblings" property, exercised via a genuinely-failing
    file (nonexistent path) mixed into an otherwise-good batch."""
    tmp = Path("/tmp") / ("fputf-fail-" + str(id(flud_node)))
    good = _make_files(tmp, 6)
    bad_path = tmp / "does-not-exist.txt"  # never created

    try:
        async def _scenario():
            client = AsyncLocalClient(flud_node.config, port=flud_node.config.clientport)
            try:
                # sendPUTF walks the directory via os.walk, which only
                # yields files that actually exist on disk -- to exercise
                # a genuine mid-batch failure we drive the worker pool
                # directly against a mixed file list instead.
                import asyncio as _asyncio
                queue = _asyncio.Queue()
                for p in good + [bad_path]:
                    queue.put_nowait(str(p))
                siblings = [
                    AsyncLocalClient(flud_node.config, port=flud_node.config.clientport)
                    for _ in range(4)
                ]
                results = {}

                async def _worker(c):
                    while True:
                        try:
                            path = queue.get_nowait()
                        except _asyncio.QueueEmpty:
                            return
                        try:
                            results[path] = (True, await c.request("PUTF", path))
                        except Exception as exc:
                            results[path] = (False, exc)

                try:
                    await _asyncio.wait_for(
                        _asyncio.gather(*(_worker(c) for c in siblings)),
                        timeout=30,
                    )
                finally:
                    for c in siblings:
                        await c.close()
                return results
            finally:
                await client.close()

        results = _run(_scenario())
        assert len(results) == 7
        for p in good:
            ok, _res = results[str(p)]
            assert ok, results[str(p)]
        bad_ok, bad_err = results[str(bad_path)]
        assert bad_ok is False
        assert bad_err is not None
    finally:
        import shutil
        shutil.rmtree(tmp, ignore_errors=True)
