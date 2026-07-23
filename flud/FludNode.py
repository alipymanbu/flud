#!/usr/bin/env python3

"""
FludNode.py (c) 2003-2006 Alen Peacock.  This program is distributed under the
terms of the GNU General Public License (the GPL), verison 3.

FludNode is the process that runs to talk with other nodes in the flud backup network.
"""

import asyncio
import signal
import time
import os
import random
import logging

from flud.FludConfig import FludConfig
from flud.fencode import fencode, fdecode
from flud.protocol.AiohttpServer import FludAiohttpServer
from flud.protocol.FludClient import FludClient
from flud.protocol.ClientDHTPrimitives import _TTLCache, _default_cache_ttl_seconds
from flud.async_runtime import AsyncHTTPClient, AsyncRuntime

PINGTIME=60
SYNCTIME=900
REPUBLISH_TIME=int(os.environ.get("FLUD_DHT_REPUBLISH_INTERVAL_S", 6 * 3600))
REPUBLISH_CONCURRENCY=5

class FludNode(object):
    """
    A node in the flud network.  A node is both a client and a server.  It
    listens on a network accessible port for both DHT and Storage layer
    requests, and it listens on a local port for client interface commands.
    """
    
    def __init__(self, port=None):
        self._initLogger()
        self.config = FludConfig()
        self.logger.removeHandler(self.screenhandler)
        self.config.load(serverport=port)
        self.client = FludClient(self)
        self.async_runtime = AsyncRuntime()
        self.async_runtime.start()
        self.async_http = AsyncHTTPClient(self.async_runtime)
        self.DHTtstamp = time.time()+10
        self._use_async_server = True
        self._async_tasks = []
        self._background_dht_tasks = set()
        self.dht_cache = _TTLCache(_default_cache_ttl_seconds())

    def _initLogger(self):
        logger = logging.getLogger('flud')
        self.screenhandler = logging.StreamHandler()
        self.screenhandler.setLevel(logging.INFO)
        logger.addHandler(self.screenhandler)
        self.logger = logger

    async def _async_sync_loop(self):
        while True:
            await asyncio.sleep(SYNCTIME)
            self.config.save()

    async def _async_republish_loop(self):
        while True:
            await asyncio.sleep(REPUBLISH_TIME)
            try:
                await self._republish_owned_values()
            except Exception:
                self.logger.exception("DHT republish cycle failed")

    async def _republish_owned_values(self):
        """Periodically re-issues k_store for values this node owns (its
        manifest CAS pointer, and any block metadata it originated), so
        replicas don't silently expire under churn between file operations.
        See flud/docs/dht-metadata-performance.md, recommendation A2."""
        semaphore = asyncio.Semaphore(REPUBLISH_CONCURRENCY)

        async def _store(key, value):
            async with semaphore:
                try:
                    await self.client.k_store(key, value)
                except Exception as error:
                    self.logger.info("republish failed for %x: %s", key, error)

        jobs = []

        manifest_cas = getattr(self.config, "manifest_cas", None)
        if manifest_cas:
            jobs.append(_store(int(self.config.nodeID, 16), manifest_cas))

        with self.config.manifest_lock:
            manifest_entries = list(self.config.manifest.items())
        for _fname, entry in manifest_entries:
            if not (isinstance(entry, tuple) and len(entry) == 2):
                continue
            sK, _tstamp = entry
            try:
                cache_path = os.path.join(self.config.metadir, fencode(sK))
            except Exception:
                continue
            if not os.path.isfile(cache_path):
                continue
            try:
                with open(cache_path, "rb") as f:
                    cached_metadata = fdecode(f.read())
            except Exception:
                continue
            jobs.append(_store(sK, cached_metadata))

        if jobs:
            await asyncio.gather(*jobs, return_exceptions=True)

    def _schedule_async_tasks(self):
        self._async_tasks.append(self.async_runtime.submit(self._async_sync_loop()))
        self._async_tasks.append(self.async_runtime.submit(self._async_republish_loop()))

    def start(self, twistd=False):
        """Starts the asyncio server in this thread."""
        self.logger.log(logging.INFO,
                "FludAiohttpServer starting on %d" % self.config.port)
        self.run()
        if not twistd:
            self.join()

    def run(self):
        """Starts the asyncio server in its own thread."""
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        self.webserver = FludAiohttpServer(self, self.config.port)
        self._schedule_async_tasks()
        self.webserver.start()
    
    def stop(self):
        self.logger.log(logging.INFO, "shutting down FludNode")
        self.webserver.stop()
        for task in self._async_tasks:
            task.cancel()
        self._async_tasks = []
        for task in list(self._background_dht_tasks):
            # Background DHT tasks may belong to a loop running on a
            # different thread (async_runtime, the aiohttp server thread,
            # or a caller's own asyncio.run()); cancel via that loop rather
            # than calling task.cancel() directly from this thread.
            try:
                task.get_loop().call_soon_threadsafe(task.cancel)
            except RuntimeError:
                pass  # loop already closed
        self._background_dht_tasks.clear()
        self.async_http.close()
        self.async_runtime.stop()

    def join(self):
        self.webserver.join()

    def sighandler(self, sig, frame):
        self.logger.log(logging.INFO, "handling signal %s" % sig)
    
    def connectViaGateway(self, host, port):
        self._async_tasks.append(
            self.async_runtime.submit(self._async_connectViaGateway(host, port))
        )

    async def _async_connectViaGateway(self, host, port):
        max_attempts = int(os.environ.get("FLUDGWRETRIES", "20"))
        initial_delay = float(os.environ.get("FLUDGWRETRY_DELAY", "0.5"))
        max_delay = float(os.environ.get("FLUDGWRETRY_MAX_DELAY", "10.0"))
        request_timeout = float(os.environ.get("FLUDGWCONNECT_TIMEOUT", "5.0"))
        delay = initial_delay

        for attempt in range(1, max_attempts + 1):
            try:
                if request_timeout > 0:
                    knodes = await asyncio.wait_for(
                        self.client.send_k_find_node(
                            host, port, self.config.routing.node[2]),
                        timeout=request_timeout)
                else:
                    knodes = await self.client.send_k_find_node(
                        host, port, self.config.routing.node[2])
                await self._async_refresh_buckets(knodes)
                print("flud node connected and listening on port %d"
                        % self.config.port)
                return
            except Exception as error:
                self.logger.warning(
                    "gateway connect attempt %d/%d to %s:%s failed: %s",
                    attempt, max_attempts, host, port, str(error)
                )
                if attempt >= max_attempts:
                    self.logger.error(
                        "giving up gateway connection to %s:%s after %d attempts",
                        host, port, max_attempts
                    )
                    raise
                await asyncio.sleep(min(delay, max_delay))
                delay = min(max_delay, delay * 2)

    async def _async_refresh_buckets(self, knodes):
        dlist = []
        for bucket in self.config.routing.kBuckets:
            if bucket.begin <= self.config.routing.node[2] < bucket.end:
                continue
            begin = int(bucket.begin)
            end = int(bucket.end)
            if end <= begin:
                continue
            refreshID = random.randrange(begin, end)
            dlist.append(
                self.client.k_find_node(refreshID)
            )
        if dlist:
            results = await asyncio.gather(*dlist, return_exceptions=True)
            self.logger.info("bucket refreshes finished: %s" % results)

def getPath():
    # this is a hack to be able to get the location of FludNode.tac
    return os.path.dirname(os.path.abspath(__file__))
