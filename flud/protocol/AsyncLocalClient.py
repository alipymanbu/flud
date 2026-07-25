import asyncio
import logging
import os

from flud.fencode import fdecode, fencode

logger = logging.getLogger("flud.local.async_client")


class AsyncLocalClient:
    def __init__(self, config, host="127.0.0.1", port=None):
        self.config = config
        self.host = host
        self.port = port if port is not None else config.clientport
        self._reader = None
        self._writer = None
        self._lock = asyncio.Lock()

    async def connect(self):
        if self._writer is not None and not self._writer.is_closing():
            return
        self._reader, self._writer = await asyncio.open_connection(
            self.host, self.port
        )
        await self._authenticate()

    async def close(self):
        if self._writer is None:
            return
        self._writer.close()
        await self._writer.wait_closed()
        self._reader = None
        self._writer = None

    async def _authenticate(self):
        response = await self._send_line("AUTH?")
        if response[0] != "AUTH" or response[1] != "?":
            raise RuntimeError("unexpected auth challenge response")
        challenge = response[2]
        challenge = (fdecode(challenge),)
        answer = self.config.Kr.decrypt(challenge)
        if isinstance(answer, bytes):
            answer = answer.decode("utf-8")
        response = await self._send_line("AUTH:%s" % answer)
        if response[0] != "AUTH" or response[1] != ":":
            raise RuntimeError("authentication failed")

    async def _send_line(self, line):
        if self._writer is None:
            await self.connect()
        self._writer.write((line + "\r\n").encode("utf-8"))
        await self._writer.drain()
        raw = await self._reader.readline()
        if not raw:
            raise ConnectionError("connection closed by local server")
        decoded = raw.decode("utf-8").rstrip("\r\n")
        command = decoded[:4]
        status = decoded[4]
        data = decoded[5:]
        return command, status, data

    async def request(self, command, data=""):
        async with self._lock:
            response_command, status, payload = await self._send_line(
                "%s?%s" % (command, data)
            )
        if response_command == "DIAG":
            subcommand = payload[:4]
            payload = payload[4:]
            response_command = subcommand
        if status == ":":
            if command in {"NODE", "BKTS"}:
                return payload
            if ":" in payload:
                response, _orig = payload.split(":", 1)
                return fdecode(response)
            return fdecode(payload) if payload else None
        if status == "!":
            if "!" in payload:
                message, _orig = payload.split("!", 1)
                raise RuntimeError(message)
            raise RuntimeError(payload)
        if status == "?":
            return payload
        raise RuntimeError("unexpected response %s%s%s" % (
            response_command, status, payload))

    async def sendPUTF(self, fname, concurrency=8):
        """Stores a single file, or (if fname is a directory) every file in
        its subtree concurrently, via a small pool of sibling connections.

        A single AsyncLocalClient instance can't provide real concurrency
        on its own: request() serializes each call's entire round trip
        (write + read response) under self._lock, so dispatching multiple
        requests on *one* instance just queues them one at a time (this
        exact "looks concurrent, isn't" trap already existed in
        FludScheduler.storeFiles, which does asyncio.gather over one
        shared client). Real concurrency needs separate connections, since
        the wire protocol has no per-request correlation ID to safely
        multiplex many in-flight requests over a single one.

        Returns the raw PUTF result for a single file (unchanged from
        before), or -- for a directory -- a flat list of (ok,
        result_or_exception) tuples, one per file found anywhere in the
        subtree (previously a list nested one level per subdirectory,
        mirroring the recursion; flattening is a deliberate, checked-safe
        change since no caller depended on the nesting).
        """
        if not os.path.isdir(fname):
            return await self.request("PUTF", fname)

        files = []
        for dirpath, _dirnames, filenames in os.walk(fname):
            for name in filenames:
                files.append(os.path.join(dirpath, name))
        if not files:
            return []

        queue = asyncio.Queue()
        for path in files:
            queue.put_nowait(path)

        pool_size = max(1, min(concurrency, len(files)))
        siblings = [AsyncLocalClient(self.config, self.host, self.port)
                for _ in range(pool_size)]
        results = {}

        async def _worker(client):
            while True:
                try:
                    path = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    results[path] = (True, await client.request("PUTF", path))
                except Exception as exc:
                    results[path] = (False, exc)

        try:
            await asyncio.gather(*(_worker(client) for client in siblings))
        finally:
            await asyncio.gather(
                    *(client.close() for client in siblings),
                    return_exceptions=True)

        return [results[path] for path in files]

    async def sendGETI(self, fid):
        return await self.request("GETI", fid)

    async def sendGETF(self, fname):
        return await self.request("GETF", fname)

    async def sendFNDN(self, node_id):
        return await self.request("FNDN", node_id)

    async def sendLIST(self, path=""):
        return await self.request("LIST", path)

    async def sendGETM(self):
        return await self.request("GETM")

    async def sendPUTM(self):
        return await self.request("PUTM")

    async def sendDIAGNODE(self):
        async with self._lock:
            command, status, payload = await self._send_line("DIAG?NODE")
        if command != "DIAG" or status != ":":
            raise RuntimeError(payload)
        return fdecode(payload[4:])

    async def sendDIAGBKTS(self):
        async with self._lock:
            command, status, payload = await self._send_line("DIAG?BKTS")
        if command != "DIAG" or status != ":":
            raise RuntimeError(payload)
        return fdecode(payload[4:])

    async def sendDIAGSTOR(self, command):
        async with self._lock:
            resp_command, status, payload = await self._send_line("DIAG?STOR %s" % command)
        return self._decode_diag_response(resp_command, status, payload)

    async def sendDIAGRTRV(self, command):
        async with self._lock:
            resp_command, status, payload = await self._send_line("DIAG?RTRV %s" % command)
        return self._decode_diag_response(resp_command, status, payload)

    async def sendDIAGVRFY(self, command):
        async with self._lock:
            resp_command, status, payload = await self._send_line("DIAG?VRFY %s" % command)
        return self._decode_diag_response(resp_command, status, payload)

    async def sendDIAGFNDV(self, value):
        return await self.request("FNDV", value)

    def _decode_diag_response(self, response_command, status, payload):
        if response_command != "DIAG":
            raise RuntimeError("unexpected diag response")
        subcommand = payload[:4]
        body = payload[4:]
        if status == ":":
            response, _orig = body.split(":", 1)
            return fdecode(response)
        if status == "!":
            message, _orig = body.split("!", 1)
            raise RuntimeError(message)
        raise RuntimeError("unexpected diag status %s for %s" % (status, subcommand))
