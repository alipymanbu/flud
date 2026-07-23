"""
ClientDHTPrimitives.py (c) 2003-2006 Alen Peacock.  This program is distributed
under the terms of the GNU General Public License (the GPL), version 3.

Primitive client DHT protocol
"""
import time, logging, asyncio, socket
from http import HTTPStatus
import flud.FludkRouting as FludkRouting
from flud.fencode import fencode
from flud.async_runtime import maybe_await
from .ClientPrimitives import _normalize_headers
from .FludCommUtil import *

try:
    import aiohttp
except Exception:  # pragma: no cover - aiohttp is optional at runtime
    aiohttp = None


logger = logging.getLogger("flud.client.dht")

# FUTURE: check flud protocol version for backwards compatibility
# XXX: need to make sure we have appropriate timeouts for all comms.
# FUTURE: DOS attacks.  For now, assume that network hardware can filter these 
#      out (by throttling individual IPs) -- i.e., it isn't our problem.  If we
#      want to defend against this at some point, we need to keep track of who
#      is generating requests and then ignore them.
# XXX: might want to consider some self-healing for the kademlia layer, as 
#      outlined by this thread: 
#      http://zgp.org/pipermail/p2p-hackers/2003-August/001348.html (should also
#      consider Zooko's links in the parent to this post).  Basic idea: don't
#      always take the k-closest -- take x random and k-x of the k-closest.
#      Can alternate each round (k-closest / x + k-x-closest) for a bit more
#      diversity (as in "Sybil-resistent DHT routing").
# XXX: right now, calls to updateNode are chained.  Might want to think about
#      doing some of this more asynchronously, so that the recursive parts
#      aren't waiting for remote GETIDs to return before recursing.

"""
The active DHT client path is asyncio-native. Single-hop helpers use the
``send_k_*`` naming scheme, and recursive helpers use the canonical
``k_*`` names.
"""


async def send_k_find_node(node, host, port, key, command_name="nodes", metrics=None):
    return await maybe_await(
            node.async_runtime.submit(
                _send_k_find_node(node, host, port, key, command_name, metrics)))


def _is_timeout_error(exc):
    return isinstance(exc, (asyncio.TimeoutError, socket.timeout))


def _operation_trace(metrics, op_type, **context):
    if metrics is None:
        return None
    if hasattr(metrics, "start_operation"):
        return metrics.start_operation(op_type, **context)
    return metrics


async def _send_k_find_node(node, host, port, key, command_name="nodes", metrics=None):
    if aiohttp is None:
        raise RuntimeError("aiohttp not available for async DHT request")
    host = getCanonicalIP(host)
    headers = {'Fludprotocol': PROTOCOL_VERSION, 'User-Agent': 'FludClient'}
    Ku = node.config.Ku.exportPublicKey()
    url = ('http://%s:%d/%s/%s?nodeID=%s&Ku_e=%s&Ku_n=%s&port=%s') % (
            host, port, command_name, fencode(key), node.config.nodeID,
            Ku['e'], Ku['n'], node.config.port)
    timeoutcount = 0
    while True:
        try:
            if metrics:
                metrics.record_rpc_attempt()
            timeout = aiohttp.ClientTimeout(total=kprimitive_to)
            resp = await node.async_http.request(
                    "GET", url,
                    headers=_normalize_headers(headers),
                    timeout=timeout)
            try:
                status = resp.status
                body = await resp.text()
            finally:
                resp.release()
            if status != HTTPStatus.OK:
                raise RuntimeError(
                        "%s FAILED from %s:%d: received status %s, '%s'"
                        % (command_name, host, port, status, body))
            response = eval(body)
            nID = int(response['id'], 16)
            updateNode(node.client, node.config, host, port, None, nID)
            updateNodes(node.client, node.config, response['k'])
            return response
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            if metrics:
                metrics.record_rpc_failure(timed_out=_is_timeout_error(exc))
            timeoutcount += 1
            if timeoutcount >= MAXTIMEOUTS:
                raise socket.error(str(exc))


async def send_k_find_value(node, host, port, key, metrics=None):
    return await maybe_await(
            node.async_runtime.submit(
                _send_k_find_value(node, host, port, key, metrics)))


async def _send_k_find_value(node, host, port, key, metrics=None):
    if aiohttp is None:
        raise RuntimeError("aiohttp not available for async DHT request")
    host = getCanonicalIP(host)
    headers = {'Fludprotocol': PROTOCOL_VERSION, 'User-Agent': 'FludClient'}
    Ku = node.config.Ku.exportPublicKey()
    url = ('http://%s:%d/meta/%s?nodeID=%s&Ku_e=%s&Ku_n=%s&port=%s') % (
            host, port, fencode(key), node.config.nodeID,
            Ku['e'], Ku['n'], node.config.port)
    timeoutcount = 0
    while True:
        try:
            if metrics:
                metrics.record_rpc_attempt()
            timeout = aiohttp.ClientTimeout(total=kprimitive_to)
            resp = await node.async_http.request(
                    "GET", url,
                    headers=_normalize_headers(headers),
                    timeout=timeout)
            try:
                status = resp.status
                body = await resp.text()
                content_type = resp.headers.get("Content-Type", "")
                node_id = resp.headers.get("nodeID")
            finally:
                resp.release()
            if status != HTTPStatus.OK:
                raise RuntimeError(
                        "meta FAILED from %s:%d: received status %s, '%s'"
                        % (host, port, status, body))
            if content_type == "application/x-flud-data":
                updateNode(node.client, node.config, host, port, None, node_id)
                return body
            response = eval(body)
            nID = int(response['id'], 16)
            updateNode(node.client, node.config, host, port, None, nID)
            updateNodes(node.client, node.config, response['k'])
            return response
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            if metrics:
                metrics.record_rpc_failure(timed_out=_is_timeout_error(exc))
            timeoutcount += 1
            if timeoutcount >= MAXTIMEOUTS:
                raise socket.error(str(exc))


async def send_k_store(node, host, port, key, val, metrics=None):
    return await maybe_await(
            node.async_runtime.submit(
                _send_k_store(node, host, port, key, val, metrics)))


async def _send_k_store(node, host, port, key, val, metrics=None):
    if aiohttp is None:
        raise RuntimeError("aiohttp not available for async kSTORE")
    host = getCanonicalIP(host)
    headers = {'Fludprotocol': PROTOCOL_VERSION, 'User-Agent': 'FludClient'}
    Ku = node.config.Ku.exportPublicKey()
    url = ('http://%s:%d/meta/%s/%s?nodeID=%s&Ku_e=%s&Ku_n=%s&port=%s') % (
            host, port, fencode(key), fencode(val), node.config.nodeID,
            Ku['e'], Ku['n'], node.config.port)
    timeoutcount = 0
    while True:
        try:
            if metrics:
                metrics.record_rpc_attempt()
            timeout = aiohttp.ClientTimeout(total=kprimitive_to)
            resp = await node.async_http.request(
                    "PUT", url,
                    headers=_normalize_headers(headers),
                    timeout=timeout)
            try:
                status = resp.status
                body = await resp.text()
            finally:
                resp.release()
            if status != HTTPStatus.OK:
                raise RuntimeError(
                        "kSTORE FAILED from %s:%d status=%s body=%s"
                        % (host, port, status, body))
            logger.info("kSTORE to %s:%d finished", host, port)
            return body
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            if metrics:
                metrics.record_rpc_failure(timed_out=_is_timeout_error(exc))
            timeoutcount += 1
            if timeoutcount >= MAXTIMEOUTS:
                raise socket.error(str(exc))


def _normalize_alpha(alpha):
    if alpha is None:
        alpha = FludkRouting.a
    try:
        alpha = int(alpha)
    except (TypeError, ValueError):
        alpha = FludkRouting.a
    return max(1, alpha)


def _normalize_alpha_mode(alpha_mode):
    mode = (alpha_mode or "fixed").lower()
    if mode not in ("fixed", "adaptive"):
        raise ValueError("invalid alpha mode %r" % alpha_mode)
    return mode


def _normalize_value_policy(value_policy):
    policy = (value_policy or "first").lower()
    if policy not in ("first", "majority"):
        raise ValueError("invalid value policy %r" % value_policy)
    return policy


def _candidate_distance(candidate, key):
    return int(candidate[2]) ^ key


def _candidate_sort_key(candidate, key):
    return (_candidate_distance(candidate, key), int(candidate[2]), candidate[0], candidate[1])


def _extract_exact_node_response(response, key):
    if not isinstance(response, dict):
        return None
    candidates = response.get('k', [])
    if len(candidates) == 1 and int(candidates[0][2]) == key:
        return response
    return None


class _AlphaController:
    def __init__(self, base_alpha, mode="fixed"):
        self.base_alpha = _normalize_alpha(base_alpha)
        self.mode = _normalize_alpha_mode(mode)
        self.current_alpha = self.base_alpha
        self._recent = []

    def alpha(self, frontier_size=0, in_flight=0):
        if self.mode == "adaptive":
            ceiling = max(self.base_alpha, min(FludkRouting.k, frontier_size + in_flight))
            self.current_alpha = max(1, min(self.current_alpha, ceiling or self.base_alpha))
        else:
            self.current_alpha = self.base_alpha
        return self.current_alpha

    def note_result(self, success):
        if self.mode != "adaptive":
            return
        self._recent.append(bool(success))
        if len(self._recent) > 8:
            self._recent.pop(0)
        success_count = sum(1 for item in self._recent if item)
        failure_count = len(self._recent) - success_count
        if failure_count >= max(2, len(self._recent) // 2):
            self.current_alpha = max(1, self.current_alpha - 1)
        elif success_count >= max(3, len(self._recent) - 1):
            self.current_alpha = min(FludkRouting.k, self.current_alpha + 1)


class _ValueAccumulator:
    def __init__(self):
        self.counts = {}
        self.total = 0

    def observe(self, value):
        self.total += 1
        self.counts[value] = self.counts.get(value, 0) + 1

    def best_value(self):
        if not self.counts:
            return None
        return max(self.counts.items(), key=lambda item: (item[1], str(item[0])))[0]

    def should_return(self, policy):
        if not self.counts:
            return (False, None)
        if policy == "first":
            return (True, self.best_value())
        best_value, best_count = max(
            self.counts.items(), key=lambda item: (item[1], str(item[0])))
        if best_count >= 2 and (best_count > (self.total / 2.0)):
            return (True, best_value)
        return (False, None)


class _LookupFrontier:
    def __init__(self, key):
        self.key = key
        self._candidates = {}

    def add_many(self, candidates):
        for candidate in candidates:
            normalized = tuple(candidate)
            if len(normalized) < 3:
                continue
            normalized = normalized[:2] + (int(normalized[2]),) + normalized[3:]
            node_id = normalized[2]
            current = self._candidates.get(node_id)
            if current is None or _candidate_distance(normalized, self.key) < _candidate_distance(current, self.key):
                self._candidates[node_id] = normalized

    def pending(self, queried_ids, in_flight_ids):
        blocked = set(queried_ids) | set(in_flight_ids)
        return sorted(
            (candidate for node_id, candidate in self._candidates.items() if node_id not in blocked),
            key=lambda item: _candidate_sort_key(item, self.key),
        )

    def best_k(self, exclude_ids=()):
        excluded = set(exclude_ids)
        return sorted(
            (candidate for node_id, candidate in self._candidates.items() if node_id not in excluded),
            key=lambda item: _candidate_sort_key(item, self.key),
        )[:FludkRouting.k]

    def has_better_pending(self, threshold_distance, queried_ids, in_flight_ids):
        blocked = set(queried_ids) | set(in_flight_ids)
        for candidate in sorted(
                (candidate for node_id, candidate in self._candidates.items()
                 if node_id not in blocked),
                key=lambda item: _candidate_sort_key(item, self.key)):
            if threshold_distance is None:
                return True
            return _candidate_distance(candidate, self.key) < threshold_distance
        return False


async def _k_find_node_impl(node, key, op_metrics=None, alpha=None, alpha_mode="fixed"):
    node.DHTtstamp = time.time()
    queried = {}
    failed = set()
    frontier = _LookupFrontier(key)
    controller = _AlphaController(alpha, alpha_mode)
    abbrvkey = ("%x" % key)[:8] + "..."
    abbrv = "(%s%s)" % (abbrvkey, str(node.DHTtstamp)[-7:])

    def _update_state(response, host, port):
        exact = _extract_exact_node_response(response, key)
        if exact is not None:
            frontier.add_many(exact.get('k', []))
            if op_metrics:
                op_metrics.set_returned_candidate_count(len(exact['k']))
            return exact
        if not isinstance(response, dict):
            return None
        responder_id = int(response['id'], 16)
        queried[responder_id] = (host, port)
        if op_metrics:
            op_metrics.record_queried_node(responder_id)
        frontier.add_many(response.get('k', []))
        return None

    localhost = getCanonicalIP('localhost')
    local_response = {
        'id': node.config.nodeID,
        'k': node.config.routing.findNode(key),
    }
    exact = _update_state(local_response, localhost, node.config.port)
    if exact is not None:
        return exact

    in_flight = {}
    wait_count = 0
    while True:
        pending = frontier.pending(queried.keys(), (candidate[2] for candidate in in_flight.values()))
        active_alpha = controller.alpha(len(pending), len(in_flight))
        if op_metrics:
            op_metrics.record_alpha(active_alpha)
        while len(in_flight) < active_alpha and pending:
            candidate = pending.pop(0)
            host, port, node_id = candidate[0], candidate[1], candidate[2]
            task = asyncio.create_task(
                send_k_find_node(node, host, port, key, metrics=op_metrics))
            in_flight[task] = candidate
        if not in_flight:
            break
        if op_metrics:
            op_metrics.record_round()
        logger.debug("FN: %s doing async wait %d", abbrv, wait_count)
        wait_count += 1
        done, _ = await asyncio.wait(
            list(in_flight.keys()), return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            candidate = in_flight.pop(task)
            host, port, node_id = candidate[0], candidate[1], candidate[2]
            try:
                response = await task
                controller.note_result(True)
            except Exception as exc:
                controller.note_result(False)
                failed.add(node_id)
                logger.info("kFindNode %s request to %s:%d failed -- %s",
                        abbrv, host, port, str(exc))
                continue
            exact = _update_state(response, host, port)
            if exact is not None:
                return exact
        best = frontier.best_k(exclude_ids=failed)
        threshold = _candidate_distance(best[-1], key) if best else None
        if not frontier.has_better_pending(
                threshold, queried.keys(),
                (candidate[2] for candidate in in_flight.values())) and not in_flight:
            break

    logger.info("kFindNode %s terminated successfully after %d queries.",
            abbrv, len(queried))
    result = {'k': frontier.best_k(exclude_ids=failed)}
    if op_metrics:
        op_metrics.set_returned_candidate_count(len(result['k']))
    return result


async def k_find_node(node, key, metrics=None, alpha=None, alpha_mode="fixed"):
    op_metrics = _operation_trace(metrics, "k_find_node", key=fencode(key))
    try:
        result = await _k_find_node_impl(
            node, key, op_metrics=op_metrics, alpha=alpha, alpha_mode=alpha_mode)
    except Exception as exc:
        if op_metrics:
            op_metrics.finish_failure(exc, timed_out=_is_timeout_error(exc))
        raise
    if op_metrics:
        op_metrics.finish_success()
    return result


async def k_find_value(node, key, metrics=None, alpha=None, alpha_mode="fixed",
        value_policy="first"):
    op_metrics = _operation_trace(metrics, "k_find_value", key=fencode(key))
    value_policy = _normalize_value_policy(value_policy)
    node.DHTtstamp = time.time()
    queried = {}
    failed = set()
    frontier = _LookupFrontier(key)
    controller = _AlphaController(alpha, alpha_mode)
    values = _ValueAccumulator()
    abbrvkey = ("%x" % key)[:8] + "..."
    abbrv = "(%s%s)" % (abbrvkey, str(node.DHTtstamp)[-7:])

    def _update_state(response, host, port):
        if not isinstance(response, dict):
            if response is not None:
                values.observe(response)
                if op_metrics:
                    op_metrics.record_value_response()
                    op_metrics.set_value_found(True)
            return response
        responder_id = int(response['id'], 16)
        queried[responder_id] = (host, port)
        if op_metrics:
            op_metrics.record_queried_node(responder_id)
        frontier.add_many(response.get('k', []))
        return None

    localhost = getCanonicalIP('localhost')
    try:
        initial = await send_k_find_value(
            node, localhost, node.config.port, key, metrics=op_metrics)
        exact = _update_state(initial, localhost, node.config.port)
        if exact is not None and not isinstance(exact, dict):
            should_return, result = values.should_return(value_policy)
            if should_return:
                if op_metrics:
                    op_metrics.set_early_terminated(True)
                    op_metrics.finish_success()
                return result

        in_flight = {}
        wait_count = 0
        while True:
            pending = frontier.pending(queried.keys(), (candidate[2] for candidate in in_flight.values()))
            active_alpha = controller.alpha(len(pending), len(in_flight))
            if op_metrics:
                op_metrics.record_alpha(active_alpha)
            while len(in_flight) < active_alpha and pending:
                candidate = pending.pop(0)
                host, port, node_id = candidate[0], candidate[1], candidate[2]
                task = asyncio.create_task(
                    send_k_find_value(node, host, port, key, metrics=op_metrics))
                in_flight[task] = candidate
            if not in_flight:
                break
            if op_metrics:
                op_metrics.record_round()
            logger.debug("FV: %s doing async wait %d", abbrv, wait_count)
            wait_count += 1
            done, _ = await asyncio.wait(
                list(in_flight.keys()), return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                candidate = in_flight.pop(task)
                host, port, node_id = candidate[0], candidate[1], candidate[2]
                try:
                    response = await task
                    controller.note_result(True)
                except Exception as exc:
                    controller.note_result(False)
                    failed.add(node_id)
                    logger.info("kFindValue %s request to %s:%d failed -- %s",
                            abbrv, host, port, str(exc))
                    continue
                exact = _update_state(response, host, port)
                if exact is not None and not isinstance(exact, dict):
                    should_return, result = values.should_return(value_policy)
                    if should_return:
                        if op_metrics:
                            op_metrics.set_early_terminated(True)
                            op_metrics.finish_success()
                        return result
            best = frontier.best_k(exclude_ids=failed)
            threshold = _candidate_distance(best[-1], key) if best else None
            if not frontier.has_better_pending(
                    threshold, queried.keys(),
                    (candidate[2] for candidate in in_flight.values())) and not in_flight:
                break

        result = values.best_value()
        if result is None:
            logger.info("couldn't get any results")
            if op_metrics:
                op_metrics.finish_success()
            return None
        if op_metrics:
            op_metrics.finish_success()
        return result
    except Exception as exc:
        if op_metrics:
            op_metrics.finish_failure(exc, timed_out=_is_timeout_error(exc))
        raise


async def k_store(node, key, val, metrics=None, alpha=None, alpha_mode="fixed"):
    op_metrics = _operation_trace(metrics, "k_store", key=fencode(key))
    try:
        knodes = await _k_find_node_impl(
            node, key, op_metrics=op_metrics, alpha=alpha, alpha_mode=alpha_mode)
        knodes = knodes['k']
        if op_metrics:
            op_metrics.set_store_destination_count(len(knodes))
            op_metrics.set_returned_candidate_count(len(knodes))
        if len(knodes) < 1:
            raise RuntimeError("can't complete kStore -- no nodes")
        results = await asyncio.gather(
            *(send_k_store(node, knode[0], knode[1], key, val, metrics=op_metrics)
              for knode in knodes),
            return_exceptions=True,
        )
        failures = []
        for result in results:
            if isinstance(result, Exception):
                failures.append(result)
                if op_metrics:
                    op_metrics.record_store_failure(
                        timed_out=_is_timeout_error(result))
            else:
                if op_metrics:
                    op_metrics.record_store_success()
        if failures:
            raise RuntimeError(results)
        logger.info("kStore finished")
        if op_metrics:
            op_metrics.finish_success()
        return ""
    except Exception as exc:
        if op_metrics:
            op_metrics.finish_failure(exc, timed_out=_is_timeout_error(exc))
        raise
