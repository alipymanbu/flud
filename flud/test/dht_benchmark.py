#!/usr/bin/env python3

import argparse
import asyncio
import json
import os
import platform
import random
import shutil
import socket
import subprocess
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from flud.protocol.DHTMetrics import DHTMetricsCollector
from flud.protocol.FludCommUtil import getCanonicalIP
from flud.test._standalone import start_test_node


TESTVAL = {
    (0, 802484): 465705,
    (1, 780638): 465705,
    (2, 169688): 465705,
    (3, 267175): 465705,
    (4, 648636): 465705,
    (5, 838315): 465705,
    (6, 477619): 465705,
    (7, 329906): 465705,
    (8, 610565): 465705,
    (9, 217811): 465705,
    (10, 374124): 465705,
    (11, 357214): 465705,
    (12, 147307): 465705,
    (13, 427751): 465705,
    (14, 927853): 465705,
    (15, 760369): 465705,
    (16, 707029): 465705,
    (17, 479234): 465705,
    (18, 190455): 465705,
    (19, 647489): 465705,
    (20, 620470): 465705,
    (21, 777532): 465705,
    (22, 622383): 465705,
    (23, 573283): 465705,
    (24, 613082): 465705,
    (25, 433593): 465705,
    (26, 584543): 465705,
    (27, 337485): 465705,
    (28, 911014): 465705,
    (29, 594065): 465705,
    (30, 375876): 465705,
    (31, 726818): 465705,
    (32, 835759): 465705,
    (33, 814060): 465705,
    (34, 237176): 465705,
    (35, 538268): 465705,
    (36, 272650): 465705,
    (37, 314058): 465705,
    (38, 257714): 465705,
    (39, 439931): 465705,
    "k": 20,
    "n": 20,
}


@dataclass
class BenchmarkCluster:
    host: str
    gateway_port: int
    nodes: list
    clients: list
    managed_nodes: list
    temp_root: str | None


@contextmanager
def _fludhome(home):
    previous = os.environ.get("FLUDHOME")
    os.environ["FLUDHOME"] = str(home)
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("FLUDHOME", None)
        else:
            os.environ["FLUDHOME"] = previous


def _git_commit():
    try:
        output = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=Path(__file__).resolve().parents[2],
            stderr=subprocess.DEVNULL,
            text=True,
        )
    except Exception:
        return None
    return output.strip()


def _wait_for_routing_population(node, min_known, timeout):
    deadline = time.time() + timeout
    while time.time() < deadline:
        known = len(node.config.routing.knownExternalNodes())
        if known >= min_known:
            return known
        time.sleep(0.2)
    raise RuntimeError(
        "routing population timed out after %.1fs (%d known external nodes, wanted %d)"
        % (timeout, len(node.config.routing.knownExternalNodes()), min_known)
    )


async def _start_local_cluster(args):
    host = getCanonicalIP(args.gateway_host)
    temp_root = tempfile.mkdtemp(prefix="flud-dht-bench-", dir="/tmp")
    homes = [Path(temp_root) / f".flud{i}" for i in range(args.nodes)]
    nodes = []
    try:
        with _fludhome(homes[0]):
            gateway = start_test_node(args.base_port)
        nodes.append(gateway)
        for index, home in enumerate(homes[1:], start=1):
            with _fludhome(home):
                node = start_test_node(args.base_port + index)
            node._async_tasks.append(
                node.async_runtime.submit(
                    node._async_connectViaGateway(host, gateway.config.port)
                )
            )
            node._async_tasks[-1].result(timeout=args.timeout)
            nodes.append(node)
        for node in nodes:
            for peer in nodes:
                if peer is node:
                    continue
                await node.client.get_id(host, peer.config.port)
        clients = nodes[-min(args.clients, len(nodes)) :]
        min_known = max(1, min(5, args.nodes - 1))
        for client in clients:
            _wait_for_routing_population(client, min_known, args.timeout)
        return BenchmarkCluster(
            host=host,
            gateway_port=gateway.config.port,
            nodes=nodes,
            clients=clients,
            managed_nodes=nodes,
            temp_root=temp_root,
        )
    except Exception:
        for node in reversed(nodes):
            try:
                node.stop()
            except Exception:
                pass
        shutil.rmtree(temp_root, ignore_errors=True)
        raise


async def _attach_benchmark_clients(args):
    host = getCanonicalIP(args.gateway_host)
    temp_root = tempfile.mkdtemp(prefix="flud-dht-bench-attach-", dir="/tmp")
    homes = [Path(temp_root) / f".flud-client{i}" for i in range(args.clients)]
    nodes = []
    gateway_port = args.gateway_port or args.base_port
    try:
        for index, home in enumerate(homes):
            with _fludhome(home):
                node = start_test_node(args.base_port + args.nodes + index + 1)
            node._async_tasks.append(
                node.async_runtime.submit(
                    node._async_connectViaGateway(host, gateway_port)
                )
            )
            node._async_tasks[-1].result(timeout=args.timeout)
            nodes.append(node)
        min_known = max(1, min(5, args.nodes - 1))
        for client in nodes:
            _wait_for_routing_population(client, min_known, args.timeout)
        return BenchmarkCluster(
            host=host,
            gateway_port=gateway_port,
            nodes=[],
            clients=nodes,
            managed_nodes=nodes,
            temp_root=temp_root,
        )
    except Exception:
        for node in reversed(nodes):
            try:
                node.stop()
            except Exception:
                pass
        shutil.rmtree(temp_root, ignore_errors=True)
        raise


def _stop_cluster(cluster):
    for node in reversed(cluster.managed_nodes):
        try:
            node.stop()
        except Exception:
            pass
    if cluster.temp_root:
        shutil.rmtree(cluster.temp_root, ignore_errors=True)


def _percentile(values, pct):
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    ordered = sorted(values)
    idx = (len(ordered) - 1) * pct
    lower = int(idx)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = idx - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _series_summary(values):
    if not values:
        return {
            "count": 0,
            "min": None,
            "mean": None,
            "p50": None,
            "p95": None,
            "p99": None,
            "max": None,
        }
    return {
        "count": len(values),
        "min": min(values),
        "mean": sum(values) / len(values),
        "p50": _percentile(values, 0.50),
        "p95": _percentile(values, 0.95),
        "p99": _percentile(values, 0.99),
        "max": max(values),
    }


def _summarize_phase(name, routing_state, collector, duration_seconds):
    operations = collector.to_dict()
    successes = [op for op in operations if op["status"] == "success"]
    failures = [op for op in operations if op["status"] != "success"]
    timed_out = [op for op in operations if op["timed_out"]]
    durations = [op["duration_seconds"] for op in operations if op["duration_seconds"] is not None]
    queried = [op["queried_node_count"] for op in operations]
    rounds = [op["round_count"] for op in operations]
    rpc_attempts = [op["rpc_attempts"] for op in operations]
    rpc_failures = [op["rpc_failures"] for op in operations]
    store_destinations = [op["store_destination_count"] for op in operations]
    store_successes = [op["store_success_count"] for op in operations]
    store_failures = [op["store_failure_count"] for op in operations]
    value_responses = [op["value_response_count"] for op in operations]
    alpha_values = [sample for op in operations for sample in op.get("alpha_samples", [])]
    value_found_count = sum(1 for op in operations if op["value_found"])
    early_terminated_count = sum(1 for op in operations if op["early_terminated"])
    return {
        "name": name,
        "routing_state": routing_state,
        "attempted_ops": len(operations),
        "completed_ops": len(successes),
        "failed_ops": len(failures),
        "timeout_count": len(timed_out),
        "value_found_count": value_found_count,
        "early_terminated_count": early_terminated_count,
        "duration_seconds": duration_seconds,
        "ops_per_second": (len(successes) / duration_seconds) if duration_seconds > 0 else 0.0,
        "latency_seconds": _series_summary(durations),
        "queried_node_count": _series_summary(queried),
        "round_count": _series_summary(rounds),
        "rpc_attempts": _series_summary(rpc_attempts),
        "rpc_failures": _series_summary(rpc_failures),
        "value_response_count": _series_summary(value_responses),
        "alpha": _series_summary(alpha_values),
        "store_destination_count": _series_summary(store_destinations),
        "store_success_count": _series_summary(store_successes),
        "store_failure_count": _series_summary(store_failures),
        "operations": operations,
    }


def _print_phase_summary(summary):
    latency = summary["latency_seconds"]
    print(
        "%s [%s] ops=%d ok=%d fail=%d timeout=%d ops/s=%.2f p50=%.4fs p95=%.4fs p99=%.4fs"
        % (
            summary["name"],
            summary["routing_state"],
            summary["attempted_ops"],
            summary["completed_ops"],
            summary["failed_ops"],
            summary["timeout_count"],
            summary["ops_per_second"],
            latency["p50"] or 0.0,
            latency["p95"] or 0.0,
            latency["p99"] or 0.0,
        )
    )


async def _benchmark_call(
    client,
    opname,
    key,
    collector,
    timeout,
    phase_name,
    routing_state,
    alpha,
    alpha_mode,
    value_policy,
):
    op = collector.start_operation(
        opname,
        key=str(key),
        client_port=client.config.port,
        phase=phase_name,
        routing_state=routing_state,
        alpha=alpha,
        alpha_mode=alpha_mode,
        value_policy=value_policy if opname == "k_find_value" else None,
    )
    try:
        if opname == "k_find_node":
            coro = client.client.k_find_node(
                key, metrics=op, alpha=alpha, alpha_mode=alpha_mode)
        elif opname == "k_find_value":
            coro = client.client.k_find_value(
                key,
                metrics=op,
                alpha=alpha,
                alpha_mode=alpha_mode,
                value_policy=value_policy,
            )
        elif opname == "k_store":
            coro = client.client.k_store(
                key, TESTVAL, metrics=op, alpha=alpha, alpha_mode=alpha_mode)
        else:
            raise ValueError("unknown op %s" % opname)
        result = await asyncio.wait_for(coro, timeout=timeout)
        if op.completed_at is None:
            op.finish_success()
        return result
    except Exception as exc:
        if op.completed_at is None:
            op.finish_failure(exc, timed_out=isinstance(exc, asyncio.TimeoutError))
        return exc


async def _run_phase(
    name,
    routing_state,
    clients,
    op_keys,
    concurrency,
    timeout,
    alpha,
    alpha_mode,
    value_policy,
):
    collector = DHTMetricsCollector()
    semaphore = asyncio.Semaphore(concurrency)
    started = time.perf_counter()

    async def _run_one(index, op_name, key):
        client = clients[index % len(clients)]
        async with semaphore:
            return await _benchmark_call(
                client,
                op_name,
                key,
                collector,
                timeout,
                name,
                routing_state,
                alpha,
                alpha_mode,
                value_policy,
            )

    tasks = [
        asyncio.create_task(_run_one(index, op_name, key))
        for index, (op_name, key) in enumerate(op_keys)
    ]
    await asyncio.gather(*tasks)
    duration_seconds = time.perf_counter() - started
    summary = _summarize_phase(name, routing_state, collector, duration_seconds)
    _print_phase_summary(summary)
    return summary


async def _warmup(clients, keys, concurrency, timeout, alpha, alpha_mode, value_policy):
    mixed = []
    for index, key in enumerate(keys):
        op = ("k_find_node", key)
        if index % 3 == 1:
            op = ("k_find_value", key)
        elif index % 3 == 2:
            op = ("k_store", key)
        mixed.append(op)
    await _run_phase(
        "warmup", "warmup", clients, mixed, concurrency, timeout,
        alpha, alpha_mode, value_policy)


async def _prepopulate_values(client, keys, timeout, alpha, alpha_mode):
    for key in keys:
        await asyncio.wait_for(
            client.client.k_store(key, TESTVAL, alpha=alpha, alpha_mode=alpha_mode),
            timeout=timeout,
        )


def _build_phase_keys(
    find_node_keys,
    find_value_keys,
    cold_store_keys,
    warm_store_keys,
    ops_per_phase,
):
    return {
        "find_node": [("k_find_node", key) for key in find_node_keys[:ops_per_phase]],
        "find_value": [("k_find_value", key) for key in find_value_keys[:ops_per_phase]],
        "cold_store": [("k_store", key) for key in cold_store_keys[:ops_per_phase]],
        "warm_store": [("k_store", key) for key in warm_store_keys[:ops_per_phase]],
    }


def _default_output_path():
    stamp = time.strftime("%Y%m%d-%H%M%S")
    return str(Path("/tmp") / f"dht-benchmark-{stamp}.json")


def _write_results(path, payload):
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return output_path


def _build_parser():
    parser = argparse.ArgumentParser(description="Benchmark flud DHT performance.")
    parser.add_argument("--nodes", type=int, default=25)
    parser.add_argument("--gateway-host", default="127.0.0.1")
    parser.add_argument("--gateway-port", type=int, default=None)
    parser.add_argument("--base-port", type=int, default=18080)
    parser.add_argument("--clients", type=int, default=4)
    parser.add_argument("--ops-per-phase", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=25)
    parser.add_argument("--key-count", type=int, default=300)
    parser.add_argument("--warmup-ops", type=int, default=150)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--alpha", type=int, default=3)
    parser.add_argument("--alpha-mode", choices=["fixed", "adaptive"], default="fixed")
    parser.add_argument("--value-policy", choices=["first", "majority"], default="first")
    parser.add_argument("--output", default=None)
    parser.add_argument("--seed", type=int, default=1337)
    parser.add_argument("--attach", action="store_true")
    parser.add_argument("--cold-only", action="store_true")
    parser.add_argument("--warm-only", action="store_true")
    return parser


async def _run_benchmark(args):
    random.seed(args.seed)
    cluster = await (_attach_benchmark_clients(args) if args.attach else _start_local_cluster(args))
    try:
        rng = random.Random(args.seed)
        required_keys = max(args.key_count, args.ops_per_phase * 4, args.warmup_ops)
        all_keys = [rng.randrange(2**256) for _ in range(required_keys)]
        find_node_keys = all_keys[: args.ops_per_phase]
        find_value_keys = all_keys[args.ops_per_phase : (2 * args.ops_per_phase)]
        cold_store_keys = all_keys[(2 * args.ops_per_phase) : (3 * args.ops_per_phase)]
        warm_store_keys = all_keys[(3 * args.ops_per_phase) : (4 * args.ops_per_phase)]
        warmup_keys = all_keys[: args.warmup_ops]

        await _prepopulate_values(
            cluster.clients[0], find_value_keys, args.timeout, args.alpha, args.alpha_mode)
        phase_keys = _build_phase_keys(
            find_node_keys,
            find_value_keys,
            cold_store_keys,
            warm_store_keys,
            args.ops_per_phase,
        )

        phase_results = []
        if not args.warm_only:
            phase_results.append(
                await _run_phase(
                    "k_find_node", "cold", cluster.clients, phase_keys["find_node"],
                    args.concurrency, args.timeout,
                    args.alpha, args.alpha_mode, args.value_policy,
                )
            )
            phase_results.append(
                await _run_phase(
                    "k_find_value", "cold", cluster.clients, phase_keys["find_value"],
                    args.concurrency, args.timeout,
                    args.alpha, args.alpha_mode, args.value_policy,
                )
            )
            phase_results.append(
                await _run_phase(
                    "k_store", "cold", cluster.clients, phase_keys["cold_store"],
                    args.concurrency, args.timeout,
                    args.alpha, args.alpha_mode, args.value_policy,
                )
            )

        if not args.cold_only:
            await _warmup(
                cluster.clients, warmup_keys, args.concurrency, args.timeout,
                args.alpha, args.alpha_mode, args.value_policy)
            phase_results.append(
                await _run_phase(
                    "k_find_node", "warm", cluster.clients, phase_keys["find_node"],
                    args.concurrency, args.timeout,
                    args.alpha, args.alpha_mode, args.value_policy,
                )
            )
            phase_results.append(
                await _run_phase(
                    "k_find_value", "warm", cluster.clients, phase_keys["find_value"],
                    args.concurrency, args.timeout,
                    args.alpha, args.alpha_mode, args.value_policy,
                )
            )
            phase_results.append(
                await _run_phase(
                    "k_store", "warm", cluster.clients, phase_keys["warm_store"],
                    args.concurrency, args.timeout,
                    args.alpha, args.alpha_mode, args.value_policy,
                )
            )

        payload = {
            "benchmark_version": 1,
            "timestamp": time.time(),
            "git_commit": _git_commit(),
            "seed": args.seed,
            "environment": {
                "python_version": platform.python_version(),
                "platform": platform.platform(),
                "hostname": socket.getfqdn(),
                "attach_mode": args.attach,
            },
            "config": {
                "nodes": args.nodes,
                "clients": args.clients,
                "concurrency": args.concurrency,
                "ops_per_phase": args.ops_per_phase,
                "warmup_ops": args.warmup_ops,
                "key_count": args.key_count,
                "timeout": args.timeout,
                "alpha": args.alpha,
                "alpha_mode": args.alpha_mode,
                "value_policy": args.value_policy,
                "gateway_host": args.gateway_host,
                "gateway_port": cluster.gateway_port,
                "base_port": args.base_port,
            },
            "phases": phase_results,
        }
        output_path = _write_results(args.output or _default_output_path(), payload)
        print("wrote benchmark results to %s" % output_path)
        return output_path
    finally:
        _stop_cluster(cluster)


def main(argv=None):
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.cold_only and args.warm_only:
        parser.error("--cold-only and --warm-only are mutually exclusive")
    asyncio.run(_run_benchmark(args))


if __name__ == "__main__":
    main()
