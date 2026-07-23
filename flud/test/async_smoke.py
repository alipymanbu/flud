#!/usr/bin/env python3

"""
async_smoke.py

Standalone async smoke test for Flud. This script:
1) stops nodes
2) cleans nodes
3) starts nodes
4) runs FludFileOpTest against a gateway

It uses asyncio subprocesses and returns non-zero on failure.
"""

import argparse
import asyncio
import os
import sys


async def run_cmd(cmd, cwd=None, env=None):
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        cwd=cwd,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    out, _ = await proc.communicate()
    output = out.decode("utf-8", errors="replace")
    return proc.returncode, output


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", default="8081")
    parser.add_argument("--range", default="1-10")
    parser.add_argument("--wait", type=float, default=2.0)
    parser.add_argument(
        "--use-poetry",
        choices=("auto", "always", "never"),
        default="auto",
        help="Wrap commands with 'poetry run' (auto uses poetry unless already in a venv).",
    )
    parser.add_argument("--poetry", default="poetry")
    args = parser.parse_args()

    base = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    env = os.environ.copy()

    in_venv = bool(env.get("VIRTUAL_ENV") or env.get("POETRY_ACTIVE"))
    if args.use_poetry == "always":
        use_poetry = True
    elif args.use_poetry == "never":
        use_poetry = False
    else:
        use_poetry = not in_venv

    if use_poetry:
        prefix = [args.poetry, "run"]
        stop_cmd = prefix + ["flud/bin/stop-fludnodes", args.range]
        clean_cmd = prefix + ["flud/bin/clean-fludnodes", args.range]
        start_cmd = prefix + ["flud/bin/start-fludnodes", args.range]
        test_cmd = prefix + ["flud/test/FludFileOpTest.py", args.host, args.port]
    else:
        stop_cmd = ["bash", "flud/bin/stop-fludnodes", args.range]
        clean_cmd = ["bash", "flud/bin/clean-fludnodes", args.range]
        start_cmd = ["bash", "flud/bin/start-fludnodes", args.range]
        test_cmd = [sys.executable, "flud/test/FludFileOpTest.py", args.host, args.port]

    for cmd in (stop_cmd, clean_cmd, start_cmd):
        code, output = await run_cmd(cmd, cwd=base, env=env)
        sys.stdout.write(output)
        if code != 0:
            return code
    if args.wait > 0:
        await asyncio.sleep(args.wait)

    code, output = await run_cmd(test_cmd, cwd=base, env=env)
    sys.stdout.write(output)
    return code


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
