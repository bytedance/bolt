#!/usr/bin/env python3
# Copyright (c) ByteDance Ltd. and/or its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Spark Connect client helper used by launch-spark.sh. Two subcommands:
#   connect.py sql   --remote sc://... [-e SQL | -f FILE]    [--outputformat ...]
#   connect.py shell --remote sc://...                       (Python REPL)
#
# Shared bits: --vanilla flag, JVM crash watchdog, SparkSession bootstrap.

import argparse
import code
import json
import os
import signal
import sys
import threading
import time

from pyspark.sql import SparkSession


# ---------------------------------------------------------------------------
# JVM-death watchdog. If the Connect Server JVM (PID passed via env
# CONNECT_SERVER_PID, set by launch-spark.sh) dies — typically a native
# coredump in libbolt_backend.so / libvelox.so — kill ourselves with a clear
# stderr line so the caller doesn't hang on the half-open gRPC channel
# (pyspark's default deadlines are infinite).
# ---------------------------------------------------------------------------
def _start_jvm_watchdog() -> None:
    pid_str = os.environ.get("CONNECT_SERVER_PID", "").strip()
    if not pid_str.isdigit():
        return
    pid = int(pid_str)
    log_dir = os.environ.get("SPARK_LOG_DIR", "/tmp/spark-bolt/logs")

    def _poll() -> None:
        while True:
            try:
                os.kill(pid, 0)  # 0 = liveness probe, no signal sent
            except (ProcessLookupError, PermissionError):
                sys.stderr.write(
                    f"\n[connect-watchdog] Connect Server JVM (PID {pid}) died — "
                    f"likely a native coredump. Check {log_dir} for "
                    f"hs_err_pid*.log and the server's *.out stderr.\n"
                )
                sys.stderr.flush()
                # SIGTERM kills the main thread; sys.exit only raises in this
                # daemon thread and would leave the gRPC call hanging.
                os.kill(os.getpid(), signal.SIGTERM)
                return
            time.sleep(1)

    threading.Thread(target=_poll, name="jvm-watchdog", daemon=True).start()


def _build_session(remote: str, vanilla: bool) -> SparkSession:
    spark = SparkSession.builder.remote(remote).getOrCreate()
    if vanilla:
        spark.sql("SET spark.gluten.enabled=false")
        print(
            "==> vanilla mode: spark.gluten.enabled=false (session-only)",
            file=sys.stderr,
        )
    return spark


# ---------------------------------------------------------------------------
# SQL subcommand.
# ---------------------------------------------------------------------------
def run_sql(args) -> int:
    spark = _build_session(args.remote, args.vanilla)

    stmts: list[str] = []
    for s in args.inline:
        stmts.extend(_split(s))
    if args.file:
        with open(args.file, "r") as f:
            stmts.extend(_split(f.read()))

    if not stmts:
        print(
            "usage: connect.py sql --remote sc://... -e SQL [-e SQL ...] | -f FILE",
            file=sys.stderr,
        )
        return 2

    had_error = False
    first = True
    for stmt in stmts:
        stmt = stmt.strip()
        if not stmt:
            continue
        if not first:
            print("", file=sys.stderr, flush=True)
        first = False
        print(f"> {stmt}", file=sys.stderr, flush=True)
        t0 = time.time()
        try:
            df = spark.sql(stmt)
            _render(df, args.outputformat)
            sys.stdout.flush()
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr, flush=True)
            had_error = True
        print(f"-- elapsed: {time.time() - t0:.3f}s", file=sys.stderr, flush=True)
    return 1 if had_error else 0


def _split(text: str) -> list[str]:
    """Split SQL on semicolons, ignoring ';' inside quoted strings and
    '-- ...' line comments. Statements that end up empty after stripping
    are skipped by the caller."""
    out: list[str] = []
    buf: list[str] = []
    quote: str | None = None
    i, n = 0, len(text)
    while i < n:
        ch = text[i]
        if quote:
            buf.append(ch)
            if ch == quote:
                quote = None
        elif ch in ("'", '"'):
            quote = ch
            buf.append(ch)
        elif ch == "-" and i + 1 < n and text[i + 1] == "-":
            while i < n and text[i] != "\n":
                i += 1
            continue
        elif ch == ";":
            out.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        out.append(tail)
    return out


def _render(df, fmt: str) -> None:
    rows = df.collect()
    cols = list(df.columns)
    if fmt == "json":
        for r in rows:
            print(json.dumps({c: _jsonable(r[c]) for c in cols}, ensure_ascii=False))
    elif fmt == "csv":
        print(",".join(cols))
        for r in rows:
            print(",".join(_csv_quote(r[c]) for c in cols))
    else:  # table
        cells = [[_cell(r[c]) for c in cols] for r in rows]
        widths = [
            max(len(c), *(len(row[i]) for row in cells), 0) for i, c in enumerate(cols)
        ]

        def fmt_row(vals):
            return "| " + " | ".join(v.ljust(w) for v, w in zip(vals, widths)) + " |"

        bar = "+-" + "-+-".join("-" * w for w in widths) + "-+"
        print(bar, fmt_row(cols), bar, *map(fmt_row, cells), bar, sep="\n")


def _cell(v) -> str:
    return "NULL" if v is None else str(v)


def _csv_quote(v) -> str:
    s = _cell(v)
    if s == "NULL":
        return ""
    if any(c in s for c in ',\n"'):
        return '"' + s.replace('"', '""') + '"'
    return s


def _jsonable(v):
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    return str(v)


# ---------------------------------------------------------------------------
# Shell subcommand (Python REPL).
# ---------------------------------------------------------------------------
def run_shell(args) -> int:
    spark = _build_session(args.remote, args.vanilla)
    # Connect mode has no SparkContext; hasattr can't be used because
    # __getattr__ raises PySparkNotImplementedError instead of AttributeError.
    sc = None

    print(f"==> Connected to {args.remote}", file=sys.stderr)
    print(
        "    `spark` is your SparkSession; type `exit()` or Ctrl-D to leave.",
        file=sys.stderr,
    )

    namespace = {
        "spark": spark,
        "sc": sc,
        "SparkSession": SparkSession,
        "__name__": "__main__",
    }
    try:
        from IPython import embed

        embed(user_ns=namespace, colors="neutral")
    except ImportError:
        try:
            import readline
            import rlcompleter  # noqa: F401

            readline.set_completer(rlcompleter.Completer(namespace).complete)
            readline.parse_and_bind("tab: complete")
        except ImportError:
            pass
        code.interact(banner="", local=namespace)
    return 0


# ---------------------------------------------------------------------------
# Dispatch.
# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser(prog="connect.py", allow_abbrev=False)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_sql = sub.add_parser("sql", help="run SQL against the Connect Server")
    p_sql.add_argument(
        "--remote", required=True, help="Connect URL, e.g. sc://localhost:21002"
    )
    p_sql.add_argument(
        "--vanilla",
        action="store_true",
        help="SET spark.gluten.enabled=false on this session",
    )
    p_sql.add_argument(
        "-e",
        dest="inline",
        action="append",
        default=[],
        help="inline SQL (repeatable; ';' separates statements)",
    )
    p_sql.add_argument("-f", dest="file", help="path to a .sql file")
    p_sql.add_argument(
        "--outputformat", default="table", choices=["table", "csv", "json"]
    )

    p_shell = sub.add_parser("shell", help="Python REPL bound to the Connect Server")
    p_shell.add_argument(
        "--remote", required=True, help="Connect URL, e.g. sc://localhost:21002"
    )
    p_shell.add_argument(
        "--vanilla",
        action="store_true",
        help="SET spark.gluten.enabled=false on this session",
    )

    args, _unknown = ap.parse_known_args()

    _start_jvm_watchdog()

    if args.cmd == "sql":
        return run_sql(args)
    if args.cmd == "shell":
        return run_shell(args)
    ap.print_help(sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
