"""File transfer client built on the reliable data transfer protocol.

Examples::

    python3 client.py --host 127.0.0.1 --port 9000 put report.pdf
    python3 client.py --host 127.0.0.1 --port 9000 get report.pdf -o out.pdf
    python3 client.py --host 127.0.0.1 --port 9000 list

Use the simulator's port instead of the server's to test over a lossy
network.  The exit status is 0 on success and 1 on failure.
"""

import argparse
import hashlib
import json
import logging
import os
import sys
import time

import rdt

CHUNK = 64 * 1024
MAX_HEADER = 4096


def send_json(conn, **fields):
    """Send one JSON line to the server."""
    conn.send(json.dumps(fields).encode() + b"\n")


def read_response(conn):
    """Read a JSON response line, raising on an error status.

    :return: the decoded response dictionary.
    :raises RuntimeError: if the server reported an error.
    """
    response = json.loads(conn.recv_line(MAX_HEADER))
    if response.get("status") != "ok":
        raise RuntimeError(response.get("message", "server error"))
    return response


def sha256_of(path):
    """Return the hex SHA-256 of a file."""
    digest = hashlib.sha256()
    with open(path, "rb") as src:
        for block in iter(lambda: src.read(CHUNK), b""):
            digest.update(block)
    return digest.hexdigest()


def progress(done, total, started):
    """Print a one-line progress indicator to stderr."""
    pct = 100.0 if total == 0 else 100.0 * done / total
    rate = done / max(time.monotonic() - started, 1e-6) / 1024
    sys.stderr.write("\r  %6.1f%%  %d/%d bytes  %.1f KiB/s"
                     % (pct, done, total, rate))
    sys.stderr.flush()


def put_file(conn, path, remote_name):
    """Upload ``path`` to the server as ``remote_name``."""
    size = os.path.getsize(path)
    digest = sha256_of(path)
    send_json(conn, cmd="PUT", name=remote_name, size=size, sha256=digest)
    read_response(conn)  # server is ready (or rejected the request)
    started = time.monotonic()
    sent = 0
    with open(path, "rb") as src:
        for block in iter(lambda: src.read(CHUNK), b""):
            conn.send(block)
            sent += len(block)
            progress(sent, size, started)
    if size == 0:
        progress(0, 0, started)
    sys.stderr.write("\n")
    result = read_response(conn)
    if result.get("sha256") != digest:
        raise RuntimeError("server stored a different file")
    print("uploaded %s (%d bytes, sha256 verified)" % (remote_name, size))


def get_file(conn, remote_name, dest):
    """Download ``remote_name`` from the server into ``dest``."""
    send_json(conn, cmd="GET", name=remote_name)
    meta = read_response(conn)
    size, expected = meta["size"], meta["sha256"]
    temp = dest + ".part"
    digest = hashlib.sha256()
    started = time.monotonic()
    remaining = size
    with open(temp, "wb") as out:
        while remaining > 0:
            chunk = conn.recv(min(CHUNK, remaining))
            if not chunk:
                raise EOFError("server closed during download")
            out.write(chunk)
            digest.update(chunk)
            remaining -= len(chunk)
            progress(size - remaining, size, started)
    sys.stderr.write("\n")
    if digest.hexdigest() != expected:
        os.remove(temp)
        raise RuntimeError("checksum mismatch, download discarded")
    os.replace(temp, dest)
    print("downloaded %s -> %s (%d bytes, sha256 verified)"
          % (remote_name, dest, size))


def list_files(conn):
    """Print the server's file listing."""
    send_json(conn, cmd="LIST")
    meta = read_response(conn)
    body = conn.recv_exact(meta["size"]).decode()
    print(body if body else "(no files)")


def main():
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        description="File transfer client over reliable UDP.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("-v", "--verbose", action="store_true")
    rdt.add_protocol_arguments(parser)
    sub = parser.add_subparsers(dest="command", required=True)

    put = sub.add_parser("put", help="upload a file")
    put.add_argument("file", help="local file to upload")
    put.add_argument("-n", "--name", help="name on the server")

    get = sub.add_parser("get", help="download a file")
    get.add_argument("name", help="file name on the server")
    get.add_argument("-o", "--output", help="local destination path")

    sub.add_parser("list", help="list files on the server")

    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(asctime)s %(name)s: %(message)s")

    status = 0
    conn = None
    try:
        conn = rdt.connect((args.host, args.port),
                           rdt.config_from_args(args))
        if args.command == "put":
            name = args.name or os.path.basename(args.file)
            put_file(conn, args.file, name)
        elif args.command == "get":
            get_file(conn, args.name, args.output or args.name)
        else:
            list_files(conn)
    except (OSError, EOFError, RuntimeError, ValueError) as exc:
        print("error: %s" % exc, file=sys.stderr)
        status = 1
    finally:
        if conn is not None:
            conn.close()
            if args.verbose:
                print("stats:", dict(conn.stats), file=sys.stderr)
    sys.exit(status)


if __name__ == "__main__":
    main()
