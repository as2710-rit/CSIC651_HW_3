"""File transfer server built on the reliable data transfer protocol.

Serves files from a root directory.  Requests are single JSON lines
followed by (optionally) raw file bytes::

    {"cmd": "PUT", "name": "a.txt", "size": 123, "sha256": "..."}
    {"cmd": "GET", "name": "a.txt"}
    {"cmd": "LIST"}

Responses are a JSON line (``{"status": "ok", ...}`` or
``{"status": "error", "message": "..."}``), followed by file bytes for GET
and LIST.  File contents are verified end to end with SHA-256.

Clients are served one at a time.

Example::

    python3 server.py --port 9000 --root ./server_files
"""

import argparse
import hashlib
import json
import logging
import math
import os

import rdt

LOG = logging.getLogger("server")
CHUNK = 64 * 1024
MAX_HEADER = 4096


def send_json(conn, **fields):
    """Send one JSON line to the peer."""
    conn.send(json.dumps(fields).encode() + b"\n")


def safe_path(root, name):
    """Map a client-supplied file name into ``root``.

    :param root: the served directory.
    :param name: name requested by the client.
    :return: absolute path inside ``root``.
    :raises ValueError: for anything but a plain file name (this blocks
        path traversal such as ``../x`` or absolute paths).
    """
    if (not isinstance(name, str) or not name or name in (".", "..")
            or os.path.basename(name) != name or "\x00" in name
            or "\\" in name):
        raise ValueError("invalid file name")
    return os.path.join(root, name)


def handle_put(conn, root, request):
    """Receive a file from the client and store it in ``root``."""
    path = safe_path(root, request.get("name"))
    size = request.get("size")
    expected = request.get("sha256")
    if not isinstance(size, int) or size < 0:
        raise ValueError("invalid size")
    LOG.info("receiving %s: %d bytes = %d data packets",
             request["name"], size, math.ceil(size / conn.cfg.mss))
    send_json(conn, status="ok", message="ready")
    temp = path + ".part"
    digest = hashlib.sha256()
    remaining = size
    with open(temp, "wb") as out:
        while remaining > 0:
            chunk = conn.recv(min(CHUNK, remaining))
            if not chunk:
                raise EOFError("client closed during upload")
            out.write(chunk)
            digest.update(chunk)
            remaining -= len(chunk)
    if expected and digest.hexdigest() != expected:
        os.remove(temp)
        send_json(conn, status="error", message="checksum mismatch")
        LOG.error("PUT %s: checksum mismatch", request["name"])
        return
    os.replace(temp, path)
    send_json(conn, status="ok", message="stored", size=size,
              sha256=digest.hexdigest())
    LOG.info("PUT %s (%d bytes)", request["name"], size)


def handle_get(conn, root, request):
    """Send a file from ``root`` to the client."""
    path = safe_path(root, request.get("name"))
    if not os.path.isfile(path):
        raise FileNotFoundError("no such file")
    digest = hashlib.sha256()
    with open(path, "rb") as src:
        for block in iter(lambda: src.read(CHUNK), b""):
            digest.update(block)
    size = os.path.getsize(path)
    LOG.info("sending %s: %d bytes = %d data packets",
             request["name"], size, math.ceil(size / conn.cfg.mss))
    send_json(conn, status="ok", size=size, sha256=digest.hexdigest())
    with open(path, "rb") as src:
        for block in iter(lambda: src.read(CHUNK), b""):
            conn.send(block)
    LOG.info("GET %s (%d bytes)", request["name"], size)


def handle_list(conn, root, _request):
    """Send a directory listing (``name<TAB>size`` per line)."""
    lines = []
    for name in sorted(os.listdir(root)):
        full = os.path.join(root, name)
        if os.path.isfile(full) and not name.endswith(".part"):
            lines.append("%s\t%d" % (name, os.path.getsize(full)))
    body = "\n".join(lines).encode()
    send_json(conn, status="ok", size=len(body))
    conn.send(body)
    LOG.info("LIST (%d files)", len(lines))


HANDLERS = {"PUT": handle_put, "GET": handle_get, "LIST": handle_list}


def serve_connection(conn, root):
    """Read one request from ``conn`` and answer it."""
    try:
        request = json.loads(conn.recv_line(MAX_HEADER))
        handler = HANDLERS.get(request.get("cmd"))
        if handler is None:
            raise ValueError("unknown command")
        handler(conn, root, request)
    except (ValueError, FileNotFoundError, PermissionError) as exc:
        LOG.warning("request failed: %s", exc)
        try:
            send_json(conn, status="error", message=str(exc))
        except ConnectionError:
            pass
    except (ConnectionError, EOFError) as exc:
        LOG.warning("connection problem: %s", exc)


def main():
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        description="File transfer server over reliable UDP.")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--root", default="server_files",
                        help="directory to serve (default: %(default)s)")
    parser.add_argument("-v", "--verbose", action="store_true")
    rdt.add_protocol_arguments(parser)
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s: %(message)s")
    os.makedirs(args.root, exist_ok=True)
    listener = rdt.Listener(args.host, args.port,
                            rdt.config_from_args(args))
    LOG.info("serving %s on UDP port %d", os.path.abspath(args.root),
             args.port)
    try:
        while True:
            conn = listener.accept()
            try:
                serve_connection(conn, args.root)
            finally:
                conn.close()
    except KeyboardInterrupt:
        LOG.info("shutting down")
    finally:
        listener.close()


if __name__ == "__main__":
    main()