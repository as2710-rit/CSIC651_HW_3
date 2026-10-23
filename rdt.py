"""Reliable data transfer (RDT) over UDP.

This module implements a small connection-oriented, reliable, in-order byte
stream on top of UDP datagrams.  It uses a *selective repeat* design:

* **Sequence numbers** - every DATA/FIN packet carries a 32-bit sequence
  number, so the receiver can reorder packets and drop duplicates.
* **Integrity** - every packet carries a CRC-32 over header and payload.
  Packets failing the check are silently discarded (and so are recovered by
  retransmission, exactly like a lost packet).
* **Acknowledgements** - every ACK carries both a cumulative value (all
  sequence numbers below it were received) and a selective value (the exact
  packet that triggered the ACK).
* **Timeouts / retransmission** - each unacknowledged packet has its own
  timer and is retransmitted when it expires.
* **Pipelining** - up to ``window`` packets may be in flight at once.
* **Pacing** - the sender waits ``interval`` seconds between transmissions
  to keep the sending rate low (no congestion control is implemented).
* **Connection ids** - a random id in every header keeps stale packets from
  an earlier connection from leaking into a new one.

Packet layout (network byte order)::

    +------+---------+---------+---------+--------+---------+---------+
    | type | conn id |   seq   |   ack   | length |  CRC-32 | payload |
    |  1 B |   4 B   |   4 B   |   4 B   |  2 B   |   4 B   |  0..MSS |
    +------+---------+---------+---------+--------+---------+---------+

For ACK packets ``seq`` is the cumulative acknowledgement (next sequence
number the receiver expects) and ``ack`` is the sequence number being
selectively acknowledged.

Typical use::

    # server
    listener = Listener("0.0.0.0", 9000)
    conn = listener.accept()
    data = conn.recv()
    conn.send(b"reply")
    conn.close()

    # client
    conn = connect(("127.0.0.1", 9000))
    conn.send(b"hello")
    conn.close()
"""

import collections
import logging
import random
import socket
import struct
import threading
import time
import zlib
from dataclasses import dataclass

LOG = logging.getLogger("rdt")

# Packet types.
SYN = 1
SYNACK = 2
DATA = 3
ACK = 4
FIN = 5
TYPE_NAMES = {SYN: "SYN", SYNACK: "SYNACK", DATA: "DATA", ACK: "ACK",
              FIN: "FIN"}

HEADER = struct.Struct("!BIIIHI")
MAX_DATAGRAM = 65535
# The receiver buffers out-of-order packets this many windows ahead.
RECV_WINDOW_FACTOR = 4
POLL = 0.05

Packet = collections.namedtuple(
    "Packet", "ptype conn_id seq ack payload")


@dataclass
class Config:
    """Tunable protocol parameters.

    :ivar mss: maximum payload bytes per packet.
    :ivar window: maximum number of unacknowledged packets in flight.
    :ivar timeout: retransmission timeout in seconds.
    :ivar interval: pause in seconds after every packet transmission
        (the low-rate pacing required by the assignment).
    :ivar max_retries: retransmissions of one packet before giving up.
    :ivar idle_timeout: seconds of silence before the peer is declared dead.
    """

    mss: int = 1024
    window: int = 16
    timeout: float = 0.5
    interval: float = 0.02
    max_retries: int = 20
    idle_timeout: float = 30.0


def add_protocol_arguments(parser):
    """Add the shared protocol tuning options to an ``ArgumentParser``.

    :param parser: the :class:`argparse.ArgumentParser` to extend.
    """
    defaults = Config()
    group = parser.add_argument_group("protocol options")
    group.add_argument("--mss", type=int, default=defaults.mss,
                       help="max payload bytes per packet "
                            "(default: %(default)s)")
    group.add_argument("--window", type=int, default=defaults.window,
                       help="max unacknowledged packets in flight "
                            "(default: %(default)s)")
    group.add_argument("--timeout", type=float, default=defaults.timeout,
                       help="retransmission timeout in seconds "
                            "(default: %(default)s)")
    group.add_argument("--interval", type=float, default=defaults.interval,
                       help="seconds to wait between packet transmissions; "
                            "raise it for a lower sending rate "
                            "(default: %(default)s)")
    group.add_argument("--max-retries", type=int,
                       default=defaults.max_retries,
                       help="retransmissions before giving up "
                            "(default: %(default)s)")


def config_from_args(args):
    """Build a :class:`Config` from parsed command-line arguments.

    :param args: namespace produced by a parser extended with
        :func:`add_protocol_arguments`.
    :return: the resulting :class:`Config`.
    """
    return Config(mss=args.mss, window=args.window, timeout=args.timeout,
                  interval=args.interval, max_retries=args.max_retries)


def encode_packet(ptype, conn_id, seq=0, ack=0, payload=b""):
    """Serialise a packet and append its CRC-32.

    :param ptype: one of the packet type constants.
    :param conn_id: 32-bit connection identifier.
    :param seq: sequence number (cumulative ack value for ACK packets).
    :param ack: selectively acknowledged sequence number (ACK only).
    :param payload: payload bytes.
    :return: the datagram as ``bytes``.
    """
    header = HEADER.pack(ptype, conn_id, seq, ack, len(payload), 0)
    crc = zlib.crc32(header + payload) & 0xFFFFFFFF
    header = HEADER.pack(ptype, conn_id, seq, ack, len(payload), crc)
    return header + payload


def decode_packet(raw):
    """Parse and verify a datagram.

    :param raw: the received datagram.
    :return: a :class:`Packet`, or ``None`` if it is malformed or corrupt.
    """
    if len(raw) < HEADER.size:
        return None
    ptype, conn_id, seq, ack, length, crc = HEADER.unpack_from(raw)
    payload = raw[HEADER.size:]
    if ptype not in TYPE_NAMES or len(payload) != length:
        return None
    zeroed = HEADER.pack(ptype, conn_id, seq, ack, length, 0)
    if zlib.crc32(zeroed + payload) & 0xFFFFFFFF != crc:
        return None
    return Packet(ptype, conn_id, seq, ack, payload)


def describe_packet(raw):
    """Return a short human-readable description of a datagram.

    The checksum is *not* verified, so corrupt packets can be described too.

    :param raw: the datagram.
    :return: a one-line description, used for debug logging.
    """
    if len(raw) < HEADER.size:
        return "runt(%d bytes)" % len(raw)
    ptype, _, seq, ack, length, _ = HEADER.unpack_from(raw)
    name = TYPE_NAMES.get(ptype, "?%d" % ptype)
    if ptype == ACK:
        return "ACK cum=%d sel=%d" % (seq, ack)
    return "%s seq=%d len=%d" % (name, seq, length)


class _Entry:
    """A packet that has been sent but not yet acknowledged."""

    __slots__ = ("raw", "sent_at", "retries", "size")

    def __init__(self, raw, sent_at, size=0):
        self.raw = raw
        self.sent_at = sent_at
        self.retries = 0
        self.size = size  # payload bytes carried (DATA packets only)


class Connection:
    """One reliable, in-order, full-duplex byte stream to a single peer.

    Two background threads do the protocol work: a *sender* thread that
    paces transmissions and handles retransmission timers, and a *receiver*
    thread that verifies, orders and acknowledges incoming packets.
    Applications use the blocking :meth:`send`, :meth:`recv`,
    :meth:`recv_exact`, :meth:`recv_line` and :meth:`close` methods.

    Instances are created by :func:`connect` and :meth:`Listener.accept`.

    :ivar stats: counters (``sent``, ``retransmitted``, ``received``,
        ``corrupt``, ``duplicates``) useful for evaluating the protocol.
    """

    def __init__(self, sock, peer, conn_id, config, owns_socket=False):
        self._sock = sock
        self._peer = peer
        self.conn_id = conn_id
        self.cfg = config
        self._owns_socket = owns_socket
        self._cond = threading.Condition()

        # Sender state.
        self._unsent = collections.deque()
        self._inflight = {}
        self._next_seq = 0
        self._fin_queued = False

        # Receiver state.
        self._rcv_base = 0
        self._rcv_buf = {}
        self._inbox = bytearray()
        self._peer_fin = False

        self._failed = None
        self._stopping = False
        self._last_heard = time.monotonic()
        self.stats = collections.Counter()

        self._threads = [
            threading.Thread(target=self._send_loop, daemon=True),
            threading.Thread(target=self._recv_loop, daemon=True),
        ]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    @property
    def peer(self):
        """Address ``(host, port)`` of the remote endpoint."""
        return self._peer

    def start(self):
        """Start the background threads (called by connect/accept)."""
        self._sock.settimeout(POLL)
        for thread in self._threads:
            thread.start()

    def send(self, data):
        """Queue ``data`` for reliable delivery.

        Blocks while the send queue is full, which throttles the caller to
        the pace of the protocol.

        :param data: bytes-like object of any length.
        :raises ConnectionError: if the connection failed or was closed.
        """
        view = memoryview(data)
        limit = self.cfg.window * 2
        with self._cond:
            for off in range(0, len(view), self.cfg.mss):
                while len(self._unsent) >= limit and not self._failed:
                    self._cond.wait(POLL)
                self._check_writable()
                chunk = bytes(view[off:off + self.cfg.mss])
                self._unsent.append((DATA, chunk))
                self._cond.notify_all()

    def recv(self, max_bytes=65536):
        """Receive up to ``max_bytes`` of in-order data.

        :param max_bytes: upper bound on the returned length.
        :return: the data, or ``b""`` once the peer closed its side and all
            data has been read.
        :raises ConnectionError: if the connection failed.
        """
        with self._cond:
            while not self._inbox:
                if self._peer_fin:
                    return b""
                if self._failed:
                    raise ConnectionError(self._failed)
                self._cond.wait(POLL)
            data = bytes(self._inbox[:max_bytes])
            del self._inbox[:max_bytes]
            return data

    def recv_exact(self, count):
        """Receive exactly ``count`` bytes.

        :param count: number of bytes to read.
        :return: the bytes read.
        :raises EOFError: if the peer closed before ``count`` bytes arrived.
        """
        parts = []
        remaining = count
        while remaining > 0:
            chunk = self.recv(remaining)
            if not chunk:
                raise EOFError("connection closed by peer")
            parts.append(chunk)
            remaining -= len(chunk)
        return b"".join(parts)

    @property
    def bytes_acked(self):
        """Payload bytes the peer has acknowledged so far."""
        return self.stats["acked_bytes"]

    def flush(self, on_wait=None):
        """Block until every queued packet has been acknowledged.

        Unlike :meth:`send`, which returns once data is *queued*, this
        returns only when the peer has confirmed receipt, so the elapsed
        time reflects the real transfer speed.

        :param on_wait: optional callable invoked about every 50 ms while
            waiting (used for progress display).
        :raises ConnectionError: if the connection fails while waiting.
        """
        with self._cond:
            while self._unsent or self._inflight:
                if self._failed:
                    raise ConnectionError(self._failed)
                self._cond.wait(POLL)
                if on_wait:
                    on_wait()

    def recv_line(self, max_length=65536):
        """Receive one ``\\n``-terminated line.

        :param max_length: longest accepted line, newline included.
        :return: the line without its newline.
        :raises EOFError: if the peer closed before a full line arrived.
        :raises ValueError: if the line exceeds ``max_length``.
        """
        with self._cond:
            while True:
                idx = self._inbox.find(b"\n")
                if idx >= 0:
                    line = bytes(self._inbox[:idx])
                    del self._inbox[:idx + 1]
                    return line
                if len(self._inbox) >= max_length:
                    raise ValueError("line too long")
                if self._peer_fin:
                    raise EOFError("connection closed by peer")
                if self._failed:
                    raise ConnectionError(self._failed)
                self._cond.wait(POLL)

    def close(self):
        """Gracefully close the connection.

        Sends a FIN after all queued data, waits for everything to be
        acknowledged and for the peer's FIN, then lingers briefly so that
        a lost final ACK can still be repeated.  Never raises.
        """
        cfg = self.cfg
        with self._cond:
            if not self._fin_queued and not self._failed:
                self._fin_queued = True
                self._unsent.append((FIN, b""))
                self._cond.notify_all()
            while (self._unsent or self._inflight) and not self._failed:
                self._cond.wait(POLL)
            deadline = time.monotonic() + cfg.timeout * cfg.max_retries
            while (not self._peer_fin and not self._failed
                   and time.monotonic() < deadline):
                self._cond.wait(POLL)
            clean = self._peer_fin and not self._failed
        if clean:
            time.sleep(cfg.timeout * 3)
        with self._cond:
            self._stopping = True
            self._cond.notify_all()
        for thread in self._threads:
            thread.join(timeout=1.0)
        if self._owns_socket:
            self._sock.close()
        LOG.info("connection closed: %s", dict(self.stats))

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _check_writable(self):
        """Raise if the connection can no longer accept data (locked)."""
        if self._failed:
            raise ConnectionError(self._failed)
        if self._fin_queued:
            raise ConnectionError("connection is closed for writing")

    def _fail(self, reason):
        """Mark the connection as failed and wake all waiters (locked)."""
        if not self._failed:
            LOG.warning("connection failed: %s", reason)
            self._failed = reason
        self._cond.notify_all()

    def _next_packet(self):
        """Choose the next datagram to transmit (called with lock held).

        Expired retransmissions take priority over new data.

        :return: raw datagram, or ``None`` if there is nothing to send now.
        """
        now = time.monotonic()
        for seq in sorted(self._inflight):
            entry = self._inflight[seq]
            if now - entry.sent_at >= self.cfg.timeout:
                entry.retries += 1
                if entry.retries > self.cfg.max_retries:
                    self._fail("peer unresponsive (seq %d)" % seq)
                    return None
                entry.sent_at = now
                self.stats["retransmitted"] += 1
                LOG.debug("timeout, retransmit seq=%d", seq)
                return entry.raw
        if self._unsent:
            base = min(self._inflight) if self._inflight else self._next_seq
            if self._next_seq - base < self.cfg.window:
                ptype, payload = self._unsent.popleft()
                seq = self._next_seq
                self._next_seq += 1
                raw = encode_packet(ptype, self.conn_id, seq, 0, payload)
                self._inflight[seq] = _Entry(
                    raw, now, len(payload) if ptype == DATA else 0)
                self.stats["sent"] += 1
                self._cond.notify_all()
                return raw
        return None

    def _send_loop(self):
        """Sender thread: paced transmission and retransmission."""
        while True:
            with self._cond:
                if self._stopping or self._failed:
                    return
                raw = self._next_packet()
                if raw is None:
                    self._cond.wait(POLL)
                    continue
            try:
                self._sock.sendto(raw, self._peer)
            except OSError as exc:
                with self._cond:
                    self._fail("socket error: %s" % exc)
                return
            time.sleep(self.cfg.interval)

    def _recv_loop(self):
        """Receiver thread: verify, dispatch and acknowledge packets."""
        idle_limit = max(self.cfg.idle_timeout, 10 * self.cfg.interval)
        while True:
            with self._cond:
                if self._stopping or self._failed:
                    return
            try:
                raw, addr = self._sock.recvfrom(MAX_DATAGRAM)
            except socket.timeout:
                if time.monotonic() - self._last_heard > idle_limit:
                    with self._cond:
                        self._fail("idle timeout")
                continue
            except OSError:
                return
            if addr != self._peer:
                continue
            pkt = decode_packet(raw)
            if pkt is None:
                self.stats["corrupt"] += 1
                LOG.debug("discarded corrupt packet")
                continue
            if pkt.conn_id != self.conn_id:
                continue
            self._last_heard = time.monotonic()
            self.stats["received"] += 1
            if pkt.ptype in (DATA, FIN):
                self._on_data(pkt)
            elif pkt.ptype == ACK:
                self._on_ack(pkt)
            elif pkt.ptype == SYN:
                # Our SYNACK was lost; the peer is still handshaking.
                self._send_raw(encode_packet(SYNACK, self.conn_id))

    def _send_raw(self, raw):
        """Transmit a datagram immediately, ignoring socket errors."""
        try:
            self._sock.sendto(raw, self._peer)
        except OSError:
            pass

    def _on_data(self, pkt):
        """Handle an incoming DATA or FIN packet."""
        ack = None
        with self._cond:
            limit = self._rcv_base + self.cfg.window * RECV_WINDOW_FACTOR
            if pkt.seq < limit:
                if pkt.seq < self._rcv_base or pkt.seq in self._rcv_buf:
                    self.stats["duplicates"] += 1
                else:
                    if pkt.seq > self._rcv_base:
                        LOG.debug("out-of-order packet seq=%d (expected "
                                  "%d), buffered", pkt.seq, self._rcv_base)
                    self._rcv_buf[pkt.seq] = (pkt.ptype, pkt.payload)
                while self._rcv_base in self._rcv_buf:
                    ptype, payload = self._rcv_buf.pop(self._rcv_base)
                    if ptype == DATA:
                        self._inbox += payload
                    else:
                        self._peer_fin = True
                    self._rcv_base += 1
                self._cond.notify_all()
                ack = encode_packet(ACK, self.conn_id, self._rcv_base,
                                    pkt.seq)
        if ack:
            self._send_raw(ack)

    def _on_ack(self, pkt):
        """Handle an incoming ACK (cumulative and selective parts)."""
        with self._cond:
            done = [s for s in self._inflight
                    if s < pkt.seq or s == pkt.ack]
            for seq in done:
                self.stats["acked_bytes"] += self._inflight[seq].size
                del self._inflight[seq]
            if done:
                self._cond.notify_all()


def _resolve(address):
    """Resolve ``(host, port)`` to an IPv4 ``(ip, port)`` tuple."""
    host, port = address
    return socket.gethostbyname(host), int(port)


def connect(address, config=None):
    """Open a reliable connection to a :class:`Listener`.

    Performs a SYN / SYNACK handshake, retransmitting the SYN on timeout.

    :param address: ``(host, port)`` of the server (or simulator).
    :param config: optional :class:`Config`.
    :return: an established :class:`Connection`.
    :raises ConnectionError: if the peer does not answer.
    """
    cfg = config or Config()
    peer = _resolve(address)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    conn_id = random.getrandbits(32)
    syn = encode_packet(SYN, conn_id)
    try:
        for _ in range(cfg.max_retries):
            sock.sendto(syn, peer)
            deadline = time.monotonic() + cfg.timeout
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                sock.settimeout(remaining)
                try:
                    raw, addr = sock.recvfrom(MAX_DATAGRAM)
                except socket.timeout:
                    break
                pkt = decode_packet(raw)
                if (addr == peer and pkt and pkt.ptype == SYNACK
                        and pkt.conn_id == conn_id):
                    conn = Connection(sock, peer, conn_id, cfg,
                                      owns_socket=True)
                    conn.start()
                    LOG.info("connected to %s:%d", *peer)
                    return conn
    except BaseException:
        sock.close()
        raise
    sock.close()
    raise ConnectionError("no response from %s:%d" % peer)


class Listener:
    """A UDP socket that accepts reliable connections, one at a time.

    :param host: address to bind to.
    :param port: UDP port to bind to.
    :param config: optional :class:`Config` used for accepted connections.
    """

    def __init__(self, host="0.0.0.0", port=9000, config=None):
        self.cfg = config or Config()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((host, port))

    def accept(self, timeout=None):
        """Wait for a client and complete the handshake.

        :param timeout: seconds to wait, or ``None`` to wait forever.
        :return: a new :class:`Connection`.
        :raises TimeoutError: if ``timeout`` expires first.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        self.sock.settimeout(POLL * 4)
        while True:
            if deadline is not None and time.monotonic() > deadline:
                raise TimeoutError("accept timed out")
            try:
                raw, addr = self.sock.recvfrom(MAX_DATAGRAM)
            except socket.timeout:
                continue
            pkt = decode_packet(raw)
            if pkt is None or pkt.ptype != SYN:
                continue
            self.sock.sendto(encode_packet(SYNACK, pkt.conn_id), addr)
            conn = Connection(self.sock, addr, pkt.conn_id, self.cfg)
            conn.start()
            LOG.info("accepted connection from %s:%d", *addr)
            return conn

    def close(self):
        """Close the listening socket."""
        self.sock.close()