"""Network impairment simulator for testing the RDT protocol.

The simulator sits between a client and a server as a UDP relay::

    client  <-->  simulator (--listen-port)  <-->  server (--server-port)

Every datagram, in both directions, can be randomly **lost**, **corrupted**
(random bytes altered), **duplicated**, **delayed** and **reordered**
(some packets get extra delay so later packets overtake them).

Example::

    python3 simulator.py --listen-port 8000 --server-port 9000 \\
        --loss 0.1 --corrupt 0.05 --reorder 0.2 --duplicate 0.05

Then point the client at port 8000 instead of the server's port.
"""

import argparse
import collections
import heapq
import itertools
import logging
import random
import socket
import time

from rdt import MAX_DATAGRAM, describe_packet

LOG = logging.getLogger("simulator")


class Simulator:
    """A UDP relay that injects loss, corruption, reordering and more.

    The first datagram from any address other than the server marks that
    address as the client; replies from the server are relayed back to it.

    :param listen: ``(host, port)`` the relay binds to.
    :param server: ``(host, port)`` of the real server.
    :param loss: probability of dropping a packet.
    :param corrupt: probability of corrupting a packet.
    :param reorder: probability of giving a packet extra delay.
    :param duplicate: probability of sending a packet twice.
    :param delay: fixed one-way delay in seconds.
    :param jitter: extra random delay in ``[0, jitter]`` seconds.
    :param reorder_delay: maximum extra delay for reordered packets.
    :param seed: seed for reproducible runs (``None`` for random).
    """

    def __init__(self, listen, server, loss=0.0, corrupt=0.0, reorder=0.0,
                 duplicate=0.0, delay=0.0, jitter=0.0, reorder_delay=0.3,
                 seed=None):
        self.server = (socket.gethostbyname(server[0]), server[1])
        self.client = None
        self.loss = loss
        self.corrupt = corrupt
        self.reorder = reorder
        self.duplicate = duplicate
        self.delay = delay
        self.jitter = jitter
        self.reorder_delay = reorder_delay
        self.rng = random.Random(seed)
        self.stats = collections.Counter()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(listen)
        self._queue = []
        self._tiebreak = itertools.count()

    def _corrupt_bytes(self, raw):
        """Return ``raw`` with 1-3 random bytes altered."""
        data = bytearray(raw)
        for _ in range(self.rng.randint(1, 3)):
            pos = self.rng.randrange(len(data))
            data[pos] ^= self.rng.randint(1, 255)
        return bytes(data)

    def _handle(self, raw, src):
        """Apply impairments to one received datagram and schedule it.

        With ``-v`` exactly one line is logged per packet, giving its
        final outcome: ``DELIVERED`` (forwarded unchanged), ``DROPPED``, or
        a list of the impairments applied (``CORRUPTED``, ``DUPLICATED``,
        ``DELAYED``).
        """
        if src == self.server:
            dest, label = self.client, "server->client"
        else:
            self.client = src
            dest, label = self.server, "client->server"
        if dest is None:
            return
        self.stats["received"] += 1
        number = self.stats["received"]
        self.stats[label + " received"] += 1
        what = describe_packet(raw)
        if self.rng.random() < self.loss:
            self.stats["dropped"] += 1
            LOG.debug("#%d %s %s: DROPPED", number, label, what)
            return
        events = []
        if self.rng.random() < self.corrupt:
            raw = self._corrupt_bytes(raw)
            self.stats["corrupted"] += 1
            events.append("CORRUPTED")
        copies = 1
        if self.rng.random() < self.duplicate:
            copies = 2
            self.stats["duplicated"] += 1
            events.append("DUPLICATED")
        for _ in range(copies):
            wait = self.delay + self.rng.uniform(0, self.jitter)
            if self.rng.random() < self.reorder:
                wait += self.rng.uniform(0, self.reorder_delay)
                self.stats["reordered"] += 1
                events.append("DELAYED %.3fs" % wait)
            heapq.heappush(self._queue, (time.monotonic() + wait,
                                         next(self._tiebreak), raw, dest))
        if not events:
            self.stats["delivered_unchanged"] += 1
            events.append("DELIVERED")
        LOG.debug("#%d %s %s: %s", number, label, what, ", ".join(events))

    def _flush_due(self):
        """Transmit every queued datagram whose release time has passed."""
        now = time.monotonic()
        while self._queue and self._queue[0][0] <= now:
            _, _, raw, dest = heapq.heappop(self._queue)
            self.sock.sendto(raw, dest)
            self.stats["forwarded"] += 1

    def summary(self):
        """Return a multi-line summary of everything relayed so far."""
        st = self.stats
        rows = [
            ("packets received from endpoints", st["received"]),
            ("  client->server", st["client->server received"]),
            ("  server->client", st["server->client received"]),
            ("delivered unchanged", st["delivered_unchanged"]),
            ("dropped", st["dropped"]),
            ("corrupted", st["corrupted"]),
            ("duplicated", st["duplicated"]),
            ("delayed (reordering)", st["reordered"]),
            ("datagrams forwarded (incl. duplicates)", st["forwarded"]),
        ]
        lines = ["", "======== simulator summary ========"]
        lines += ["%-42s %d" % row for row in rows]
        lines.append("===================================")
        return "\n".join(lines)

    def run(self):
        """Relay packets until interrupted (Ctrl-C)."""
        LOG.info("relaying %s -> %s", self.sock.getsockname(), self.server)
        try:
            while True:
                wait = 0.05
                if self._queue:
                    wait = min(wait, self._queue[0][0] - time.monotonic())
                self.sock.settimeout(max(wait, 0.001))
                try:
                    raw, src = self.sock.recvfrom(MAX_DATAGRAM)
                    self._handle(raw, src)
                except socket.timeout:
                    pass
                self._flush_due()
        except KeyboardInterrupt:
            pass
        finally:
            LOG.info(self.summary())
            self.sock.close()


def probability(text):
    """argparse type for a probability in ``[0, 1]``."""
    value = float(text)
    if not 0.0 <= value <= 1.0:
        raise argparse.ArgumentTypeError("must be between 0 and 1")
    return value


def main():
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        description="UDP relay that simulates an unreliable network.")
    parser.add_argument("--listen-host", default="0.0.0.0")
    parser.add_argument("--listen-port", type=int, default=8000)
    parser.add_argument("--server-host", default="127.0.0.1")
    parser.add_argument("--server-port", type=int, default=9000)
    parser.add_argument("--loss", type=probability, default=0.0,
                        help="packet loss probability (default: 0)")
    parser.add_argument("--corrupt", type=probability, default=0.0,
                        help="packet corruption probability (default: 0)")
    parser.add_argument("--reorder", type=probability, default=0.0,
                        help="probability a packet is delayed so that "
                             "later ones overtake it (default: 0)")
    parser.add_argument("--duplicate", type=probability, default=0.0,
                        help="packet duplication probability (default: 0)")
    parser.add_argument("--delay", type=float, default=0.0,
                        help="fixed one-way delay in seconds")
    parser.add_argument("--jitter", type=float, default=0.0,
                        help="random extra delay up to this many seconds")
    parser.add_argument("--reorder-delay", type=float, default=0.3,
                        help="max extra delay for reordered packets "
                             "(default: %(default)s)")
    parser.add_argument("--seed", type=int, default=None,
                        help="random seed for reproducible runs")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="log every packet decision")
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s: %(message)s")
    sim = Simulator((args.listen_host, args.listen_port),
                    (args.server_host, args.server_port),
                    loss=args.loss, corrupt=args.corrupt,
                    reorder=args.reorder, duplicate=args.duplicate,
                    delay=args.delay, jitter=args.jitter,
                    reorder_delay=args.reorder_delay, seed=args.seed)
    sim.run()


if __name__ == "__main__":
    main()