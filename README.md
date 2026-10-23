# Reliable Data Transfer over UDP

A reliable, in-order byte-stream protocol built on UDP datagrams, a network
impairment simulator for testing it, and a file transfer client and server
that use it. Pure Python 3.8+, no third-party packages.

## Files

| File | Purpose |
|---|---|
| `rdt.py` | The reliable data transfer protocol (library). Imported by the other three files, so keep all four in the same folder. |
| `simulator.py` | UDP relay that randomly drops, corrupts, duplicates, delays and reorders packets between the client and the server. |
| `server.py` | File transfer server. Handles `put`, `get` and `list` requests, one client at a time. |
| `client.py` | File transfer client with `put`, `get` and `list` commands. |

## How it works

`rdt.py` uses **selective repeat**:

- **Sequence numbers** let the receiver reorder packets and discard duplicates.
- **CRC-32** on every packet. A corrupted packet is discarded and recovered by retransmission, like a lost one.
- **ACKs** carry a cumulative value and a selective value (`cum=4 sel=5`).
- **Per-packet timers** retransmit only the packets that were not acknowledged.
- **Sliding window** (`--window`, default 16) keeps several packets in flight.
- **Low sending rate:** the sender waits `--interval` seconds after each packet.
- **Connection handshake** (SYN / SYNACK) and a graceful close (FIN).

The file transfer sends every file with a SHA-256 hash, and both sides verify
it, so a transfer either arrives intact or is reported as failed.

## Quick start

Open three terminals in the project folder. On Windows, use `python` instead
of `python3`.

**1. Create a test file (20,000 bytes = 20 data packets):**

```
python3 -c "import os; open('test_small.bin','wb').write(os.urandom(1000))"
python3 -c "import os; open('test_large.bin','wb').write(os.urandom(20000))"
```

**2. Terminal 1 - start the server** (listens on UDP port 9000, serves the
`server_files` folder, which is created if missing):

```
python3 server.py --port 9000 --root server_files
```

**3. Terminal 2 - start the simulator** (listens on port 8000, forwards to the
server on port 9000):

```
python3 simulator.py --listen-port 8000 --server-port 9000 --loss 0.1 --corrupt 0.05 --reorder 0.2 --duplicate 0.05
```

**4. Terminal 3 - run the client through the simulator (port 8000):**

```
python3 client.py --port 8000 put test_large.bin -n test_upload.bin
python3 client.py --port 8000 list
python3 client.py --port 8000 get test_large.bin -o test_downloaded.bin
```

To skip the simulator, leave it out and use `--port 9000` on the client.

Every transfer ends with `sha256 verified` on success. The exit status of
`client.py` is 0 on success and 1 on failure. Stop the server or simulator
with Ctrl-C; the simulator then prints a summary of all packets.

## Commands

### `server.py`

```
python3 server.py [--host HOST] [--port PORT] [--root DIR] [-v] [protocol options]
```

| Option | Default | Meaning |
|---|---|---|
| `--host` | `0.0.0.0` | Address to bind. |
| `--port` | `9000` | UDP port. |
| `--root` | `server_files` | Directory to serve. |
| `-v` | off | Verbose logging (discarded corrupt packets, retransmissions, out-of-order packets). |

### `client.py`

```
python3 client.py [--host HOST] [--port PORT] [-v] [protocol options] COMMAND
```

Put the options **before** the command.

| Command | Meaning |
|---|---|
| `put FILE [-n NAME]` | Upload `FILE`. `-n` sets the name on the server (default: the file's own name). |
| `get NAME [-o PATH]` | Download `NAME`. `-o` sets the local path (default: `NAME` in the current folder). |
| `list` | List the files on the server. |

| Option | Default | Meaning |
|---|---|---|
| `--host` | `127.0.0.1` | Server (or simulator) address. |
| `--port` | `9000` | Server (or simulator) UDP port. |
| `-v` | off | Verbose logging, plus protocol statistics (`sent`, `retransmitted`, `corrupt`, ...) at the end. |

At the start of each transfer the client prints how many data packets it
needs, for example `uploading test.bin: 20000 bytes = 20 data packets`.

### `simulator.py`

```
python3 simulator.py [options]
```

| Option | Default | Meaning |
|---|---|---|
| `--listen-host`, `--listen-port` | `0.0.0.0`, `8000` | Where the simulator listens. Point the client here. |
| `--server-host`, `--server-port` | `127.0.0.1`, `9000` | Where the real server is. |
| `--loss P` | 0 | Probability a packet is dropped. |
| `--corrupt P` | 0 | Probability a packet has random bytes altered. |
| `--reorder P` | 0 | Probability a packet is delayed so later ones overtake it. |
| `--duplicate P` | 0 | Probability a packet is delivered twice. |
| `--delay S` | 0 | Fixed one-way delay in seconds. |
| `--jitter S` | 0 | Random extra delay of up to `S` seconds. |
| `--reorder-delay S` | 0.3 | Maximum extra delay for reordered packets. |
| `--seed N` | random | Seed for reproducible runs. |
| `-v` | off | Log one line per packet with its outcome. |

`P` is a probability between 0 and 1. The simulator supports one client at
a time and applies the impairments in both directions.

### Protocol options (`server.py` and `client.py`)

Use the same values on both sides, since each side paces its own sending.

| Option | Default | Meaning |
|---|---|---|
| `--mss` | 1024 | Maximum payload bytes per packet. |
| `--window` | 16 | Maximum unacknowledged packets in flight. |
| `--timeout` | 0.5 | Retransmission timeout in seconds. Keep it above the round-trip time. |
| `--interval` | 0.02 | Seconds to wait after each packet sent (sets the sending rate). |
| `--max-retries` | 20 | Retransmissions of one packet before giving up. |

### Low sending rate

The payload rate is `mss * 8 / interval` bits per second. The default
interval is fast so testing is quick. For a rate under 500 bits/s, use
`--interval 20` on **both** the server and the client:

```
python3 server.py --port 9000 --interval 20
python3 client.py --port 9000 --interval 20 put test_small.bin
```

That gives about 410 bits/s with the default packet size, so use a very small
file. ACKs, the handshake and the closing FIN are not paced.

## Showing recovery from loss, corruption and reordering

Run the server and client with `-v`, and the simulator with `-v` and one
impairment at a time. Use `--interval 0.05` on the server and client so the
output is easy to read.

```
python3 server.py --port 9000 --root server_files -v --interval 0.05
python3 simulator.py --listen-port 8000 --server-port 9000 --loss 0.2 --seed 3 -v
python3 client.py --port 8000 -v --interval 0.05 put test.bin
```

| To show | Simulator flags | What to look for |
|---|---|---|
| Corruption | `--corrupt 0.2 --seed 3` | Simulator: `CORRUPTED`. Server: `discarded corrupt packet`. Client: `timeout, retransmit seq=N`. |
| Loss | `--loss 0.2 --seed 3` | Simulator: `DROPPED`. Client: `timeout, retransmit seq=N`. |
| Reordering | `--reorder 0.4 --reorder-delay 0.4 --seed 5` | Simulator: `DELAYED`. Server: `out-of-order packet seq=7 (expected 3), buffered`. |
| Everything | `--loss 0.15 --corrupt 0.1 --reorder 0.25 --duplicate 0.1 --seed 7` | All of the above. |

In every case the client ends with `uploaded ... sha256 verified`.

Restart the server and simulator between scenarios. The simulator's `-v`
output shows one line per packet:

```
#10 client->server DATA seq=1 len=1024: DROPPED
#11 client->server DATA seq=2 len=1024: DELIVERED
#13 client->server DATA seq=3 len=1024: CORRUPTED
#21 client->server DATA seq=3 len=1024: DELAYED 0.295s
```

When you press Ctrl-C it prints a summary of packets received, delivered
unchanged, dropped, corrupted, duplicated and delayed.

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `error: no response from ...` | The server or simulator is not running, or the wrong port was used. The client retries for about 10 seconds first. |
| `error: peer unresponsive` or `idle timeout` | The other side stopped, or loss was so high that a packet used up `--max-retries`. |
| Transfer is very slow | `--interval` is large. Lower it unless you are demonstrating the low rate. |
| Many retransmissions without loss | `--timeout` is shorter than the round-trip time (for example with the simulator's `--delay`). Raise it. |
| `ModuleNotFoundError: rdt` | The four files are not in the same folder. |
| `Address already in use` | Another program is using that UDP port. Choose another port. |

