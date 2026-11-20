# floating_ip.py: the leader holds one extra address (config.FLOATING_IP) so clients always
# open the same URL, whichever machine leads. Leadership changes in several places
# (election._claim, election.on_coordinator, monitor), so instead of hooking into each one,
# a loop compares "should I hold it?" with "do I hold it?" once a second and fixes the
# difference. Running it again when nothing changed does nothing.
#
# Supported: macOS (ifconfig alias), Windows (netsh) and Linux (ip addr, used for testing).
# Adding the address makes the operating system announce it on the network (a gratuitous ARP
# on macOS and Linux, duplicate-address detection then an announcement on Windows), so the
# phone and router learn which machine now owns it.
from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import sys

from ds1 import state as s
from ds1.config import FLOATING_IP

CHECK_INTERVAL = 1.0        # seconds between checks; the IP follows the leader within this
RETRY_AFTER = 5.0           # after a failed add, wait this long before trying again

iface: str | None = None    # network adapter, from --iface; None turns the feature off
ip: str = FLOATING_IP


# ---------- the operating-system commands ----------

def _sudo() -> list[str]:
    """macOS/Linux need root to change addresses. 'sudo -n' fails at once instead of waiting
    for a password nobody will type; see the sudoers rule in the README."""
    return [] if os.geteuid() == 0 else ["sudo", "-n"]


def _add_cmd() -> list[str]:
    if sys.platform == "win32":
        return ["netsh", "interface", "ipv4", "add", "address", iface, ip, "255.255.255.255"]
    if sys.platform == "darwin":
        return _sudo() + ["ifconfig", iface, "alias", ip, "netmask", "255.255.255.255"]
    return _sudo() + ["ip", "addr", "add", f"{ip}/32", "dev", iface]


def _delete_cmd() -> list[str]:
    if sys.platform == "win32":
        return ["netsh", "interface", "ipv4", "delete", "address", iface, ip]
    if sys.platform == "darwin":
        return _sudo() + ["ifconfig", iface, "-alias", ip]
    return _sudo() + ["ip", "addr", "del", f"{ip}/32", "dev", iface]


def _run(cmd: list[str]) -> tuple[bool, str]:
    """Run a command, return (succeeded, its output). Blocking: call through to_thread."""
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        return p.returncode == 0, (p.stdout + p.stderr).strip()
    except (OSError, subprocess.TimeoutExpired) as e:
        return False, str(e)


def holds() -> bool:
    """Do we really own the address right now? Binding a socket to it only works if the
    operating system has it on an adapter and is ready to use it. That also catches the
    Windows case where the address was added but marked 'Duplicate' because another
    machine still answers for it."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
            probe.bind((ip, 0))
        return True
    except OSError:
        return False


def release_now(log) -> None:
    """Drop the address. Safe to call when we don't hold it. Used at startup (a crash may
    have left it behind) and at shutdown (so Ctrl+C hands it over quickly)."""
    if iface is None or not holds():
        return
    ok, out = _run(_delete_cmd())
    if ok:
        log.info(f"Released floating IP {ip}")
    else:
        log.warning(f"Couldn't release floating IP {ip}: {out}")


# ---------- the loop ----------

def _check_permissions(log) -> None:
    if sys.platform == "win32":
        import ctypes
        if not ctypes.windll.shell32.IsUserAnAdmin():
            log.warning("Not running as administrator: netsh can't add the floating IP. "
                        "Start the node from an administrator terminal.")


async def floating_ip(log):
    if iface is None:
        log.info("Floating IP off (start with --iface <adapter> to turn it on)")
        return
    log.info(f"Floating IP {ip} on adapter {iface!r} ({sys.platform})")
    _check_permissions(log)
    await asyncio.to_thread(release_now, log)        # left over from a crash?

    held = False                                     # what we last logged, to log changes only
    next_try = 0.0
    loop = asyncio.get_running_loop()
    while True:
        await asyncio.sleep(CHECK_INTERVAL)
        # Hold it only as the leader, and only once synced, so the first page a client sees
        # from a new leader already has all the photos.
        should_hold = s.Members.leader == s.MyDetails.node_id and s.Members.is_synced
        have = await asyncio.to_thread(holds)

        if should_hold and not have and loop.time() >= next_try:
            ok, out = await asyncio.to_thread(_run, _add_cmd())
            if ok:
                log.info(f"Took floating IP {ip}")
            else:
                log.warning(f"Couldn't take floating IP {ip}: {out}")
            next_try = loop.time() + RETRY_AFTER     # Windows needs a moment to finish adding
        elif not should_hold and have:
            ok, out = await asyncio.to_thread(_run, _delete_cmd())
            log.info(f"Released floating IP {ip} (no longer leader)" if ok
                     else f"Couldn't release floating IP {ip}: {out}")

        if have != held:
            held = have
            if have:
                log.info(f"Floating IP {ip} is now answering here")
            elif should_hold:
                log.warning(f"Lost floating IP {ip}; trying to take it back")