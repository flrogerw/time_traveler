#!/usr/bin/env python3
"""Turn a Kasa smart WiFi plug on/off (uses python-kasa, installed in .venv).

Usage: python kasa_plug.py on|off|status
The plug is hard-coded below (EP10; old protocol, no login needed). Override with --host.
No network discovery: it only talks to that one host.
"""
import argparse
import asyncio

from kasa import Discover

HOST = "192.168.1.199"


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("action", choices=["on", "off", "status"])
    ap.add_argument("--host", default=HOST)
    a = ap.parse_args()

    dev = await Discover.discover_single(a.host)
    try:
        if a.action == "on":
            await dev.turn_on()
        elif a.action == "off":
            await dev.turn_off()
        await dev.update()
        print(f"{dev.alias} ({a.host}): {'ON' if dev.is_on else 'OFF'}")
    finally:
        await dev.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
