"""CLI entry point for zabbix-argus-glue."""

import argparse
import asyncio
import logging
import re
import sys
from pathlib import Path

import aiohttp

from zabbixargus import __version__
from zabbixargus.argus_client import ArgusClient
from zabbixargus.config import CONFIG_SEARCH_PATHS, find_config, load_config
from zabbixargus.zabbix_client import ZabbixClient

log = logging.getLogger("zabbixargus")


def cli(argv=None):
    args = parse_args(argv)
    setup_logging(verbose=args.verbose, quiet=args.verify)

    config_path = args.config or find_config()
    if config_path is None:
        print("No configuration file found. Searched:", file=sys.stderr)
        for path in CONFIG_SEARCH_PATHS:
            print(f"  {path}", file=sys.stderr)
        sys.exit(1)
    try:
        config = load_config(config_path)
    except Exception as e:
        print(f"Configuration error in {config_path}: {e}", file=sys.stderr)
        sys.exit(1)

    if args.verify:
        ok = asyncio.run(verify_connections(config))
        sys.exit(0 if ok else 1)

    # Default: run the service
    from zabbixargus.core import run

    asyncio.run(run(config))


def setup_logging(*, verbose: bool = False, quiet: bool = False):
    """Configure root logging.

    Timestamps are included only when stdout is a terminal; when
    running under systemd or similar, the process controller adds
    its own timestamps.
    """
    if verbose:
        level = logging.DEBUG
    elif quiet:
        level = logging.WARNING
    else:
        level = logging.INFO

    if sys.stdout.isatty():
        fmt = "%(asctime)s %(levelname)s %(name)s: %(message)s"
    else:
        fmt = "%(levelname)s %(name)s: %(message)s"

    logging.basicConfig(level=level, format=fmt)
    # httpx logs every HTTP request at INFO, which is too noisy.
    logging.getLogger("httpx").setLevel(max(level, logging.WARNING))
    # The decorator logs connection errors before re-raising them; callers
    # handle these with context, so avoid duplicate tracebacks.
    logging.getLogger("simple_rest_client.decorators").setLevel(logging.CRITICAL)


async def verify_connections(config):
    """Test connectivity to both Zabbix and Argus APIs."""
    zabbix_ok = await verify_zabbix(config)
    argus_ok = await verify_argus(config)
    return zabbix_ok and argus_ok


async def verify_zabbix(config):
    """Verify Zabbix API connectivity. Returns True on success."""
    client = ZabbixClient(config.zabbix)
    try:
        await client.connect()
        version = client.api.version
        print(f"Zabbix: OK (version {version} at {config.zabbix.url})")
    except Exception as e:
        print(f"Zabbix: FAILED ({e})")
        return False
    finally:
        await client.close()
    return True


async def verify_argus(config):
    """Verify Argus API connectivity. Returns True on success."""
    try:
        client = ArgusClient(config.argus)
        await client.get_open_incidents()
        version = await _fetch_argus_version(config.argus.url)
        version_info = f"version {version} at " if version else ""
        print(f"Argus: OK ({version_info}{config.argus.url})")
    except Exception as e:
        print(f"Argus: FAILED ({e})")
        return False
    return True


async def _fetch_argus_version(api_url: str) -> str | None:
    """Fetch server version from the Argus metadata endpoint."""
    metadata_url = re.sub(r"/v\d+/?$", "/", api_url)
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(metadata_url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("server-version")
    except Exception:
        log.debug("Could not fetch Argus server version", exc_info=True)
    return None


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Synchronizes Zabbix problems with Argus incidents",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="path to TOML configuration file (default: auto-discover)",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="test API connectivity and exit",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="enable debug logging",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"zabbix-argus-glue {__version__}",
    )
    return parser.parse_args(argv)
