"""Full sync and drift detection between Zabbix and Argus."""

import asyncio
import logging
from datetime import datetime, timezone

import httpx
from simple_rest_client.exceptions import ClientConnectionError

from zabbixargus.argus_client import ArgusClient, DuplicateIncidentError
from zabbixargus.config import Config
from zabbixargus.tags import build_tags
from zabbixargus.zabbix_client import ZabbixClient, build_details_url

log = logging.getLogger(__name__)


def _is_transient_argus_error(error: Exception) -> bool:
    # simple-rest-client also wraps decoding and local request errors.
    if not isinstance(error, ClientConnectionError) or not error.args:
        return False
    cause = error.args[0]
    return isinstance(cause, (httpx.NetworkError, httpx.TimeoutException)) or (
        # httpcore uses this message for EOF before a response. Other remote
        # protocol errors can indicate malformed HTTP and must remain errors.
        isinstance(cause, httpx.RemoteProtocolError)
        and str(cause) == "Server disconnected without sending a response."
    )


async def run_reconciliation_loop(
    zabbix: ZabbixClient, argus: ArgusClient, config: Config
):
    """Run reconciliation on startup and then at a regular interval."""
    while True:
        try:
            await reconcile(zabbix, argus, config)
        except Exception:
            log.exception("Reconciliation failed")
        await asyncio.sleep(config.reconciliation.interval)


async def reconcile(zabbix: ZabbixClient, argus: ArgusClient, config: Config):
    """Run a single reconciliation pass.

    Fetches open problems from Zabbix and open incidents from Argus,
    then creates missing incidents and closes stale ones.
    """
    problems = await zabbix.get_problems_with_hosts(
        resolve_hostgroups=config.requires_hostgroups()
    )
    try:
        argus_incidents = await argus.get_open_incidents()
    except ClientConnectionError as e:
        if not _is_transient_argus_error(e):
            raise
        log.warning(
            "Argus connection error while fetching open incidents; "
            "will retry next pass: %s",
            e,
        )
        return

    minimum = config.severity.minimum_severity
    eligible = [p for p in problems if int(p["severity"]) >= minimum]
    synced = [p for p in eligible if config.filter.allows(p.get("hostgroups", []))]
    ignored = len(eligible) - len(synced)
    synced_problem_ids = {p["eventid"] for p in synced}
    fetched_problem_ids = {p["eventid"] for p in problems}

    created = await _create_missing(synced, argus_incidents, argus, config)
    closed = await _close_stale(
        synced_problem_ids, fetched_problem_ids, argus_incidents, zabbix, argus
    )

    _log_reconciliation_summary(created, closed, ignored, config)


def _log_reconciliation_summary(
    created: int, closed: int, ignored: int, config: Config
):
    """Summarize a reconciliation pass.

    When webhooks are enabled, non-zero drift means some events were
    never received as webhooks (glue service was down, webhooks
    misconfigured, or first run); log at WARNING so it is easy to
    spot.  Otherwise log at INFO.  Zero drift is logged at DEBUG.
    ``ignored`` counts problems excluded by the host-group filter and
    is reported alongside either way.
    """
    if created == 0 and closed == 0:
        log.debug("Reconciliation pass: no drift (%d ignored by filter)", ignored)
        return
    level = logging.WARNING if config.webhook.enabled else logging.INFO
    log.log(
        level,
        "Reconciliation pass: created %d, closed %d (%d ignored by filter)",
        created,
        closed,
        ignored,
    )


async def _create_missing(
    problems: list[dict],
    argus_incidents: dict,
    argus: ArgusClient,
    config: Config,
) -> int:
    created = 0
    for problem in problems:
        eventid = problem["eventid"]
        if eventid in argus_incidents:
            continue

        try:
            await _create_incident_for_problem(problem, argus, config)
            created += 1
        except DuplicateIncidentError:
            # Recurs every pass until the problem clears, so log at DEBUG.
            log.debug(
                "Problem %s already has an Argus incident; not recreating", eventid
            )
        except Exception as e:
            if _is_transient_argus_error(e):
                log.warning(
                    "Argus connection error while creating incident for problem %s; "
                    "will retry next pass: %s",
                    eventid,
                    e,
                )
            else:
                log.exception("Failed to create incident for problem %s", eventid)
    return created


async def _create_incident_for_problem(
    problem: dict,
    argus: ArgusClient,
    config: Config,
):
    eventid = problem["eventid"]
    hosts = problem.get("hosts", [])
    hostname = hosts[0]["host"] if hosts else ""
    hostgroups = problem.get("hostgroups", [])

    zabbix_severity = int(problem["severity"])
    argus_level = config.severity.mapping[zabbix_severity]

    tags = build_tags(
        hostname=hostname,
        hostgroups=hostgroups,
        trigger=problem.get("name", ""),
        zabbix_tags=problem.get("tags", []),
        config=config.tags,
    )

    start_time = datetime.fromtimestamp(int(problem["clock"]), tz=timezone.utc)
    details_url = build_details_url(
        eventid=eventid, triggerid=problem.get("objectid", "")
    )

    await argus.create_incident_from_problem(
        description=problem.get("name", ""),
        hostname=hostname,
        prefix_hostname=config.sync.prefix_hostname,
        source_incident_id=eventid,
        details_url=details_url,
        level=argus_level,
        tags=tags,
        start_time=start_time,
    )


async def _close_stale(
    synced_problem_ids: set[str],
    fetched_problem_ids: set[str],
    argus_incidents: dict,
    zabbix: ZabbixClient,
    argus: ArgusClient,
) -> int:
    """Close incidents whose Zabbix problem is no longer in sync scope.

    For each open incident whose problem is not in ``synced_problem_ids``:

    - if the problem is still in the latest fetch from Zabbix but
      excluded by the filter/severity rules, close the incident — the
      glue no longer syncs it, and the webhook won't update it either,
      so leaving it open would orphan it;
    - if the problem is absent from the fetch entirely, re-confirm with
      Zabbix (``problem_exists``) that it is genuinely resolved before
      closing, so a flaky or partial fetch does not close a live
      incident.

    An empty fetch is treated as a likely transient failure and closes
    nothing.
    """
    if argus_incidents and not fetched_problem_ids:
        log.warning(
            "Skipping stale-incident close: Zabbix returned no problems "
            "(likely a transient fetch failure)"
        )
        return 0

    closed = 0
    for source_id, incident in argus_incidents.items():
        if source_id in synced_problem_ids:
            continue
        try:
            if source_id not in fetched_problem_ids and await zabbix.problem_exists(
                source_id
            ):
                log.warning(
                    "Not closing incident %s: Zabbix problem %s is still active "
                    "but was missing from the latest fetch",
                    incident.pk,
                    source_id,
                )
                continue
            await argus.resolve_incident(incident, "Resolved by reconciliation")
            closed += 1
        except Exception as e:
            if _is_transient_argus_error(e):
                log.warning(
                    "Argus connection error while closing incident %s for problem %s; "
                    "will retry next pass: %s",
                    incident.pk,
                    source_id,
                    e,
                )
            else:
                log.exception("Failed to close Argus incident %s", incident.pk)
    return closed
