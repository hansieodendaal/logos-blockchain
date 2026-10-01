#!/usr/bin/env python3
"""Best-effort, read-only telemetry for GitHub merge-queue candidates.

The three command modes correspond to the workflow paths:

* ``snapshot`` records a merge_group candidate and its bounded Git/API view;
* ``timing`` records one completed merge_group workflow plus available sibling
  workflow, job, step, runner, check, and status evidence;
* ``finalize`` records a merged pull request only when stack/PR evidence resolves
  its queue trunk to ``master``; merge-commit checks are a point-in-time snapshot.

Raw responses are always kept beside normalized interpretations. In particular,
queue-entry absence is distinct from an unavailable lookup, stack absence is
distinct from unresolved stack metadata, and candidate ownership is distinct
from the other PR changes present in the candidate tree. The collector is
intended to run from trusted master and never executes queued PR code. Queue
topology/configuration is read from the repository trunk, while candidate
entry/timeline evidence remains PR-specific. Candidate entries are reconciled
from both views and retain their effective source/conflicts. GraphQL responses
are classified as complete, partial, or unavailable without discarding usable
data. Queue-to-merge durations distinguish authoritative cycle pairing from an
inferred reason-less removal pairing; merge-commit checks remain a point-in-time
post-merge snapshot.

This collector intentionally keeps broad raw and derived evidence during the
initial production measurement period. After representative queue samples are
collected, exploratory fields should be removed and only measurements needed
by the CI optimization retained.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


TARGET_WORKFLOWS = (
    "Code checks",
    "Cucumber integration tests",
    "End-to-end integration tests",
)
TELEMETRY_WORKFLOW_NAME = "Merge queue telemetry"
SAFE_ENVIRONMENT_FIELDS = (
    "GITHUB_EVENT_NAME",
    "GITHUB_EVENT_PATH",
    "GITHUB_REF",
    "GITHUB_REF_NAME",
    "GITHUB_REF_TYPE",
    "GITHUB_SHA",
    "GITHUB_BASE_REF",
    "GITHUB_HEAD_REF",
    "GITHUB_RUN_ID",
    "GITHUB_RUN_NUMBER",
    "GITHUB_RUN_ATTEMPT",
    "GITHUB_WORKFLOW",
    "GITHUB_WORKFLOW_REF",
    "GITHUB_WORKFLOW_SHA",
    "GITHUB_REPOSITORY",
    "GITHUB_ACTOR",
    "GITHUB_SERVER_URL",
    "GITHUB_API_URL",
)
QUEUE_SUFFIX_PATTERN = re.compile(
    r"(?:^|/)gh-readonly-queue/(.+)/pr-(\d+)-[0-9a-fA-F]{5,}$"
)
MESSAGE_PR_PATTERNS = (
    re.compile(r"#(\d+)"),
    re.compile(r"\b(?:pull requests?|pr)\s*#?(\d+)\b", re.IGNORECASE),
)
MAX_COMMIT_ASSOCIATIONS = 200
MAX_API_PAGES = 10
MAX_QUEUE_ENTRIES = 100
MAX_TIMELINE_EVENTS = 100
HTTP_TIMEOUT_SECONDS = 20
GIT_FETCH_DEPTH = 128

# GraphQL ``itemTypes`` takes enum values; response ``__typename`` uses the
# concrete object names below. Keep the two vocabularies separate.
MERGE_QUEUE_TIMELINE_ITEM_TYPES = (
    "ADDED_TO_MERGE_QUEUE_EVENT",
    "REMOVED_FROM_MERGE_QUEUE_EVENT",
    "MERGED_EVENT",
)
MERGE_QUEUE_TIMELINE_TYPENAMES = (
    "AddedToMergeQueueEvent",
    "RemovedFromMergeQueueEvent",
    "MergedEvent",
)
KNOWN_QUEUE_REMOVAL_FAILURE_REASONS = frozenset(
    {
        "failed_checks",
        "invalid_merge_commit",
        "stack_out_of_order",
        "conflict",
        "merge_conflict",
        "merge_queue_disabled",
    }
)
KNOWN_QUEUE_REMOVAL_MANUAL_REASONS = frozenset(
    {
        "manual",
        "manually_removed",
        "removed_by_user",
        "pr_closed",
        "pull_request_closed",
        "branch_deleted",
    }
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def save_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def normalize_api_error(
    endpoint: str,
    status: int | None,
    error_type: str,
    raw_response: Any = None,
    *,
    timestamp: str | None = None,
) -> dict[str, Any]:
    """Keep sanitized REST failure evidence without headers, tokens, or secrets."""
    detail = _api_error_detail(raw_response) if raw_response is not None else None
    return {
        "endpoint": endpoint,
        "status": status,
        "error_type": error_type,
        "message": detail,
        "timestamp": timestamp or utc_now(),
    }


def copy_event(source: Path, output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    destination = output_dir / "event.json"
    shutil.copyfile(source, destination)
    return json.loads(destination.read_text(encoding="utf-8"))


def safe_environment() -> dict[str, str | None]:
    return {key: os.environ.get(key) for key in SAFE_ENVIRONMENT_FIELDS}


def normalize_branch_ref(value: Any) -> str | None:
    """Convert a branch ref to its branch name without reinterpreting other refs.

    Plain branch names and ``refs/heads/<name>`` are accepted. Other full ref
    namespaces, such as tags, remain unresolved because they do not identify a
    merge-queue target branch.
    """
    if not isinstance(value, str) or not value:
        return None
    if value.startswith("refs/heads/"):
        branch = value.removeprefix("refs/heads/")
        return branch or None
    if value.startswith("refs/"):
        return None
    return value


def parse_queue_pr_number(ref: Any) -> int | None:
    if not isinstance(ref, str):
        return None
    match = QUEUE_SUFFIX_PATTERN.search(ref)
    return int(match.group(2)) if match else None


def queue_branch_from_queue_ref(ref: Any) -> str | None:
    """Extract the full trunk component from a queue ref as evidence.

    The component may contain slashes; everything between
    ``gh-readonly-queue/`` and ``/pr-<number>-`` is preserved. The naming
    convention remains one identity signal rather than authoritative PR proof.
    """
    if not isinstance(ref, str):
        return None
    match = QUEUE_SUFFIX_PATTERN.search(ref)
    return match.group(1) if match else None


def message_pr_numbers(message: Any) -> list[int]:
    if not isinstance(message, str):
        return []
    numbers: set[int] = set()
    for pattern in MESSAGE_PR_PATTERNS:
        numbers.update(int(match.group(1)) for match in pattern.finditer(message))
    return sorted(numbers)


def _pull_numbers(value: Any) -> list[int]:
    if isinstance(value, list):
        return sorted(
            {
                int(item["number"])
                for item in value
                if isinstance(item, dict) and isinstance(item.get("number"), int)
            }
        )
    return []


def derive_candidate_identity(
    *,
    ref_signals: list[tuple[str, Any]],
    head_associated_pulls: Any,
    commit_message: Any,
    pull_requests: dict[int, dict[str, Any] | None],
) -> dict[str, Any]:
    """Keep identity clues separate and resolve only a single corroborated PR."""
    signals: list[dict[str, Any]] = []
    ref_numbers: set[int] = set()
    all_candidates: set[int] = set()

    for source, raw_value in ref_signals:
        number = parse_queue_pr_number(raw_value)
        numbers = [number] if number is not None else []
        signals.append(
            {
                "source": source,
                "raw_value": raw_value,
                "candidate_prs": numbers,
                "interpretation": (
                    "queue-ref naming pattern contains a PR number"
                    if number is not None
                    else "no recognized queue-ref PR number"
                ),
            }
        )
        ref_numbers.update(numbers)
        all_candidates.update(numbers)

    head_numbers = _pull_numbers(head_associated_pulls)
    signals.append(
        {
            "source": "head_sha_commit_associations",
            "raw_value": head_associated_pulls,
            "candidate_prs": head_numbers,
            "interpretation": (
                "GitHub associated-pull-requests response; may include cumulative changes"
            ),
        }
    )
    all_candidates.update(head_numbers)

    message_numbers = message_pr_numbers(commit_message)
    signals.append(
        {
            "source": "head_commit_message",
            "raw_value": commit_message,
            "candidate_prs": message_numbers,
            "interpretation": "message text is a clue and is not authoritative",
        }
    )
    all_candidates.update(message_numbers)

    candidate_pr: int | None = None
    confidence = "unresolved"
    evidence: list[str] = []
    reason = "no unique candidate is supported by the available signals"

    if len(ref_numbers) > 1:
        reason = "queue-ref signals disagree"
    elif len(ref_numbers) == 1:
        number = next(iter(ref_numbers))
        metadata = pull_requests.get(number)
        if metadata is None:
            reason = f"queue ref suggests PR #{number}, but its PR metadata was unavailable"
        elif head_numbers and number not in head_numbers:
            reason = (
                f"queue ref suggests PR #{number}, but the head commit association "
                "does not include it"
            )
        elif number in head_numbers:
            candidate_pr = number
            confidence = "high"
            evidence = [
                "queue/head ref names this PR",
                "head SHA associated-pull-requests response includes this PR",
                "PR metadata was fetched successfully",
            ]
            reason = "queue-ref and head-commit association signals agree"
        elif message_numbers == [number]:
            candidate_pr = number
            confidence = "medium"
            evidence = [
                "queue/head ref names this PR",
                "head commit message independently names the same PR",
                "PR metadata was fetched successfully",
            ]
            reason = "queue-ref and commit-message signals agree"
        else:
            reason = (
                f"queue ref suggests PR #{number}, but no independent identity signal "
                "corroborates it"
            )
    elif len(head_numbers) == 1:
        number = head_numbers[0]
        if len(message_numbers) == 1 and message_numbers[0] != number:
            reason = "head SHA association and commit message suggest different PRs"
        elif pull_requests.get(number) is None:
            reason = f"head SHA association suggests PR #{number}, but its metadata was unavailable"
        else:
            candidate_pr = number
            confidence = "medium"
            evidence = [
                "head SHA associated-pull-requests response contains exactly one PR",
                "PR metadata was fetched successfully",
            ]
            if message_numbers == [number]:
                confidence = "high"
                evidence.append("head commit message independently names the same PR")
            reason = "the head SHA association contains one PR"
    elif len(head_numbers) > 1:
        reason = "head SHA is associated with multiple PRs and no queue-ref candidate resolved it"
    elif len(message_numbers) == 1 and pull_requests.get(message_numbers[0]) is not None:
        reason = "commit message alone is not sufficient to identify the candidate PR"

    if candidate_pr is not None:
        all_candidates.add(candidate_pr)

    return {
        "candidate_pr": candidate_pr,
        "candidate_pr_confidence": confidence,
        "candidate_pr_evidence": evidence,
        "candidate_pr_candidates": sorted(all_candidates),
        "candidate_pr_resolution": reason,
        "signals": signals,
    }


def graphql_response_status(response: Any) -> tuple[str, list[Any]]:
    """Classify GraphQL evidence without discarding usable partial ``data``.

    ``complete`` means a usable response has no GraphQL errors, ``partial``
    means usable data arrived alongside errors, and ``unavailable`` means no
    usable data structure was returned. The original structured errors remain
    available to callers and are written with the raw response.
    """
    if not isinstance(response, dict):
        return "unavailable", []
    errors = response.get("errors")
    errors_list = errors if isinstance(errors, list) else []
    data = response.get("data")
    if not isinstance(data, dict):
        return "unavailable", errors_list
    return ("partial" if errors_list else "complete"), errors_list


def graphql_errors_affect_field(errors: list[Any], *field_names: str) -> bool:
    """Return whether a GraphQL error path names one of the requested fields."""
    for error in errors:
        path = error.get("path") if isinstance(error, dict) else None
        if isinstance(path, list) and any(name in path for name in field_names):
            return True
    return False


def _graphql_pull_request(
    response: Any,
) -> tuple[dict[str, Any] | None, str, list[Any]]:
    status, errors = graphql_response_status(response)
    if status == "unavailable":
        return None, status, errors
    data = response.get("data")
    repository = data.get("repository")
    if not isinstance(repository, dict):
        return None, "unavailable", errors
    pull_request = repository.get("pullRequest")
    if not isinstance(pull_request, dict):
        return None, "unavailable", errors
    return pull_request, status, errors


def classify_stack(
    graphql_response: Any, rest_pull_request: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Return stack membership distinctly from a failed or incomplete lookup."""
    rest_pull_request = rest_pull_request or {}
    graphql_pr, graphql_status, graphql_errors = _graphql_pull_request(graphql_response)
    rest_stack_present = "stack" in rest_pull_request
    rest_stack = rest_pull_request.get("stack")

    if graphql_pr is not None:
        stack = graphql_pr.get("stack")
        entry = graphql_pr.get("stackEntry")
        if stack is None and isinstance(entry, dict):
            stack = entry.get("stack")
        if stack is None and entry is None:
            if rest_stack_present and isinstance(rest_stack, dict):
                result = _classify_rest_stack(rest_stack, "REST pull-request stack object")
                result["graphql_status"] = graphql_status
                result["graphql_errors"] = graphql_errors
                return result
            if graphql_status == "partial":
                result = _unresolved_stack(
                    "GraphQL partial response omitted stack membership fields"
                )
                result["graphql_status"] = graphql_status
                result["graphql_errors"] = graphql_errors
                return result
            return {
                "stack_status": "not_a_stack",
                "stack_id": None,
                "stack_number": None,
                "stack_position": None,
                "stack_size": None,
                "stack_base_ref": None,
                "stack_base_sha": None,
                "stack_base_source": None,
                "is_stack_member": False,
                "is_stack_head": False,
                "evidence": ["GraphQL stack and stackEntry fields both returned null"],
                "graphql_status": graphql_status,
                "graphql_errors": graphql_errors,
            }
        if (
            not isinstance(stack, dict)
            or not isinstance(entry, dict)
            or not isinstance(entry.get("position"), int)
            or not isinstance(stack.get("size"), int)
        ):
            if rest_stack_present and isinstance(rest_stack, dict):
                result = _classify_rest_stack(
                    rest_stack, "REST pull-request stack object; GraphQL data was incomplete"
                )
                result["graphql_status"] = graphql_status
                result["graphql_errors"] = graphql_errors
                return result
            result = _unresolved_stack("GraphQL returned incomplete stack membership data")
            result["graphql_status"] = graphql_status
            result["graphql_errors"] = graphql_errors
            return result
        position = entry.get("position")
        size = stack.get("size")
        base_sha = None
        entries_value = stack.get("entries")
        entries = entries_value.get("nodes", []) if isinstance(entries_value, dict) else []
        if isinstance(entries, list):
            for item in entries:
                if not isinstance(item, dict) or item.get("position") != 1:
                    continue
                bottom_pr = item.get("pullRequest")
                if isinstance(bottom_pr, dict):
                    base_sha = bottom_pr.get("baseRefOid")
                break
        if base_sha is None and isinstance(rest_stack, dict):
            base = rest_stack.get("base")
            if isinstance(base, dict):
                base_sha = base.get("sha")
        member = True
        head = position == size if isinstance(position, int) and isinstance(size, int) else None
        return {
            "stack_status": "stack_member",
            "stack_id": stack.get("id"),
            "stack_number": stack.get("number"),
            "stack_position": position,
            "stack_size": size,
            "stack_base_ref": stack.get("baseRefName"),
            "stack_base_sha": base_sha,
            "stack_base_source": "GraphQL stack base",
            "is_stack_member": member,
            "is_stack_head": head,
            "evidence": [
                "GraphQL PullRequest.stack / PullRequest.stackEntry",
                (
                    "stack base SHA inferred from the position-1 pull request's baseRefOid"
                    if base_sha is not None
                    else "GraphQL stack schema does not expose a base SHA"
                ),
            ],
            "graphql_status": graphql_status,
            "graphql_errors": graphql_errors,
        }

    if rest_stack_present and isinstance(rest_stack, dict):
        result = _classify_rest_stack(rest_stack, "REST pull-request stack object")
        result["graphql_status"] = graphql_status
        result["graphql_errors"] = graphql_errors
        return result
    if rest_stack_present and rest_stack is None:
        return {
            "stack_status": "not_a_stack",
            "stack_id": None,
            "stack_number": None,
            "stack_position": None,
            "stack_size": None,
            "stack_base_ref": None,
            "stack_base_sha": None,
            "stack_base_source": None,
            "is_stack_member": False,
            "is_stack_head": False,
            "evidence": ["REST pull-request stack field explicitly returned null"],
            "graphql_status": graphql_status,
            "graphql_errors": graphql_errors,
        }
    result = _unresolved_stack("stack metadata lookup failed or was incomplete")
    result["graphql_status"] = graphql_status
    result["graphql_errors"] = graphql_errors
    return result


def _classify_rest_stack(stack: dict[str, Any], evidence: str) -> dict[str, Any]:
    position = stack.get("position")
    size = stack.get("size")
    base = stack.get("base")
    if isinstance(base, str):
        base_ref = base
        base_sha = None
    elif isinstance(base, dict):
        base_ref = base.get("ref")
        base_sha = base.get("sha")
    else:
        base_ref = None
        base_sha = None
    return {
        "stack_status": "stack_member",
        "stack_id": stack.get("id"),
        "stack_number": stack.get("number"),
        "stack_position": position,
        "stack_size": size,
        "stack_base_ref": base_ref,
        "stack_base_sha": base_sha,
        "is_stack_member": True,
        "is_stack_head": (
            position == size if isinstance(position, int) and isinstance(size, int) else None
        ),
        "evidence": [evidence],
        "stack_base_source": "REST stack base",
        "graphql_status": "unavailable",
        "graphql_errors": [],
    }


def _unresolved_stack(reason: str) -> dict[str, Any]:
    return {
        "stack_status": "unresolved",
        "stack_id": None,
        "stack_number": None,
        "stack_position": None,
        "stack_size": None,
        "stack_base_ref": None,
        "stack_base_sha": None,
        "stack_base_source": None,
        "is_stack_member": None,
        "is_stack_head": None,
        "evidence": [reason],
        "graphql_status": "unavailable",
        "graphql_errors": [],
    }


def _actor_login(value: Any) -> str | None:
    """Extract an actor login without treating malformed API data as identity."""
    if isinstance(value, dict) and isinstance(value.get("login"), str):
        return value["login"]
    return None


def _normalize_commit(value: Any) -> str | None:
    """Return a commit OID from a GraphQL commit object when one is present."""
    if isinstance(value, dict) and isinstance(value.get("oid"), str):
        return value["oid"]
    return None


def normalize_merge_queue_entry(value: Any) -> dict[str, Any]:
    """Normalize one MergeQueueEntry without turning absent fields into guesses."""
    if not isinstance(value, dict):
        return {
            "id": None,
            "enqueued_at": None,
            "enqueuer": None,
            "estimated_time_to_merge_seconds": None,
            "queue_entry_position": None,
            "state": None,
            "jump": None,
            "solo": None,
            "base_commit": None,
            "head_commit": None,
            "merge_queue": None,
            "pull_request_number": None,
            "pull_request": None,
            "raw": value,
        }
    pull_request = value.get("pullRequest")
    pull_request_summary = None
    if isinstance(pull_request, dict):
        stack = pull_request.get("stack")
        stack_entry = pull_request.get("stackEntry")
        pull_request_summary = {
            "number": pull_request.get("number"),
            "base_ref": pull_request.get("baseRefName"),
            "base_sha": pull_request.get("baseRefOid"),
            "head_ref": pull_request.get("headRefName"),
            "head_sha": pull_request.get("headRefOid"),
            "stack": {
                "id": stack.get("id"),
                "number": stack.get("number"),
                "size": stack.get("size"),
                "base_ref": stack.get("baseRefName"),
            }
            if isinstance(stack, dict)
            else None,
            "stack_position": stack_entry.get("position")
            if isinstance(stack_entry, dict)
            else None,
        }
    return {
        "id": value.get("id"),
        "enqueued_at": value.get("enqueuedAt"),
        "enqueuer": _actor_login(value.get("enqueuer")),
        "estimated_time_to_merge_seconds": value.get("estimatedTimeToMerge"),
        "queue_entry_position": value.get("position"),
        "state": value.get("state"),
        "jump": value.get("jump"),
        "solo": value.get("solo"),
        "base_commit": _normalize_commit(value.get("baseCommit")),
        "head_commit": _normalize_commit(value.get("headCommit")),
        "merge_queue": _timeline_merge_queue(value.get("mergeQueue")),
        "pull_request_number": (
            pull_request.get("number")
            if isinstance(pull_request, dict)
            else None
        ),
        "pull_request": pull_request_summary,
        "raw": value,
    }


def normalize_merge_queue(
    value: Any, *, graphql_errors: list[Any] | None = None
) -> dict[str, Any] | None:
    """Normalize queue policy and topology while preserving snapshot completeness."""
    if not isinstance(value, dict):
        return None
    configuration = value.get("configuration")
    normalized_configuration = None
    if isinstance(configuration, dict):
        normalized_configuration = {
            "check_response_timeout_minutes": configuration.get("checkResponseTimeout"),
            "maximum_entries_to_build": configuration.get("maximumEntriesToBuild"),
            "maximum_entries_to_merge": configuration.get("maximumEntriesToMerge"),
            "minimum_entries_to_merge": configuration.get("minimumEntriesToMerge"),
            "minimum_entries_to_merge_wait_time_minutes": configuration.get(
                "minimumEntriesToMergeWaitTime"
            ),
            "merge_method": configuration.get("mergeMethod"),
            "merging_strategy": configuration.get("mergingStrategy"),
        }
    entries = value.get("entries")
    normalized_entries: list[dict[str, Any]] = []
    entries_nodes_available = False
    truncated = None
    total_count = None
    page_info: dict[str, Any] | None = None
    if isinstance(entries, dict):
        total_count = entries.get("totalCount")
        page_info_value = entries.get("pageInfo")
        if isinstance(page_info_value, dict):
            page_info = {
                "has_next_page": page_info_value.get("hasNextPage"),
                "end_cursor": page_info_value.get("endCursor"),
            }
        nodes = entries.get("nodes")
        if isinstance(nodes, list):
            entries_nodes_available = True
            normalized_entries = [normalize_merge_queue_entry(node) for node in nodes]
            for entry in normalized_entries:
                if entry.get("merge_queue") is None:
                    entry["merge_queue"] = _timeline_merge_queue(value)
        truncated = bool(
            page_info and page_info.get("has_next_page")
        ) or (
            isinstance(total_count, int) and total_count > len(normalized_entries)
        )
    return {
        "id": value.get("id"),
        "url": value.get("url"),
        "resource_path": value.get("resourcePath"),
        "next_entry_estimated_time_to_merge_seconds": value.get(
            "nextEntryEstimatedTimeToMerge"
        ),
        "configuration": normalized_configuration,
        "entries": normalized_entries,
        "total_count": total_count,
        "page_info": page_info,
        "truncated": truncated,
        "entries_available": isinstance(entries, dict)
        and entries_nodes_available,
        "entries_complete": (
            isinstance(entries, dict)
            and entries_nodes_available
            and isinstance(page_info, dict)
            and page_info.get("has_next_page") is False
            and truncated is False
            and not graphql_errors_affect_field(
                graphql_errors or [], "entries", "nodes", "pageInfo", "totalCount"
            )
            and not any(
                isinstance(error, dict)
                and isinstance(error.get("path"), list)
                and error["path"][-1:] == ["mergeQueue"]
                for error in graphql_errors or []
            )
        ),
        "raw": value,
    }


def normalize_merge_queue_response(response: Any) -> dict[str, Any]:
    """Separate current entry state from queue/configuration availability.

    ``absent`` means the API answered and returned a null entry; ``unresolved``
    means the response could not establish the current state. Queue topology is
    normalized from the repository-level ``mergeQueue`` when present, while
    ``isInMergeQueue`` and ``isMergeQueueEnabled`` remain independent signals.
    GraphQL ``partial`` responses retain usable entry/topology fields and their
    structured errors; ``unavailable`` responses leave them unresolved.
    """
    graphql_status, graphql_errors = graphql_response_status(response)
    if graphql_status == "unavailable":
        return {
            "queue_entry_status": "unresolved",
            "entry": None,
            "merge_queue": None,
            "is_in_merge_queue": None,
            "is_merge_queue_enabled": None,
            "graphql_available": False,
            "graphql_status": graphql_status,
            "graphql_errors": graphql_errors,
        }
    data = response.get("data")
    repository = data.get("repository") if isinstance(data, dict) else None
    if not isinstance(repository, dict):
        return {
            "queue_entry_status": "unresolved",
            "entry": None,
            "merge_queue": None,
            "is_in_merge_queue": None,
            "is_merge_queue_enabled": None,
            "graphql_available": False,
            "graphql_status": "unavailable",
            "graphql_errors": graphql_errors,
        }
    pull_request = repository.get("pullRequest")
    entry_field_present = (
        isinstance(pull_request, dict) and "mergeQueueEntry" in pull_request
    )
    entry_field_error = graphql_errors_affect_field(
        graphql_errors, "mergeQueueEntry"
    )
    entry_value = pull_request.get("mergeQueueEntry") if isinstance(pull_request, dict) else None
    queue_value = repository.get("mergeQueue")
    if queue_value is None and isinstance(pull_request, dict):
        queue_value = pull_request.get("mergeQueue")
    graphql_available = "pullRequest" in repository or "mergeQueue" in repository
    return {
        "queue_entry_status": (
            "present" if isinstance(entry_value, dict)
            else "absent"
            if isinstance(pull_request, dict) and entry_field_present and entry_value is None
            and not entry_field_error
            else "unresolved"
        ),
        "entry": normalize_merge_queue_entry(entry_value)
        if isinstance(entry_value, dict)
        else None,
        "merge_queue": normalize_merge_queue(
            queue_value, graphql_errors=graphql_errors
        ),
        "is_in_merge_queue": (
            pull_request.get("isInMergeQueue")
            if isinstance(pull_request, dict)
            else None
        ),
        "is_merge_queue_enabled": (
            pull_request.get("isMergeQueueEnabled")
            if isinstance(pull_request, dict)
            else None
        ),
        "graphql_available": graphql_available,
        "graphql_status": graphql_status,
        "graphql_errors": graphql_errors,
    }


def reconcile_candidate_queue_entry(
    candidate_queue: dict[str, Any],
    repository_queue: dict[str, Any],
    candidate_pr: int | None,
    *,
    candidate_observed_at: str | None = None,
    repository_observed_at: str | None = None,
) -> dict[str, Any]:
    """Reconcile PR-specific and repository-trunk queue-entry evidence.

    Repository entries are matched only by an already resolved PR number. A
    unique match can fill a missing PR-specific entry; duplicate matches or
    material disagreement leave the effective entry unresolved while retaining
    both source records and conflict details. Queue position, state, ETA, and
    other scheduling fields are mutable observation drift, not identity. When
    both views identify the same entry, repository queue values are preferred
    as the later trunk-level state. Absence is proven only by a complete
    repository snapshot with no matching PR entry; an unavailable/truncated
    second view leaves a PR-level null unresolved because a stack member may
    participate in a different trunk queue.
    """
    pr_entry = candidate_queue.get("entry") if isinstance(candidate_queue, dict) else None
    queue_snapshot = repository_queue.get("merge_queue") or {}
    entries = queue_snapshot.get("entries", [])
    matches = [
        entry for entry in entries
        if isinstance(entry, dict) and candidate_pr is not None
        and entry.get("pull_request_number") == candidate_pr
    ]
    repository_entry = matches[0] if len(matches) == 1 else None
    identity_fields = ("id", "head_commit", "base_commit")
    identity_mismatches: list[str] = []
    drift_fields: list[str] = []
    if isinstance(pr_entry, dict) and isinstance(repository_entry, dict):
        for field in identity_fields:
            left = pr_entry.get(field)
            right = repository_entry.get(field)
            if left is not None and right is not None and left != right:
                identity_mismatches.append(field)
        for field in (
            "queue_entry_position",
            "state",
            "estimated_time_to_merge_seconds",
            "enqueued_at",
            "jump",
            "solo",
        ):
            left = pr_entry.get(field)
            right = repository_entry.get(field)
            if left is not None and right is not None and left != right:
                drift_fields.append(field)
        left_queue = (pr_entry.get("merge_queue") or {}).get("id")
        right_queue = (repository_entry.get("merge_queue") or {}).get("id")
        if left_queue and right_queue and left_queue != right_queue:
            identity_mismatches.append("merge_queue.id")
    ambiguous = len(matches) > 1
    identity_conflict = bool(identity_mismatches) or ambiguous
    if identity_conflict:
        effective = None
        source = "unresolved"
    elif isinstance(pr_entry, dict) and isinstance(repository_entry, dict):
        effective = repository_entry
        source = "corroborated"
    elif isinstance(pr_entry, dict):
        effective = pr_entry
        source = "pull_request.mergeQueueEntry"
    elif isinstance(repository_entry, dict):
        effective = repository_entry
        source = "repository.mergeQueue.entries"
    else:
        effective = None
        source = "unresolved"
    repository_complete = bool(queue_snapshot.get("entries_complete"))
    candidate_status = (
        candidate_queue.get("queue_entry_status", "unresolved")
        if isinstance(candidate_queue, dict)
        else "unresolved"
    )
    if isinstance(effective, dict):
        entry_status = "present"
    elif identity_conflict:
        entry_status = "unresolved"
    elif (
        candidate_pr is not None
        and len(matches) == 0
        and repository_complete
        and candidate_status in {"absent", "unresolved"}
    ):
        # A complete trunk snapshot is sufficient even if the candidate query
        # failed; it positively enumerates the queue without this PR.
        entry_status = "absent"
    else:
        entry_status = "unresolved"
    return {
        "candidate_entry_from_pull_request": pr_entry,
        "candidate_entry_from_repository_queue": repository_entry,
        "repository_matching_entry_count": len(matches),
        "repository_matching_entries": matches,
        "candidate_entry_from_pull_request_observed_at": candidate_observed_at,
        "candidate_entry_from_repository_queue_observed_at": repository_observed_at,
        "effective_candidate_queue_entry": effective,
        "effective_candidate_queue_entry_source": source,
        "candidate_entry_identity_conflict": identity_conflict,
        "candidate_entry_identity_conflict_fields": identity_mismatches,
        "candidate_entry_observation_drift_fields": drift_fields,
        "queue_entry_status": entry_status,
        "repository_candidate_entry_resolution": (
            "ambiguous" if ambiguous
            else "matched" if len(matches) == 1
            else "not_found_complete_snapshot"
            if candidate_pr is not None and repository_complete
            else "not_found_incomplete_snapshot"
            if candidate_pr is not None
            else "candidate_unresolved"
        ),
    }


def resolve_queue_branch(
    *,
    merge_group_base_ref: Any = None,
    webhook_stack_base_ref: Any = None,
    webhook_base_ref: Any = None,
    stack: dict[str, Any] | None = None,
    pull_request: dict[str, Any] | None = None,
) -> dict[str, str | None]:
    """Resolve a queue trunk from independent evidence without guessing.

    A merge-group base or documented webhook stack target is strongest. Native
    stack metadata then supplies the trunk for every member. A direct webhook
    or REST PR base of ``master`` is usable even if stack metadata is temporarily
    unavailable; another direct base is used only when the PR is proven not to
    be a stack member. Raw event refs remain in the evidence; only values used
    as branch names pass through :func:`normalize_branch_ref`.
    """
    merge_group_branch = normalize_branch_ref(merge_group_base_ref)
    if merge_group_branch:
        return {"queue_branch": merge_group_branch, "queue_branch_source": "merge_group.base_ref"}
    webhook_stack_branch = normalize_branch_ref(webhook_stack_base_ref)
    if webhook_stack_branch:
        return {
            "queue_branch": webhook_stack_branch,
            "queue_branch_source": "webhook stack target",
        }
    if isinstance(stack, dict) and stack.get("stack_status") == "stack_member":
        base = normalize_branch_ref(stack.get("stack_base_ref"))
        if base:
            return {
                "queue_branch": base,
                "queue_branch_source": (
                    stack.get("stack_base_source")
                    if isinstance(stack.get("stack_base_source"), str)
                    else "stack base"
                ),
            }
    webhook_base = normalize_branch_ref(webhook_base_ref)
    rest_base = normalize_branch_ref(
        pull_request.get("base_ref") if isinstance(pull_request, dict) else None
    )
    stack_status = stack.get("stack_status") if isinstance(stack, dict) else None
    if webhook_base == "master":
        return {"queue_branch": "master", "queue_branch_source": "webhook PR base"}
    if rest_base == "master":
        return {"queue_branch": "master", "queue_branch_source": "REST PR base"}
    if stack_status == "not_a_stack":
        base = webhook_base or rest_base
        if base:
            return {
                "queue_branch": base,
                "queue_branch_source": "webhook PR base" if webhook_base else "REST PR base",
            }
    return {"queue_branch": None, "queue_branch_source": "unresolved"}


def webhook_stack_target(pull_request: Any) -> str | None:
    """Read only explicitly present webhook stack-target fields.

    GitHub payload versions differ. We inspect documented-looking nested stack
    target/base fields when supplied and otherwise return ``None``; no direct
    non-master base is promoted to a trunk by this helper.
    """
    if not isinstance(pull_request, dict):
        return None
    stack = pull_request.get("stack")
    candidates = []
    if isinstance(stack, dict):
        candidates.extend(
            [stack.get("baseRefName"), stack.get("base_ref"), stack.get("baseRef")]
        )
        base = stack.get("base")
        if isinstance(base, dict):
            candidates.extend([base.get("ref"), base.get("name")])
    candidates.extend([pull_request.get("stackBaseRef"), pull_request.get("stack_base_ref")])
    return next((value for value in candidates if isinstance(value, str) and value), None)


def finalization_trunk_decision(queue_branch: Any) -> str:
    """Classify whether finalization may collect master queue metrics.

    Only an exact resolved ``master`` trunk is eligible. Other branches and
    unresolved values are intentionally distinct so the finalizer cannot turn
    uncertainty into a master queue observation.
    """
    if queue_branch == "master":
        return "master"
    if isinstance(queue_branch, str) and queue_branch:
        return "non_master"
    return "unresolved"


def pull_request_merge_fields(response: Any) -> dict[str, Any]:
    """Extract merge status and commit evidence from a pull-request response."""
    status, errors = graphql_response_status(response)
    if status == "unavailable":
        return {
            "merged": None,
            "merged_at": None,
            "merge_commit": None,
            "graphql_status": status,
            "graphql_errors": errors,
        }
    data = response.get("data")
    repository = data.get("repository") if isinstance(data, dict) else None
    pull_request = repository.get("pullRequest") if isinstance(repository, dict) else None
    if not isinstance(pull_request, dict):
        return {
            "merged": None,
            "merged_at": None,
            "merge_commit": None,
            "graphql_status": "unavailable",
            "graphql_errors": errors,
        }
    return {
        "merged": pull_request.get("merged"),
        "merged_at": pull_request.get("mergedAt"),
        "merge_commit": _normalize_commit(pull_request.get("mergeCommit")),
        "graphql_status": status,
        "graphql_errors": errors,
    }


def normalize_timeline_response(response: Any) -> dict[str, Any]:
    """Normalize bounded timeline events, truncation, and GraphQL completeness."""
    graphql_status, graphql_errors = graphql_response_status(response)
    if graphql_status == "unavailable":
        return {
            "available": False,
            "events": [],
            "total_count": None,
            "page_info": None,
            "truncated": None,
            "graphql_status": graphql_status,
            "graphql_errors": graphql_errors,
        }
    data = response.get("data")
    repository = data.get("repository") if isinstance(data, dict) else None
    pull_request = repository.get("pullRequest") if isinstance(repository, dict) else None
    timeline = pull_request.get("timelineItems") if isinstance(pull_request, dict) else None
    if not isinstance(timeline, dict):
        return {
            "available": False,
            "events": [],
            "total_count": None,
            "page_info": None,
            "truncated": None,
            "graphql_status": "unavailable",
            "graphql_errors": graphql_errors,
        }
    page_info_value = timeline.get("pageInfo")
    page_info = None
    if isinstance(page_info_value, dict):
        page_info = {
            "has_next_page": page_info_value.get("hasNextPage"),
            "end_cursor": page_info_value.get("endCursor"),
        }
    nodes = timeline.get("nodes")
    node_list = nodes if isinstance(nodes, list) else []
    events = [
        event
        for event in node_list
        if isinstance(event, dict)
        and event.get("__typename") in MERGE_QUEUE_TIMELINE_TYPENAMES
    ]
    malformed_node_count = sum(
        1
        for event in node_list
        if not isinstance(event, dict)
        or event.get("__typename") in {None, ""}
    )
    total_count = timeline.get("totalCount")
    truncated = bool(page_info and page_info.get("has_next_page")) or (
        isinstance(total_count, int) and total_count > len(node_list)
    )
    return {
        "available": True,
        "events": events,
        "total_count": total_count,
        "page_info": page_info,
        "truncated": truncated,
        "ignored_node_count": max(0, len(node_list) - len(events)),
        "malformed_node_count": malformed_node_count,
        "graphql_status": graphql_status,
        "graphql_errors": graphql_errors,
    }


def _timeline_merge_queue(value: Any) -> dict[str, Any] | None:
    """Keep the queue identity attached to a timeline event."""
    if not isinstance(value, dict):
        return None
    return {
        "id": value.get("id"),
        "url": value.get("url"),
        "resource_path": value.get("resourcePath"),
    }


def _queue_removal_reason_class(reason: Any) -> str:
    """Classify a removal reason without treating an absent reason as failure."""
    if not isinstance(reason, str) or not reason:
        return "not_provided"
    normalized = reason.casefold().replace(" ", "_")
    if normalized in KNOWN_QUEUE_REMOVAL_FAILURE_REASONS:
        return "known_failure_or_invalidation"
    if normalized in KNOWN_QUEUE_REMOVAL_MANUAL_REASONS:
        return "known_manual_or_pr_state_change"
    return "unknown_or_unsupported"


def derive_queue_lifecycle(
    timeline: dict[str, Any],
    *,
    current_entry_status: str,
    merged_at: Any = None,
    merge_commit: Any = None,
) -> dict[str, Any]:
    """Pair queue cycles and derive merge time only from conservative evidence.

    A removal alone does not prove failure: GitHub can emit a successful
    remove-then-merge sequence, including equal timestamps. An authoritative
    queue-to-merge value additionally requires a concrete ``MergedEvent`` with
    a compatible commit, compatible queue identity when both are exposed, a
    complete timeline, and no explicit failure/manual/unknown removal reason.
    A reason-less compatible remove-then-merge is retained only as an inferred
    duration because it does not prove why the removal occurred.
    """
    if not isinstance(timeline, dict):
        timeline = {"available": False, "events": [], "truncated": None}
    events = sorted(
        timeline.get("events", []),
        key=lambda event: event.get("createdAt") or "",
    )
    admissions: list[dict[str, Any]] = []
    removals: list[dict[str, Any]] = []
    merged_events: list[dict[str, Any]] = []
    cycles: list[dict[str, Any]] = []
    for event_index, event in enumerate(events):
        typename = event.get("__typename")
        if typename == "AddedToMergeQueueEvent":
            admission = {
                "id": event.get("id"),
                "created_at": event.get("createdAt"),
                "actor": _actor_login(event.get("actor")),
                "enqueuer": _actor_login(event.get("enqueuer")),
                "merge_queue": _timeline_merge_queue(event.get("mergeQueue")),
                "event_index": event_index,
            }
            admissions.append(admission)
            cycles.append({"admission": admission, "removal": None})
        elif typename == "RemovedFromMergeQueueEvent":
            removal = {
                "id": event.get("id"),
                "created_at": event.get("createdAt"),
                "actor": _actor_login(event.get("actor")),
                "enqueuer": _actor_login(event.get("enqueuer")),
                "reason": event.get("reason"),
                "reason_class": _queue_removal_reason_class(event.get("reason")),
                "before_commit": _normalize_commit(event.get("beforeCommit")),
                "merge_queue": _timeline_merge_queue(event.get("mergeQueue")),
                "event_index": event_index,
            }
            removals.append(removal)
            for cycle in reversed(cycles):
                if cycle.get("removal") is None:
                    cycle["removal"] = removal
                    break
        elif typename == "MergedEvent":
            merged_event = {
                "id": event.get("id"),
                "created_at": event.get("createdAt"),
                "actor": _actor_login(event.get("actor")),
                "merge_commit": _normalize_commit(event.get("commit")),
                "event_index": event_index,
            }
            merged_events.append(merged_event)
            for cycle in reversed(cycles):
                admission = cycle.get("admission") or {}
                if cycle.get("merged") is None and admission.get("event_index", -1) <= event_index:
                    cycle["merged"] = merged_event
                    break
    latest_admission = admissions[-1] if admissions else None
    latest_removal = removals[-1] if removals else None
    current = (
        True
        if current_entry_status == "present"
        else False
        if current_entry_status == "absent"
        else None
    )
    merged_time = merged_at or (merged_events[-1].get("created_at") if merged_events else None)
    latest_cycle = cycles[-1] if cycles else None
    latest_cycle_removal = latest_cycle.get("removal") if latest_cycle else None
    latest_cycle_merge = latest_cycle.get("merged") if latest_cycle else None
    latest_cycle_admission_time = (
        latest_cycle.get("admission", {}).get("created_at") if latest_cycle else None
    )
    queue_to_merge = None
    queue_to_merge_inferred = None
    pairing_basis = None
    pairing_confidence = None
    removal_class = (
        latest_cycle_removal.get("reason_class") if latest_cycle_removal else None
    )
    merged_after_admission = bool(
        merged_time
        and latest_cycle_admission_time
        and duration_seconds(latest_cycle_admission_time, merged_time) is not None
    )
    merge_event_commit = (
        latest_cycle_merge.get("merge_commit") if latest_cycle_merge else None
    )
    merge_commit_matches_event = bool(
        latest_cycle_merge
        and isinstance(merge_event_commit, str)
        and (
            merge_commit is None
            or merge_commit == merge_event_commit
        )
    )
    admission_queue = (
        (latest_cycle.get("admission") or {}).get("merge_queue")
        if latest_cycle
        else None
    )
    removal_queue = (
        (latest_cycle_removal or {}).get("merge_queue")
        if latest_cycle_removal
        else None
    )
    queue_identity_matches = not (
        admission_queue
        and removal_queue
        and admission_queue.get("id")
        and removal_queue.get("id")
        and admission_queue.get("id") != removal_queue.get("id")
    )
    merge_event_precedes_or_equals_merge = bool(
        latest_cycle_merge
        and duration_seconds(latest_cycle_merge.get("created_at"), merged_time)
        is not None
    )
    if merged_after_admission and latest_cycle is not None:
        if removal_class in {
            "known_failure_or_invalidation",
            "known_manual_or_pr_state_change",
            "unknown_or_unsupported",
        }:
            pairing_basis = "latest cycle ended with a removal reason that cannot prove merge completion"
        elif not merge_commit_matches_event:
            pairing_basis = "timeline has no merge event with a compatible merge commit"
        elif not merge_event_precedes_or_equals_merge:
            pairing_basis = "merge event ordering cannot be paired with mergedAt"
        elif not queue_identity_matches:
            pairing_basis = "admission and removal refer to different merge queues"
        else:
            queue_to_merge = duration_seconds(latest_cycle_admission_time, merged_time)
            pairing_confidence = "authoritative"
            if latest_cycle_removal is None:
                pairing_basis = "MergedEvent with compatible commit follows latest admission"
            elif latest_cycle_removal.get("created_at") == merged_time:
                pairing_basis = "merge and queue removal share the latest event timestamp"
            elif latest_cycle_removal.get("created_at") and latest_cycle_removal.get("created_at") < merged_time:
                pairing_basis = "queue removal precedes MergedEvent with no explicit failure reason"
            else:
                pairing_basis = "queue removal follows MergedEvent without an explicit failure reason"
            if latest_cycle_removal is not None and removal_class == "not_provided":
                queue_to_merge_inferred = queue_to_merge
                queue_to_merge = None
                pairing_confidence = "inferred"
                pairing_basis = (
                    "reason-less queue removal and compatible MergedEvent; "
                    "queue-cycle completion is not proven"
                )
    lifecycle_complete = bool(timeline.get("available")) and not bool(timeline.get("truncated"))
    if timeline.get("graphql_status") == "partial":
        lifecycle_complete = False
    incomplete_suffix = "incomplete timeline" if not lifecycle_complete else None
    reported_pairing_basis = pairing_basis or incomplete_suffix
    reported_pairing_confidence = pairing_confidence or "unresolved"
    if incomplete_suffix is not None:
        reported_pairing_basis = incomplete_suffix
        reported_pairing_confidence = "unresolved"
    historical = bool(admissions) if lifecycle_complete else None
    return {
        "queue_admission_count": len(admissions) if lifecycle_complete else None,
        "queue_removal_count": len(removals) if lifecycle_complete else None,
        "requeue_count": max(0, len(admissions) - 1) if lifecycle_complete else None,
        "observed_queue_admission_count": len(admissions),
        "observed_queue_removal_count": len(removals),
        "latest_queue_admission_at": (
            latest_admission.get("created_at")
            if latest_admission and lifecycle_complete
            else None
        ),
        "latest_queue_removal_at": (
            latest_removal.get("created_at")
            if latest_removal and lifecycle_complete
            else None
        ),
        "latest_queue_removal_reason": (
            latest_removal.get("reason")
            if latest_removal and lifecycle_complete
            else None
        ),
        "currently_queued": current,
        "historically_queued": historical,
        "lifecycle_status": (
            "partial" if timeline.get("graphql_status") == "partial"
            else "complete" if lifecycle_complete
            else "incomplete"
        ),
        "queue_cycle_history": cycles,
        "merged_at": merged_time,
        "merge_commit": merge_commit
        or (merged_events[-1].get("merge_commit") if merged_events else None),
        "queue_to_merge_seconds": queue_to_merge if incomplete_suffix is None else None,
        "queue_to_merge_inferred_seconds": (
            queue_to_merge_inferred if incomplete_suffix is None else None
        ),
        "queue_to_merge_pairing_basis": reported_pairing_basis,
        "queue_to_merge_pairing_confidence": reported_pairing_confidence,
        "latest_queue_removal_reason_class": removal_class,
        "evidence_note": incomplete_suffix,
    }


def derive_queue_timing(
    collected_at: Any,
    queue_entry: dict[str, Any] | None,
    workflow_runs: list[dict[str, Any]],
    *,
    candidate_sha: str | None = None,
    check_metrics: dict[str, Any] | None = None,
    lifecycle: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Derive queue wait milestones from one candidate cycle and observed checks.

    Workflow-created-to-started values are Actions dispatch measurements. The
    ``queue_to_first_target_workflow_*`` values use the earliest matching
    merge-group target run, while latest observed-check timing uses only checks
    queried for ``candidate_sha``.
    """
    workflow_runs = workflow_runs if isinstance(workflow_runs, list) else []
    enqueued_at = queue_entry.get("enqueued_at") if isinstance(queue_entry, dict) else None
    if (
        enqueued_at is None
        and isinstance(lifecycle, dict)
        and lifecycle.get("lifecycle_status") == "complete"
    ):
        enqueued_at = lifecycle.get("latest_queue_admission_at")
    workflow_head_sha = candidate_sha
    if workflow_head_sha is None and isinstance(queue_entry, dict):
        workflow_head_sha = queue_entry.get("head_commit")
    matching_runs = [
        run
        for run in workflow_runs
        if isinstance(run, dict)
        and (
            not workflow_head_sha
            or run.get("head_sha") in {workflow_head_sha, None}
        )
        and run.get("event") == "merge_group"
    ]
    created = [run.get("created_at") for run in matching_runs if run.get("created_at")]
    started = [run.get("run_started_at") for run in matching_runs if run.get("run_started_at")]
    completed = [run.get("updated_at") for run in matching_runs if run.get("status") == "completed"]
    latest_check = (check_metrics or {}).get("latest_observed_check_completed_at")
    return {
        "queue_age_at_snapshot_seconds": duration_seconds(enqueued_at, collected_at),
        "queue_to_first_target_workflow_created_seconds": (
            duration_seconds(enqueued_at, min(created)) if enqueued_at and created else None
        ),
        "queue_to_first_target_workflow_started_seconds": (
            duration_seconds(enqueued_at, min(started)) if enqueued_at and started else None
        ),
        "queue_to_latest_observed_check_complete_seconds": (
            duration_seconds(enqueued_at, latest_check) if enqueued_at and latest_check else None
        ),
        "queue_to_merge_seconds": (lifecycle or {}).get("queue_to_merge_seconds"),
        "queue_to_merge_inferred_seconds": (lifecycle or {}).get(
            "queue_to_merge_inferred_seconds"
        ),
        "matching_workflow_count": len(
            {
                ("run", run.get("id")) if run.get("id") is not None else ("observation", index)
                for index, run in enumerate(matching_runs)
            }
        ),
        "matching_workflow_attempt_count": len(matching_runs),
        "workflow_observation_status": "observed" if matching_runs else "unresolved",
        "completed_workflow_latest_at": max(completed) if completed else None,
    }


def derive_check_metrics(
    check_runs: Any,
    statuses: Any,
    *,
    snapshot_collected_at: Any = None,
) -> dict[str, Any]:
    """Summarize observed check-run and legacy commit-status completion data."""
    runs = check_runs if isinstance(check_runs, list) else []
    contexts = statuses if isinstance(statuses, list) else []
    started = [
        run.get("started_at")
        for run in runs
        if isinstance(run, dict) and run.get("started_at")
    ]
    completed = [
        run.get("completed_at")
        for run in runs
        if isinstance(run, dict) and run.get("completed_at")
    ]
    last_run = max(
        (run for run in runs if isinstance(run, dict) and run.get("completed_at")),
        key=lambda run: run.get("completed_at") or "",
        default=None,
    )
    conclusion_counts: dict[str, int] = {}
    for run in runs:
        if isinstance(run, dict):
            key = str(run.get("conclusion") or run.get("status") or "unknown")
            conclusion_counts[key] = conclusion_counts.get(key, 0) + 1
    completed_count = sum(
        1
        for run in runs
        if isinstance(run, dict)
        and (run.get("status") == "completed" or run.get("completed_at"))
    )
    pending_count = max(0, len(runs) - completed_count)
    return {
        "observed_check_run_count": len(runs),
        "pending_check_run_count": pending_count,
        "completed_check_run_count": completed_count,
        "snapshot_collected_at": snapshot_collected_at,
        "observed_checks_all_completed": (
            pending_count == 0 if runs else None
        ),
        "observed_status_context_count": len(contexts),
        "earliest_observed_check_started_at": min(started) if started else None,
        "latest_observed_check_completed_at": max(completed) if completed else None,
        "last_completing_observed_check": {
            "id": last_run.get("id"),
            "name": last_run.get("name"),
            "completed_at": last_run.get("completed_at"),
            "conclusion": last_run.get("conclusion"),
        }
        if last_run
        else None,
        "check_conclusion_counts": conclusion_counts,
    }


def derive_merge_commit_check_timing(
    enqueued_at: Any, check_metrics: dict[str, Any] | None
) -> dict[str, Any]:
    """Report a point-in-time merge-commit check snapshot separately.

    The supplied checks must have been queried for the final merge commit. The
    helper deliberately does not call this snapshot complete or combine it
    with merge-group SHA observations.
    """
    completed_at = (check_metrics or {}).get("latest_observed_check_completed_at")
    return {
        "queue_to_latest_observed_merge_commit_check_complete_seconds": (
            duration_seconds(enqueued_at, completed_at)
            if enqueued_at and completed_at
            else None
        ),
        "latest_observed_merge_commit_check_completed_at": completed_at,
        "last_completing_observed_merge_commit_check": (check_metrics or {}).get(
            "last_completing_observed_check"
        ),
    }


def check_runs_request_params(page: int) -> dict[str, int | str]:
    """Request every check-run attempt on a bounded page, including retries."""
    return {"per_page": 100, "page": page, "filter": "all"}


def _parse_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def duration_seconds(start: Any, end: Any) -> float | None:
    start_time = _parse_datetime(start)
    end_time = _parse_datetime(end)
    if start_time is None or end_time is None:
        return None
    seconds = (end_time - start_time).total_seconds()
    return round(seconds, 3) if seconds >= 0 else None


def classify_runner(job: dict[str, Any]) -> str:
    """Classify a job as self-hosted, GitHub-hosted, or unknown from API metadata."""
    labels = [str(label).casefold() for label in job.get("labels", []) or []]
    runner_name = str(job.get("runner_name") or "").casefold()
    group_name = str(job.get("runner_group_name") or "").casefold()
    if "self-hosted" in labels:
        return "self-hosted"
    if "github actions" in group_name or runner_name.startswith("github actions"):
        return "github-hosted"
    return "unknown"


def classify_runner_platform(job: dict[str, Any]) -> str:
    """Infer a coarse runner platform without assuming a missing label."""
    labels = [str(label).casefold() for label in job.get("labels", []) or []]
    runner_name = str(job.get("runner_name") or "").casefold()
    values = labels + [runner_name]
    if any("macos" in value or "mac-os" in value for value in values):
        return "macos"
    if any("linux" in value or "ubuntu" in value for value in values):
        return "linux"
    if any("windows" in value for value in values):
        return "windows"
    return "unknown"


def runner_metadata_missing_unexpectedly(job: dict[str, Any]) -> bool:
    """Report missing runner metadata only when a job appears to have run."""
    conclusion = job.get("conclusion")
    if conclusion == "skipped":
        return False
    completed_after_start = (
        job.get("status") == "completed"
        and conclusion not in {None, "cancelled", "startup_failure"}
    )
    if not job.get("started_at") and not completed_after_start:
        return False
    return not (
        job.get("runner_name")
        or job.get("runner_group_name")
        or job.get("labels")
    )


def derive_timing_metrics(
    latest_runs: dict[str, dict[str, Any] | None],
    jobs_by_run: dict[tuple[int, int], list[dict[str, Any]]],
    all_runs: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Normalize per-attempt timings and sum runner work across retries.

    Each workflow attempt has its own (run ID, attempt) key. Runner seconds sum
    observed completed work from every attempt, including cancelled attempts
    that ran jobs. ``timing_status`` describes latest workflow completion;
    ``runner_time_status`` separately reports whether every observed attempt's
    job listing was complete. Partial runner accounting keeps observed seconds
    as a lower bound and identifies attempts with unavailable job evidence.
    Candidate wall time uses earliest creation to latest completion and does
    not sum retry durations.
    """
    workflow_rows: list[dict[str, Any]] = []
    all_latest = [run for run in latest_runs.values() if isinstance(run, dict)]
    for name in TARGET_WORKFLOWS:
        run = latest_runs.get(name)
        if not isinstance(run, dict):
            workflow_rows.append(
                {
                    "workflow_name": name,
                    "run_id": None,
                    "status": "not_observed",
                    "conclusion": None,
                    "workflow_created_to_run_started_seconds": None,
                    "runtime_seconds": None,
                }
            )
            continue
        run_id = run.get("id")
        attempt = run.get("run_attempt") or 1
        is_complete = run.get("status") == "completed"
        end = run.get("updated_at") if is_complete else None
        workflow_rows.append(
            {
                "workflow_name": name,
                "workflow_id": run.get("workflow_id"),
                "run_id": run_id,
                "run_number": run.get("run_number"),
                "run_attempt": attempt,
                "event": run.get("event"),
                "head_branch": run.get("head_branch"),
                "head_sha": run.get("head_sha"),
                "status": run.get("status"),
                "conclusion": run.get("conclusion"),
                "created_at": run.get("created_at"),
                "run_started_at": run.get("run_started_at"),
                "updated_at": run.get("updated_at"),
                "html_url": run.get("html_url"),
                "check_suite_id": run.get("check_suite_id"),
                "workflow_created_to_run_started_seconds": duration_seconds(
                    run.get("created_at"), run.get("run_started_at")
                ),
                "runtime_seconds": duration_seconds(run.get("run_started_at"), end),
            }
        )

    complete = all(
        isinstance(latest_runs.get(name), dict)
        and latest_runs[name].get("status") == "completed"
        for name in TARGET_WORKFLOWS
    )
    observed_target_runs = [
        run
        for run in (all_runs if all_runs is not None else all_latest)
        if (run.get("name") or run.get("workflow_name")) in TARGET_WORKFLOWS
    ]
    runner_time_missing_attempts = []
    for run in observed_target_runs:
        run_id = run.get("id")
        attempt = int(run.get("run_attempt") or 1)
        key = (run_id, attempt)
        jobs_complete = run.get("jobs_complete")
        if jobs_complete is None:
            jobs_complete = key in jobs_by_run
        if jobs_complete is not True or key not in jobs_by_run:
            runner_time_missing_attempts.append(
                {
                    "run_id": run_id,
                    "run_attempt": attempt,
                    "workflow_name": run.get("name") or run.get("workflow_name"),
                    "reason": "jobs_unavailable",
                    "attempt_metadata_available": run.get(
                        "attempt_metadata_available"
                    ),
                }
            )
    created_times = [
        timestamp
        for run in observed_target_runs
        if (timestamp := _parse_datetime(run.get("created_at"))) is not None
    ]
    completed_times = [
        timestamp
        for run in observed_target_runs
        if run.get("status") == "completed"
        and (timestamp := _parse_datetime(run.get("updated_at"))) is not None
    ]
    earliest = min(created_times, default=None)
    latest_completion = max(completed_times, default=None)
    candidate_wall = (
        round((latest_completion - earliest).total_seconds(), 3)
        if complete and earliest is not None and latest_completion is not None
        else None
    )

    runner_seconds = {"self-hosted": 0.0, "github-hosted": 0.0, "unknown": 0.0}
    runner_platform_seconds = {
        "linux": 0.0,
        "macos": 0.0,
        "windows": 0.0,
        "unknown": 0.0,
    }
    runner_workflow_seconds: dict[str, float] = {}
    job_rows: list[dict[str, Any]] = []
    run_by_key = {
        (run.get("id"), int(run.get("run_attempt") or 1)): run
        for run in (all_runs if all_runs is not None else all_latest)
        if isinstance(run, dict) and isinstance(run.get("id"), int)
    }
    for (run_id, attempt), jobs in sorted(jobs_by_run.items()):
        for job in jobs:
            runtime = duration_seconds(job.get("started_at"), job.get("completed_at"))
            runner_class = classify_runner(job)
            runner_platform = classify_runner_platform(job)
            run = run_by_key.get((run_id, attempt), {})
            if (
                runtime is not None
                and job.get("status") == "completed"
                and job.get("conclusion") != "skipped"
            ):
                runner_seconds[runner_class] += runtime
                runner_platform_seconds[runner_platform] += runtime
                workflow_name = str(job.get("workflow_name") or "unknown")
                runner_workflow_seconds[workflow_name] = (
                    runner_workflow_seconds.get(workflow_name, 0.0) + runtime
                )
            job_row = {
                "workflow_name": job.get("workflow_name"),
                "workflow_id": run.get("workflow_id"),
                "run_id": run_id,
                "run_number": run.get("run_number"),
                "run_attempt": attempt,
                "job_id": job.get("id"),
                "job_name": job.get("name"),
                "status": job.get("status"),
                "conclusion": job.get("conclusion"),
                "started_at": job.get("started_at"),
                "completed_at": job.get("completed_at"),
                "runner_name": job.get("runner_name"),
                "runner_id": job.get("runner_id"),
                "runner_group_id": job.get("runner_group_id"),
                "runner_group_name": job.get("runner_group_name"),
                "labels": job.get("labels"),
                "runner_classification": runner_class,
                "runner_platform": runner_platform,
                "runtime_seconds": runtime,
                "workflow_created_to_job_started_seconds": duration_seconds(
                    run.get("created_at"), job.get("started_at")
                ),
                "workflow_run_started_to_job_started_seconds": duration_seconds(
                    run.get("run_started_at"), job.get("started_at")
                ),
                "steps": [
                    {
                        "name": step.get("name"),
                        "status": step.get("status"),
                        "conclusion": step.get("conclusion"),
                        "started_at": step.get("started_at"),
                        "completed_at": step.get("completed_at"),
                        "duration_seconds": duration_seconds(
                            step.get("started_at"), step.get("completed_at")
                        ),
                    }
                    for step in job.get("steps", []) or []
                ],
            }
            job_rows.append(job_row)

    for runner_class in runner_seconds:
        runner_seconds[runner_class] = round(runner_seconds[runner_class], 3)
    for platform in runner_platform_seconds:
        runner_platform_seconds[platform] = round(runner_platform_seconds[platform], 3)
    runner_workflow_seconds = {
        workflow: round(seconds, 3)
        for workflow, seconds in runner_workflow_seconds.items()
    }

    completed_workflows = [
        row for row in workflow_rows if row.get("status") == "completed"
    ]
    longest_workflow = max(
        completed_workflows,
        key=lambda row: row.get("runtime_seconds") or -1,
        default=None,
    )
    completed_jobs = [
        row
        for row in job_rows
        if row.get("runtime_seconds") is not None
        and row.get("conclusion") != "skipped"
    ]
    longest_job = max(
        completed_jobs,
        key=lambda row: row.get("runtime_seconds") or -1,
        default=None,
    )
    last_completing_workflow = max(
        completed_workflows,
        key=lambda row: row.get("updated_at") or "",
        default=None,
    )
    last_completing_job = max(
        completed_jobs,
        key=lambda row: row.get("completed_at") or "",
        default=None,
    )

    return {
        "timing_status": "complete" if complete else "partial",
        "runner_time_status": (
            "partial" if runner_time_missing_attempts else "complete"
        ),
        "runner_time_missing_attempts": runner_time_missing_attempts,
        "target_workflows": list(TARGET_WORKFLOWS),
        "workflow_rows": workflow_rows,
        "candidate_wall_time_seconds": candidate_wall,
        "runner_time_seconds": runner_seconds,
        "runner_time_seconds_by_platform": runner_platform_seconds,
        "runner_time_seconds_by_workflow": runner_workflow_seconds,
        "longest_workflow": longest_workflow,
        "longest_job": longest_job,
        "last_completing_workflow": last_completing_workflow,
        "last_completing_job": last_completing_job,
        "job_rows": job_rows,
    }


class GitHubAPI:
    def __init__(self, warnings: list[dict[str, str]]) -> None:
        self.warnings = warnings
        self.api_errors: list[dict[str, Any]] = []
        self.token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
        self.api_base = os.environ.get("GITHUB_API_URL", "https://api.github.com").rstrip("/")
        self.repo_name = os.environ.get("GITHUB_REPOSITORY", "")
        self.owner, separator, self.repo = self.repo_name.partition("/")
        if not separator:
            self.owner = ""
            self.repo = ""
        if self.api_base.endswith("/api/v3"):
            self.graphql_url = self.api_base[: -len("/api/v3")] + "/api/graphql"
        else:
            self.graphql_url = self.api_base + "/graphql"

    def _request(
        self,
        url: str,
        *,
        method: str = "GET",
        payload: dict[str, Any] | None = None,
        warning_label: str,
    ) -> Any:
        if not self.token:
            self.api_errors.append(
                normalize_api_error(warning_label, None, "missing_token")
            )
            self.warnings.append(
                {
                    "category": "api_unavailable",
                    "message": "GitHub token is unavailable",
                    "endpoint": warning_label,
                }
            )
            return None
        body = json.dumps(payload).encode("utf-8") if payload is not None else None
        headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {self.token}",
            "User-Agent": "logos-merge-queue-telemetry",
        }
        if not url.endswith("/graphql"):
            headers["X-GitHub-Api-Version"] = "2026-03-10"
        if body is not None:
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(url, data=body, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
                raw = response.read()
        except urllib.error.HTTPError as error:
            raw = error.read()
            detail = _api_error_detail(raw)
            self.api_errors.append(
                normalize_api_error(
                    warning_label, error.code, "http_error", raw
                )
            )
            self.warnings.append(
                {
                    "category": "api_http_error",
                    "endpoint": warning_label,
                    "message": f"GitHub API returned HTTP {error.code}: {detail}",
                }
            )
            return None
        except (urllib.error.URLError, TimeoutError) as error:
            self.api_errors.append(
                normalize_api_error(
                    warning_label, None, type(error).__name__, str(error)
                )
            )
            self.warnings.append(
                {
                    "category": "api_request_error",
                    "endpoint": warning_label,
                    "message": f"GitHub API request failed: {type(error).__name__}",
                }
            )
            return None
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            self.api_errors.append(
                normalize_api_error(
                    warning_label, None, "invalid_json", raw
                )
            )
            self.warnings.append(
                {
                    "category": "api_invalid_response",
                    "endpoint": warning_label,
                    "message": "GitHub API response was not valid JSON",
                }
            )
            return None

    def rest(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        warning_label: str | None = None,
    ) -> Any:
        url = self.api_base + "/" + path.lstrip("/")
        if params:
            url += "?" + urllib.parse.urlencode(params)
        return self._request(
            url, warning_label=warning_label or path
        )

    def graphql(self, query: str, variables: dict[str, Any], warning_label: str) -> Any:
        return self._request(
            self.graphql_url,
            method="POST",
            payload={"query": query, "variables": variables},
            warning_label=warning_label,
        )

    def associated_pulls(self, sha: str) -> Any:
        if not self.owner or not self.repo:
            self.warnings.append(
                {"category": "api_configuration", "message": "GITHUB_REPOSITORY is unavailable"}
            )
            return None
        owner = urllib.parse.quote(self.owner, safe="")
        repo = urllib.parse.quote(self.repo, safe="")
        return self.rest(
            f"repos/{owner}/{repo}/commits/{urllib.parse.quote(sha, safe='')}/pulls",
            warning_label=f"associated pull requests for commit {sha}",
        )

    def commit(self, sha: str) -> Any:
        if not self.owner or not self.repo:
            return None
        owner = urllib.parse.quote(self.owner, safe="")
        repo = urllib.parse.quote(self.repo, safe="")
        return self.rest(
            f"repos/{owner}/{repo}/commits/{urllib.parse.quote(sha, safe='')}",
            warning_label=f"commit metadata for {sha}",
        )

    def pull_request(self, number: int) -> Any:
        if not self.owner or not self.repo:
            return None
        owner = urllib.parse.quote(self.owner, safe="")
        repo = urllib.parse.quote(self.repo, safe="")
        return self.rest(
            f"repos/{owner}/{repo}/pulls/{number}",
            warning_label=f"pull request #{number}",
        )

    def workflow_run_attempt(self, run_id: int, attempt: int) -> Any:
        if not self.owner or not self.repo:
            return None
        owner = urllib.parse.quote(self.owner, safe="")
        repo = urllib.parse.quote(self.repo, safe="")
        return self.rest(
            f"repos/{owner}/{repo}/actions/runs/{run_id}/attempts/{attempt}",
            warning_label=f"workflow run {run_id}, attempt {attempt}",
        )

    def check_runs(self, sha: str, *, page: int = 1) -> Any:
        """Fetch one read-only Checks API page for a commit SHA."""
        if not self.owner or not self.repo:
            return None
        owner = urllib.parse.quote(self.owner, safe="")
        repo = urllib.parse.quote(self.repo, safe="")
        return self.rest(
            f"repos/{owner}/{repo}/commits/{urllib.parse.quote(sha, safe='')}/check-runs",
            params=check_runs_request_params(page),
            warning_label=f"check runs for commit {sha}",
        )

    def commit_statuses(self, sha: str, *, page: int = 1) -> Any:
        """Fetch one read-only legacy commit-status page for a commit SHA."""
        if not self.owner or not self.repo:
            return None
        owner = urllib.parse.quote(self.owner, safe="")
        repo = urllib.parse.quote(self.repo, safe="")
        return self.rest(
            f"repos/{owner}/{repo}/commits/{urllib.parse.quote(sha, safe='')}/status",
            params={"per_page": 100, "page": page},
            warning_label=f"commit statuses for {sha}",
        )

    def stack_graphql(self, number: int) -> Any:
        if not self.owner or not self.repo:
            return None
        query = """
        query($owner: String!, $name: String!, $number: Int!) {
          repository(owner: $owner, name: $name) {
            pullRequest(number: $number) {
              id
              number
              title
              state
              isDraft
              baseRefName
              baseRefOid
              headRefName
              headRefOid
              mergeStateStatus
              stack {
                id
                number
                size
                baseRefName
                entries(first: __MAX_QUEUE_ENTRIES__) {
                  totalCount
                  nodes {
                    id
                    position
                    pullRequest {
                      number
                      title
                      state
                      baseRefName
                      baseRefOid
                      headRefName
                      headRefOid
                    }
                  }
                }
              }
              stackEntry {
                id
                position
                stack {
                  id
                  number
                  size
                  baseRefName
                }
              }
            }
          }
        }
        """.replace("__MAX_QUEUE_ENTRIES__", str(MAX_QUEUE_ENTRIES))
        return self.graphql(
            query,
            {"owner": self.owner, "name": self.repo, "number": number},
            f"GraphQL stack metadata for pull request #{number}",
        )

    def candidate_queue_entry_graphql(self, number: int) -> Any:
        """Fetch only candidate-specific queue-entry and queue-state fields."""
        if not self.owner or not self.repo:
            return None
        query = """
        query($owner: String!, $name: String!, $number: Int!) {
          repository(owner: $owner, name: $name) {
            pullRequest(number: $number) {
              number
              merged
              mergedAt
              mergeCommit { oid }
              isInMergeQueue
              isMergeQueueEnabled
              mergeQueueEntry {
                id
                enqueuedAt
                enqueuer { login }
                estimatedTimeToMerge
                position
                state
                jump
                solo
                baseCommit { oid }
                headCommit { oid }
                pullRequest { number }
                mergeQueue { id url resourcePath nextEntryEstimatedTimeToMerge }
              }
            }
          }
        }
        """
        return self.graphql(
            query,
            {"owner": self.owner, "name": self.repo, "number": number},
            f"GraphQL candidate merge-queue entry for pull request #{number}",
        )

    def repository_merge_queue_graphql(self, queue_branch: str) -> Any:
        """Fetch bounded queue policy/topology for the repository trunk branch."""
        if not self.owner or not self.repo:
            return None
        query = """
        query($owner: String!, $name: String!, $branch: String!) {
          repository(owner: $owner, name: $name) {
            mergeQueue(branch: $branch) {
              id
              url
              resourcePath
              nextEntryEstimatedTimeToMerge
              configuration {
                checkResponseTimeout
                maximumEntriesToBuild
                maximumEntriesToMerge
                minimumEntriesToMerge
                minimumEntriesToMergeWaitTime
                mergeMethod
                mergingStrategy
              }
              entries(first: __MAX_QUEUE_ENTRIES__) {
                totalCount
                pageInfo { hasNextPage endCursor }
                nodes {
                  id
                  enqueuedAt
                  enqueuer { login }
                  estimatedTimeToMerge
                  position
                  state
                  jump
                  solo
                  baseCommit { oid }
                  headCommit { oid }
                  pullRequest {
                    number
                    baseRefName
                    baseRefOid
                    headRefName
                    headRefOid
                    stack { id number size baseRefName }
                    stackEntry { position }
                  }
                }
              }
            }
          }
        }
        """.replace("__MAX_QUEUE_ENTRIES__", str(MAX_QUEUE_ENTRIES))
        return self.graphql(
            query,
            {"owner": self.owner, "name": self.repo, "branch": queue_branch},
            f"GraphQL repository merge-queue topology for {queue_branch}",
        )

    def timeline_graphql(self, number: int) -> Any:
        """Fetch bounded queue lifecycle events and merge evidence for a PR."""
        if not self.owner or not self.repo:
            return None
        item_types = "\n                  ".join(MERGE_QUEUE_TIMELINE_ITEM_TYPES)
        query = """
        query($owner: String!, $name: String!, $number: Int!) {
          repository(owner: $owner, name: $name) {
            pullRequest(number: $number) {
              number
              merged
              mergedAt
              mergeCommit { oid }
              timelineItems(
                first: __MAX_TIMELINE_EVENTS__
                itemTypes: [
                  __TIMELINE_ITEM_TYPES__
                ]
              ) {
                totalCount
                pageInfo { hasNextPage endCursor }
                nodes {
                  __typename
                  ... on AddedToMergeQueueEvent {
                    id
                    createdAt
                    actor { login }
                    enqueuer { login }
                    mergeQueue { id url resourcePath }
                  }
                  ... on RemovedFromMergeQueueEvent {
                    id
                    createdAt
                    actor { login }
                    enqueuer { login }
                    reason
                    beforeCommit { oid }
                    mergeQueue { id url resourcePath }
                  }
                  ... on MergedEvent {
                    id
                    createdAt
                    actor { login }
                    commit { oid }
                  }
                }
              }
            }
          }
        }
        """.replace("__TIMELINE_ITEM_TYPES__", item_types).replace(
            "__MAX_TIMELINE_EVENTS__", str(MAX_TIMELINE_EVENTS)
        )
        return self.graphql(
            query,
            {"owner": self.owner, "name": self.repo, "number": number},
            f"GraphQL merge queue timeline for pull request #{number}",
        )


def _api_error_detail(raw: Any) -> str:
    if isinstance(raw, str):
        return raw[:240]
    if isinstance(raw, dict):
        message = raw.get("message")
        return message[:240] if isinstance(message, str) else "response body omitted"
    if not isinstance(raw, (bytes, bytearray)):
        return "response body omitted"
    try:
        value = json.loads(bytes(raw).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return "response body omitted"
    if isinstance(value, dict) and isinstance(value.get("message"), str):
        return value["message"][:240]
    return "response body omitted"


def _write_raw_api(path: Path, value: Any) -> None:
    if value is not None:
        save_json(path, value)


def _write_api_errors(api: GitHubAPI, output_dir: Path) -> None:
    """Persist sanitized REST failure records without credentials or headers."""
    save_json(output_dir / "api" / "api-errors.json", api.api_errors)


def _git(
    repo: Path,
    args: list[str],
    warnings: list[dict[str, str]],
    *,
    label: str,
) -> str | None:
    completed = subprocess.run(
        ["git", "-C", str(repo), *args],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if completed.returncode != 0:
        warnings.append(
            {
                "category": "git_unavailable",
                "message": f"{label} failed (exit {completed.returncode})",
            }
        )
        return None
    return completed.stdout


def bounded_git_fetch_args(ref: str) -> list[str]:
    """Build an explicit, bounded fetch command for a ref or object SHA."""
    return ["fetch", "--no-tags", f"--depth={GIT_FETCH_DEPTH}", "origin", ref]


def ancestry_from_exit_code(
    return_code: int, history_shallow: bool | None
) -> bool | None:
    """Interpret ancestor checks conservatively when bounded history is shallow."""
    if return_code == 0:
        return True
    if return_code == 1:
        return False if history_shallow is False else None
    return None


def _git_is_ancestor(
    repo: Path,
    possible_ancestor: Any,
    possible_descendant: Any,
    warnings: list[dict[str, str]],
    *,
    history_shallow: bool | None = None,
) -> bool | None:
    if (
        not isinstance(possible_ancestor, str)
        or not re.fullmatch(r"[0-9a-fA-F]{40,64}", possible_ancestor)
        or not isinstance(possible_descendant, str)
        or not re.fullmatch(r"[0-9a-fA-F]{40,64}", possible_descendant)
    ):
        return None
    completed = subprocess.run(
        [
            "git",
            "-C",
            str(repo),
            "merge-base",
            "--is-ancestor",
            possible_ancestor,
            possible_descendant,
        ],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if completed.returncode == 0:
        return True
    if completed.returncode == 1:
        result = ancestry_from_exit_code(
            completed.returncode, history_shallow
        )
        if result is None and not any(
            warning.get("category") == "git_ancestry_incomplete"
            for warning in warnings
        ):
            warnings.append(
                {
                    "category": "git_ancestry_incomplete",
                    "message": (
                        "A negative ancestry result is unresolved because bounded Git "
                        "history is shallow or its completeness could not be determined"
                    ),
                }
            )
        return result
    warnings.append(
        {
            "category": "git_ancestry_unavailable",
            "message": "Could not compare a PR head SHA with the candidate ancestry",
        }
    )
    return None


def _write_git_text(
    repo: Path,
    output_path: Path,
    args: list[str],
    warnings: list[dict[str, str]],
    *,
    label: str,
) -> str | None:
    value = _git(repo, args, warnings, label=label)
    if value is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(value, encoding="utf-8")
    return value


def _api_run_pages(
    api: GitHubAPI, head_sha: str, output_dir: Path, warnings: list[dict[str, str]]
) -> tuple[list[dict[str, Any]], list[Any]]:
    """Collect bounded Actions workflow-run pages for a candidate SHA."""
    all_runs: list[dict[str, Any]] = []
    raw_pages: list[Any] = []
    for page in range(1, MAX_API_PAGES + 1):
        data = api.rest(
            "repos/" + urllib.parse.quote(api.repo_name, safe="/") + "/actions/runs",
            params={"head_sha": head_sha, "per_page": 100, "page": page},
            warning_label=f"Actions runs for head SHA {head_sha}, page {page}",
        )
        if data is None:
            break
        raw_pages.append(data)
        _write_raw_api(output_dir / f"page-{page}.json", data)
        runs = data.get("workflow_runs", []) if isinstance(data, dict) else []
        if isinstance(runs, list):
            all_runs.extend(item for item in runs if isinstance(item, dict))
        if not isinstance(runs, list) or len(runs) < 100:
            break
        if page == MAX_API_PAGES:
            warnings.append(
                {
                    "category": "api_page_limit",
                    "message": f"Actions run listing reached the {MAX_API_PAGES}-page limit",
                }
            )
    return all_runs, raw_pages


def _api_job_pages(
    api: GitHubAPI,
    run_id: int,
    attempt: int,
    output_dir: Path,
    warnings: list[dict[str, str]],
) -> tuple[list[dict[str, Any]], list[Any], bool]:
    """Collect bounded job pages and report whether the listing is complete.

    A successful empty page is complete evidence of zero observed jobs. An API
    failure, malformed page, or exhausted page bound returns ``False`` so
    runner totals can remain visible while their completeness is marked partial.
    """
    jobs: list[dict[str, Any]] = []
    raw_pages: list[Any] = []
    complete = False
    for page in range(1, MAX_API_PAGES + 1):
        path = (
            f"repos/{urllib.parse.quote(api.repo_name, safe='/')}/actions/runs/"
            f"{run_id}/attempts/{attempt}/jobs"
        )
        data = api.rest(
            path,
            params={"per_page": 100, "page": page},
            warning_label=f"jobs for workflow run {run_id}, attempt {attempt}, page {page}",
        )
        if data is None:
            break
        raw_pages.append(data)
        _write_raw_api(output_dir / f"page-{page}.json", data)
        if not isinstance(data, dict):
            break
        page_jobs = data.get("jobs")
        if not isinstance(page_jobs, list):
            break
        jobs.extend(item for item in page_jobs if isinstance(item, dict))
        if len(page_jobs) < 100:
            complete = True
            break
        if page == MAX_API_PAGES:
            warnings.append(
                {
                    "category": "api_page_limit",
                    "message": (
                        f"Job listing for run {run_id} reached the "
                        f"{MAX_API_PAGES}-page limit"
                    ),
                }
            )
    return jobs, raw_pages, complete


def _collect_workflow_attempts(
    api: GitHubAPI,
    observed_runs: list[dict[str, Any]],
    source_run: dict[str, Any],
    output_dir: Path,
    warnings: list[dict[str, str]],
    *,
    preloaded_details: dict[tuple[int, int], Any] | None = None,
    preloaded_jobs: dict[tuple[int, int], list[dict[str, Any]]] | None = None,
    preloaded_jobs_complete: dict[tuple[int, int], bool] | None = None,
) -> tuple[list[dict[str, Any]], dict[tuple[int, int], list[dict[str, Any]]]]:
    """Collect each observed target workflow's bounded attempt history.

    A GitHub run's ``run_attempt=N`` exposes attempts 1 through N under the
    same run ID. Each attempt's metadata and jobs are retained separately;
    job-list completeness is recorded independently of attempt metadata, so
    available jobs can still establish cost completeness when metadata is
    missing. Preloaded source-event data avoids refetching the current attempt
    and prevents duplicate runner cost.
    """
    details = dict(preloaded_details or {})
    jobs_by_run = dict(preloaded_jobs or {})
    jobs_completeness = dict(preloaded_jobs_complete or {})
    source_id = source_run.get("id")
    source_attempt = int(source_run.get("run_attempt") or 1)
    latest_by_id: dict[int, dict[str, Any]] = {}
    for run in observed_runs:
        run_id = run.get("id")
        if not isinstance(run_id, int):
            warnings.append(
                {
                    "category": "missing_run_id",
                    "message": f"Observed workflow {run.get('name')} had no run id",
                }
            )
            continue
        existing = latest_by_id.get(run_id)
        if existing is None or int(run.get("run_attempt") or 1) > int(
            existing.get("run_attempt") or 1
        ):
            latest_by_id[run_id] = run
    if isinstance(source_id, int):
        existing = latest_by_id.get(source_id)
        if existing is None or source_attempt >= int(existing.get("run_attempt") or 1):
            latest_by_id[source_id] = source_run

    attempts: list[dict[str, Any]] = []
    for run_id, latest in sorted(latest_by_id.items()):
        attempt_count = max(1, int(latest.get("run_attempt") or 1))
        for attempt in range(1, attempt_count + 1):
            key = (run_id, attempt)
            if key not in details:
                details[key] = api.workflow_run_attempt(run_id, attempt)
                _write_raw_api(
                    output_dir / "run-attempts" / f"run-{run_id}-attempt-{attempt}.json",
                    details[key],
                )
            detail = details[key]
            if isinstance(detail, dict):
                record = dict(detail)
                record["attempt_metadata_source"] = "workflow_run_attempt_api"
            elif attempt == attempt_count or (run_id == source_id and attempt == source_attempt):
                fallback_run = (
                    source_run
                    if run_id == source_id and attempt == source_attempt
                    else latest
                )
                record = dict(fallback_run)
                record["attempt_metadata_source"] = (
                    "source_workflow_run_event"
                    if run_id == source_id and attempt == source_attempt
                    else "workflow_runs_listing"
                )
                if not (run_id == source_id and attempt == source_attempt):
                    warnings.append(
                        {
                            "category": "workflow_attempt_details_unavailable",
                            "message": (
                                f"Could not fetch metadata for latest workflow run {run_id} "
                                f"attempt {attempt}; used its observed run record"
                            ),
                        }
                    )
            else:
                record = {
                    key_name: latest.get(key_name)
                    for key_name in (
                        "name", "workflow_id", "run_number", "event", "head_branch",
                        "head_sha", "created_at", "html_url", "check_suite_id",
                    )
                }
                record.update(
                    {
                        "id": run_id,
                        "run_attempt": attempt,
                        "status": "unavailable",
                        "conclusion": None,
                        "run_started_at": None,
                        "updated_at": None,
                        "attempt_metadata_source": "unavailable",
                    }
                )
                warnings.append(
                    {
                        "category": "historical_workflow_attempt_unavailable",
                        "message": (
                            f"Historical workflow run {run_id} attempt {attempt} was "
                            "not available from the Actions API"
                        ),
                    }
                )
            record["id"] = run_id
            record["run_attempt"] = attempt
            record["attempt_metadata_available"] = (
                detail is not None
                or attempt == attempt_count
                or (run_id == source_id and attempt == source_attempt)
            )
            attempts.append(record)

            if key not in jobs_by_run:
                attempt_jobs, _, attempt_jobs_complete = _api_job_pages(
                    api,
                    run_id,
                    attempt,
                    output_dir / "jobs" / f"run-{run_id}-attempt-{attempt}",
                    warnings,
                )
                jobs_by_run[key] = attempt_jobs
                jobs_completeness[key] = attempt_jobs_complete
            elif key not in jobs_completeness:
                # Explicitly preloaded job rows are a successful observation
                # unless the caller also supplied a failed/incomplete marker.
                jobs_completeness[key] = True
            record["jobs_complete"] = jobs_completeness.get(
                key, key in jobs_by_run
            )
            if record["jobs_complete"] is not True:
                warnings.append(
                    {
                        "category": "workflow_attempt_jobs_unavailable",
                        "message": (
                            f"Jobs for workflow run {run_id} attempt {attempt} "
                            "were unavailable or incomplete"
                        ),
                    }
                )
    attempts.sort(
        key=lambda run: (
            run.get("name") or "",
            int(run.get("run_number") or 0),
            int(run.get("id") or 0),
            int(run.get("run_attempt") or 0),
        )
    )
    return attempts, jobs_by_run


def _api_check_pages(
    api: GitHubAPI,
    sha: str,
    output_dir: Path,
    warnings: list[dict[str, str]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[Any], list[Any]]:
    """Collect bounded raw check/status pages for one SHA, preserving retries.

    Check Runs requests use ``filter=all`` and remain paginated, so normalized
    counts describe every observed attempt rather than only GitHub's latest
    result. The caller supplies either a merge-group candidate SHA or a final
    merge commit SHA and must keep those views separate.
    """
    check_runs: list[dict[str, Any]] = []
    statuses: list[dict[str, Any]] = []
    raw_check_pages: list[Any] = []
    raw_status_pages: list[Any] = []
    for page in range(1, MAX_API_PAGES + 1):
        data = api.check_runs(sha, page=page)
        if data is not None:
            raw_check_pages.append(data)
            _write_raw_api(output_dir / f"check-runs-page-{page}.json", data)
            page_runs = data.get("check_runs", []) if isinstance(data, dict) else []
            if isinstance(page_runs, list):
                check_runs.extend(item for item in page_runs if isinstance(item, dict))
            if not isinstance(page_runs, list) or len(page_runs) < 100:
                break
        else:
            break
        if page == MAX_API_PAGES:
            warnings.append(
                {
                    "category": "api_page_limit",
                    "message": f"Check-run listing reached the {MAX_API_PAGES}-page limit",
                }
            )
    for page in range(1, MAX_API_PAGES + 1):
        status_data = api.commit_statuses(sha, page=page)
        if status_data is None:
            break
        raw_status_pages.append(status_data)
        _write_raw_api(output_dir / f"commit-statuses-page-{page}.json", status_data)
        page_statuses = status_data.get("statuses", []) if isinstance(status_data, dict) else []
        if isinstance(page_statuses, list):
            statuses.extend(item for item in page_statuses if isinstance(item, dict))
        if not isinstance(page_statuses, list) or len(page_statuses) < 100:
            break
        if page == MAX_API_PAGES:
            warnings.append(
                {
                    "category": "api_page_limit",
                    "message": f"Commit-status listing reached the {MAX_API_PAGES}-page limit",
                }
            )
    save_json(
        output_dir / "check-runs.json",
        {"pages": len(raw_check_pages), "check_runs": check_runs},
    )
    save_json(
        output_dir / "commit-statuses.json",
        {"pages": len(raw_status_pages), "statuses": statuses},
    )
    return check_runs, statuses, raw_check_pages, raw_status_pages


def _pr_metadata(
    api: GitHubAPI, numbers: set[int], output_dir: Path, warnings: list[dict[str, str]]
) -> dict[int, dict[str, Any] | None]:
    raw_dir = output_dir / "api" / "prs"
    metadata: dict[int, dict[str, Any] | None] = {}
    for number in sorted(numbers):
        rest_value = api.pull_request(number)
        _write_raw_api(raw_dir / f"pr-{number}.json", rest_value)
        if not isinstance(rest_value, dict):
            metadata[number] = None
        else:
            metadata[number] = rest_value
        graphql_value = api.stack_graphql(number)
        _write_raw_api(raw_dir / f"pr-{number}-graphql.json", graphql_value)
        if isinstance(graphql_value, dict) and graphql_value.get("errors"):
            warnings.append(
                {
                    "category": "stack_metadata_unavailable",
                    "message": f"GraphQL stack lookup for PR #{number} returned errors",
                }
            )
    return metadata


def _stack_records(
    numbers: set[int],
    rest_metadata: dict[int, dict[str, Any] | None],
    output_dir: Path,
) -> dict[int, dict[str, Any]]:
    result: dict[int, dict[str, Any]] = {}
    for number in sorted(numbers):
        graphql_path = output_dir / "api" / "prs" / f"pr-{number}-graphql.json"
        graphql_value = None
        if graphql_path.exists():
            graphql_value = json.loads(graphql_path.read_text(encoding="utf-8"))
        result[number] = classify_stack(graphql_value, rest_metadata.get(number))
    return result


def _metadata_summary(pr: dict[str, Any] | None, stack: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(pr, dict):
        return None
    base = pr.get("base") if isinstance(pr.get("base"), dict) else {}
    head = pr.get("head") if isinstance(pr.get("head"), dict) else {}
    return {
        "number": pr.get("number"),
        "title": pr.get("title"),
        "state": pr.get("state"),
        "draft": pr.get("draft"),
        "base_ref": base.get("ref"),
        "base_sha": base.get("sha"),
        "head_ref": head.get("ref"),
        "head_sha": head.get("sha"),
        "mergeable": pr.get("mergeable"),
        "mergeable_state": pr.get("mergeable_state"),
        "html_url": pr.get("html_url"),
        "stack": stack,
    }


def _graphql_response_warning(
    response: Any, warnings: list[dict[str, str]], *, label: str
) -> None:
    """Record a warning for partial/unavailable GraphQL evidence."""
    status, errors = graphql_response_status(response)
    if status == "partial":
        warnings.append(
            {
                "category": "graphql_partial",
                "message": f"{label} returned usable data with {len(errors)} GraphQL error(s)",
            }
        )
    elif status == "unavailable" and isinstance(response, dict):
        warnings.append(
            {
                "category": "graphql_unavailable",
                "message": f"{label} returned no usable GraphQL data",
            }
        )


def _collect_queue_observation(
    api: GitHubAPI,
    candidate_pr: int | None,
    queue_branch: str | None,
    queue_branch_source: str,
    output_dir: Path,
    warnings: list[dict[str, str]],
) -> dict[str, Any]:
    """Collect trunk queue topology and optional candidate lifecycle evidence.

    Candidate-entry and repository-queue GraphQL requests are independent and
    each raw response is retained. Repository-level topology is collected
    whenever ``queue_branch`` is known, even when candidate ownership is
    unresolved. A unique repository entry is reconciled with the PR-specific
    entry; identity conflicts, mutable queue-state drift, and partial GraphQL
    statuses remain distinct. Absence is reported only when a resolved PR has
    no match in a complete repository snapshot. If either observation is
    unavailable or the repository snapshot is truncated, a null entry remains
    unresolved. PullRequest timeline and entry data are collected only for a
    resolved candidate number.
    """
    raw_dir = output_dir / "api" / "queue"
    candidate_observed_at = utc_now()
    candidate_response = (
        api.candidate_queue_entry_graphql(candidate_pr)
        if isinstance(candidate_pr, int)
        else None
    )
    repository_observed_at = utc_now()
    repository_response = (
        api.repository_merge_queue_graphql(queue_branch)
        if isinstance(queue_branch, str) and queue_branch
        else None
    )
    _write_raw_api(raw_dir / "candidate-entry.json", candidate_response)
    _write_raw_api(raw_dir / "repository-queue.json", repository_response)
    _graphql_response_warning(
        candidate_response, warnings, label="candidate merge-queue entry lookup"
    )
    _graphql_response_warning(
        repository_response, warnings, label="repository merge-queue topology lookup"
    )
    repository_data = (
        repository_response.get("data")
        if isinstance(repository_response, dict)
        else None
    )
    repository = repository_data.get("repository") if isinstance(repository_data, dict) else None
    raw_queue = repository.get("mergeQueue") if isinstance(repository, dict) else None
    _write_raw_api(raw_dir / "queue.json", raw_queue)
    if isinstance(raw_queue, dict):
        _write_raw_api(raw_dir / "queue-entries.json", raw_queue.get("entries"))
        _write_raw_api(raw_dir / "configuration.json", raw_queue.get("configuration"))
    candidate_queue = normalize_merge_queue_response(candidate_response)
    repository_queue = normalize_merge_queue_response(repository_response)
    entry_reconciliation = reconcile_candidate_queue_entry(
        candidate_queue,
        repository_queue,
        candidate_pr,
        candidate_observed_at=candidate_observed_at,
        repository_observed_at=repository_observed_at,
    )
    effective_entry = entry_reconciliation.get("effective_candidate_queue_entry")
    queue = {
        "queue_entry_status": entry_reconciliation.get("queue_entry_status", "unresolved"),
        "entry": effective_entry,
        **entry_reconciliation,
        "merge_queue": repository_queue.get("merge_queue"),
        "is_in_merge_queue": candidate_queue.get("is_in_merge_queue"),
        "is_merge_queue_enabled": candidate_queue.get("is_merge_queue_enabled"),
        "graphql_available": bool(
            candidate_queue.get("graphql_available")
            or repository_queue.get("graphql_available")
        ),
        "candidate_graphql_status": candidate_queue.get("graphql_status"),
        "candidate_graphql_errors": candidate_queue.get("graphql_errors", []),
        "repository_graphql_status": repository_queue.get("graphql_status"),
        "repository_graphql_errors": repository_queue.get("graphql_errors", []),
    }
    queue["queue_branch"] = queue_branch
    queue["queue_branch_source"] = queue_branch_source
    if isinstance(candidate_pr, int) and queue.get("queue_entry_status") == "unresolved":
        warnings.append(
            {
                "category": "merge_queue_unresolved",
                "message": (
                    "The merge-queue GraphQL response did not provide a usable pull request"
                ),
            }
        )
    merge_queue = queue.get("merge_queue")
    if queue_branch and merge_queue is None:
        warnings.append(
            {
                "category": "merge_queue_snapshot_unavailable",
                "message": f"Repository merge queue for {queue_branch} was unavailable",
            }
        )
    if isinstance(merge_queue, dict) and merge_queue.get("truncated") is True:
        warnings.append(
            {
                "category": "queue_snapshot_truncated",
                "message": (
                    "The bounded merge-queue snapshot reached its page limit; totalCount "
                    "and pageInfo were preserved"
                ),
            }
        )
    timeline_response = api.timeline_graphql(candidate_pr) if isinstance(candidate_pr, int) else None
    _write_raw_api(raw_dir / "timeline.json", timeline_response)
    _write_raw_api(output_dir / "api" / "timeline" / "merge-queue.json", timeline_response)
    _graphql_response_warning(timeline_response, warnings, label="merge queue timeline lookup")
    timeline = normalize_timeline_response(timeline_response)
    if isinstance(candidate_pr, int) and not timeline.get("available"):
        warnings.append(
            {
                "category": "merge_queue_timeline_unavailable",
                "message": "The pull-request timeline response was unavailable or incomplete",
            }
        )
    if isinstance(candidate_pr, int) and timeline.get("truncated") is True:
        warnings.append(
            {
                "category": "timeline_truncated",
                "message": "The bounded pull-request timeline did not include all events",
            }
        )
    merge_fields = (
        pull_request_merge_fields(candidate_response)
        if isinstance(candidate_pr, int)
        else pull_request_merge_fields(None)
    )
    timeline_data = timeline_response.get("data") if isinstance(timeline_response, dict) else None
    timeline_repository = (
        timeline_data.get("repository") if isinstance(timeline_data, dict) else None
    )
    timeline_pull_request = (
        timeline_repository.get("pullRequest")
        if isinstance(timeline_repository, dict)
        else {}
    )
    if isinstance(timeline_pull_request, dict):
        for key, value in (
            ("merged", timeline_pull_request.get("merged")),
            ("merged_at", timeline_pull_request.get("mergedAt")),
            ("merge_commit", _normalize_commit(timeline_pull_request.get("mergeCommit"))),
        ):
            if merge_fields.get(key) is None:
                merge_fields[key] = value
    lifecycle = derive_queue_lifecycle(
        timeline,
        current_entry_status=queue.get("queue_entry_status", "unresolved"),
        merged_at=merge_fields.get("merged_at"),
        merge_commit=merge_fields.get("merge_commit"),
    )
    return {
        **queue,
        "timeline": timeline,
        "lifecycle": lifecycle,
        "merge_fields": merge_fields,
    }


def _git_snapshot(
    repo: Path,
    event: dict[str, Any],
    environment: dict[str, str | None],
    output_dir: Path,
    warnings: list[dict[str, str]],
) -> dict[str, Any]:
    """Capture candidate Git evidence using bounded object fetches only.

    The original ``merge_group.base_ref`` is retained in the event evidence.
    Diagnostic queue-ref enumeration needs a branch name, so it strips only a
    ``refs/heads/`` prefix and skips unsupported namespaces rather than
    constructing a queue pattern from a tag or assuming a default branch.
    """
    git_dir = output_dir / "git"
    git_dir.mkdir(parents=True, exist_ok=True)
    group = event.get("merge_group") if isinstance(event.get("merge_group"), dict) else {}
    head_sha = group.get("head_sha") or environment.get("GITHUB_SHA")
    base_sha = group.get("base_sha")
    head_ref = group.get("head_ref")
    queue_ref = environment.get("GITHUB_REF")

    # Fetch Git objects only, with a fixed history bound. Never unshallow the
    # trusted depth-1 checkout: unavailable ancestry remains explicitly partial.
    fetch_messages: list[str] = [f"maximum_history_depth: {GIT_FETCH_DEPTH}"]

    def commit_available(ref: str) -> bool:
        result = subprocess.run(
            ["git", "-C", str(repo), "cat-file", "-e", f"{ref}^{{commit}}"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return result.returncode == 0

    def fetch_bounded(label: str, ref: str) -> bool:
        result = subprocess.run(
            ["git", "-C", str(repo), *bounded_git_fetch_args(ref)],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        fetch_messages.append(f"{label} {ref}: exit {result.returncode}")
        if result.stdout.strip():
            fetch_messages.append(result.stdout.strip())
        if result.stderr.strip():
            fetch_messages.append(result.stderr.strip())
        return result.returncode == 0

    valid_head_sha = isinstance(head_sha, str) and bool(
        re.fullmatch(r"[0-9a-fA-F]{40,64}", head_sha)
    )
    valid_base_sha = isinstance(base_sha, str) and bool(
        re.fullmatch(r"[0-9a-fA-F]{40,64}", base_sha)
    )
    safe_queue_ref = (
        isinstance(queue_ref, str)
        and queue_ref.startswith("refs/heads/gh-readonly-queue/")
        and "\n" not in queue_ref
        and "\r" not in queue_ref
    )

    candidate_fetch_succeeded = False
    if valid_head_sha:
        candidate_fetch_succeeded = fetch_bounded("candidate SHA", head_sha)
    candidate_available = bool(valid_head_sha and commit_available(head_sha))
    if safe_queue_ref and (not candidate_fetch_succeeded or not candidate_available):
        candidate_fetch_succeeded = (
            fetch_bounded("candidate queue ref", queue_ref)
            or candidate_fetch_succeeded
        )
        candidate_available = bool(valid_head_sha and commit_available(head_sha))
    if not candidate_available:
        warnings.append(
            {
                "category": "candidate_git_unavailable",
                "message": (
                    "Could not fetch the merge-group candidate within the bounded "
                    "history depth"
                ),
            }
        )

    base_fetch_succeeded = False
    if valid_base_sha:
        base_fetch_succeeded = fetch_bounded("base SHA", base_sha)
    base_available = bool(valid_base_sha and commit_available(base_sha))
    if valid_base_sha and not base_available:
        warnings.append(
            {
                "category": "base_git_unavailable",
                "message": "Could not fetch merge_group.base_sha within the bounded history depth",
            }
        )
    (git_dir / "fetch.txt").write_text(
        "\n".join(fetch_messages) + "\n", encoding="utf-8"
    )

    if not isinstance(head_sha, str) or not head_sha:
        warnings.append(
            {"category": "candidate_sha_missing", "message": "No merge-group head SHA was supplied"}
        )
        return {"head_sha": None, "base_sha": base_sha, "available": False}

    resolved_head = _git(
        repo,
        ["rev-parse", "--verify", f"{head_sha}^{{commit}}"],
        warnings,
        label="resolve candidate HEAD",
    )
    if resolved_head is None:
        return {"head_sha": head_sha, "base_sha": base_sha, "available": False}
    resolved_head = resolved_head.strip()

    head_line = _git(repo, ["rev-parse", resolved_head], warnings, label="read candidate HEAD")
    tree_line = _git(
        repo,
        ["show", "-s", "--format=%T", resolved_head],
        warnings,
        label="read candidate tree",
    )
    parents_line = _git(
        repo,
        ["show", "-s", "--format=%P", resolved_head],
        warnings,
        label="read candidate parents",
    )
    commit_text = _git(
        repo,
        ["show", "-s", "--format=fuller", resolved_head],
        warnings,
        label="read candidate commit metadata",
    )
    if head_line is not None:
        (git_dir / "head.txt").write_text(head_line, encoding="utf-8")
    if tree_line is not None:
        (git_dir / "tree.txt").write_text(tree_line, encoding="utf-8")
    if parents_line is not None:
        (git_dir / "parents.txt").write_text(parents_line, encoding="utf-8")
    if commit_text is not None:
        (git_dir / "commit.txt").write_text(commit_text, encoding="utf-8")

    parent_shas = parents_line.strip().split() if parents_line else []
    for parent_sha in parent_shas:
        if not commit_available(parent_sha):
            fetch_bounded("candidate parent SHA", parent_sha)
        if not commit_available(parent_sha):
            warnings.append(
                {
                    "category": "candidate_parent_unavailable",
                    "message": (
                        f"Could not fetch candidate parent {parent_sha} within the "
                        "bounded history depth"
                    ),
                }
            )

    shallow_text = _git(
        repo,
        ["rev-parse", "--is-shallow-repository"],
        warnings,
        label="inspect bounded Git history",
    )
    shallow_value = shallow_text.strip().casefold() if shallow_text else None
    history_shallow = (
        True if shallow_value == "true" else False if shallow_value == "false" else None
    )
    if history_shallow is not False:
        warnings.append(
            {
                "category": "git_history_incomplete",
                "message": (
                    "Git history remains shallow after bounded fetches; ancestry checks "
                    "that cannot be proven from available objects are unresolved"
                ),
            }
        )
    (git_dir / "ancestry.txt").write_text(
        f"history_shallow: {history_shallow}\nfetch_depth: {GIT_FETCH_DEPTH}\n",
        encoding="utf-8",
    )
    (git_dir / "fetch.txt").write_text(
        "\n".join(fetch_messages) + "\n", encoding="utf-8"
    )

    merge_base = None
    commit_shas: list[str] = []
    if isinstance(base_sha, str) and base_sha:
        merge_base_text = _git(
            repo,
            ["merge-base", base_sha, resolved_head],
            warnings,
            label="compute merge base",
        )
        if merge_base_text:
            merge_base = merge_base_text.strip()
        commit_text_list = _git(
            repo,
            ["rev-list", "--topo-order", "--reverse", f"{base_sha}..{resolved_head}"],
            warnings,
            label="list candidate commits",
        )
        if commit_text_list is not None:
            commit_shas = [line for line in commit_text_list.splitlines() if line]
        changes = _git(
            repo,
            ["diff", "--name-status", base_sha, resolved_head],
            warnings,
            label="list changed files",
        )
        if changes is not None:
            (git_dir / "changes.txt").write_text(changes, encoding="utf-8")
    else:
        warnings.append(
            {"category": "base_sha_missing", "message": "merge_group.base_sha was unavailable"}
        )

    graph = _git(
        repo,
        ["log", "--graph", "--decorate", "--oneline", "--boundary", "-n", "80", resolved_head],
        warnings,
        label="capture candidate graph",
    )
    if graph is not None:
        (git_dir / "graph.txt").write_text(graph, encoding="utf-8")
    (git_dir / "commits.txt").write_text("\n".join(commit_shas) + "\n", encoding="utf-8")

    refs = _git(repo, ["show-ref", "--head"], warnings, label="list local refs")
    refs_output = ["Local refs:", refs or "(unavailable)", "", "Visible queue refs:"]
    branch = normalize_branch_ref(group.get("base_ref"))
    if branch:
        queue_pattern = f"refs/heads/gh-readonly-queue/{branch}/*"
        remote_refs = _git(
            repo,
            ["ls-remote", "--heads", "origin", queue_pattern],
            warnings,
            label="list visible merge-queue refs",
        )
        refs_output.append(remote_refs or "(none or unavailable)")
    else:
        refs_output.append(
            f"(skipped: merge_group.base_ref is not a supported branch ref: {group.get('base_ref')!r})"
        )
        warnings.append(
            {
                "category": "queue_refs_unresolved",
                "message": "Skipped queue-ref enumeration because merge_group.base_ref was unavailable",
            }
        )
    (git_dir / "queue-refs.txt").write_text("\n".join(refs_output) + "\n", encoding="utf-8")

    return {
        "head_sha": resolved_head,
        "tree_sha": tree_line.strip() if tree_line else None,
        "parent_shas": parent_shas,
        "base_sha": base_sha,
        "merge_base": merge_base,
        "commit_shas": commit_shas,
        "commit_count": len(commit_shas),
        "available": True,
        "fetch_succeeded": candidate_fetch_succeeded,
        "candidate_available": candidate_available,
        "base_fetch_succeeded": base_fetch_succeeded,
        "base_available": base_available,
        "history_shallow": history_shallow,
        "fetch_depth": GIT_FETCH_DEPTH,
        "queue_ref_transport": queue_ref,
        "merge_group_head_ref": head_ref,
    }


def _snapshot(
    event_path: Path, repo: Path, output_dir: Path, step_summary: Path | None
) -> dict[str, Any]:
    """Run the lightweight merge_group candidate snapshot path."""
    warnings: list[dict[str, str]] = []
    event = copy_event(event_path, output_dir)
    environment = safe_environment()
    save_json(output_dir / "environment.json", environment)
    group = event.get("merge_group") if isinstance(event.get("merge_group"), dict) else {}
    save_json(
        output_dir / "merge-group.json",
        {
            "action": event.get("action"),
            "merge_group": group,
        },
    )

    git_info = _git_snapshot(repo, event, environment, output_dir, warnings)
    api = GitHubAPI(warnings)
    head_sha = git_info.get("head_sha") or group.get("head_sha") or environment.get("GITHUB_SHA")
    parent_shas = git_info.get("parent_shas", [])
    api_dir = output_dir / "api"
    assoc_dir = api_dir / "associated-pulls"
    association_by_sha: dict[str, Any] = {}

    if isinstance(head_sha, str) and head_sha:
        head_associations = api.associated_pulls(head_sha)
        association_by_sha[head_sha] = head_associations
        _write_raw_api(assoc_dir / "head.json", head_associations)
        head_commit = api.commit(head_sha)
        _write_raw_api(api_dir / "commits" / "head.json", head_commit)
    else:
        head_associations = None
        head_commit = None

    parent_associations: dict[str, Any] = {}
    for index, parent_sha in enumerate(parent_shas):
        associations = api.associated_pulls(parent_sha)
        association_by_sha[parent_sha] = associations
        parent_associations[parent_sha] = associations
        _write_raw_api(assoc_dir / "parents" / f"{index}-{parent_sha}.json", associations)
        commit = api.commit(parent_sha)
        _write_raw_api(api_dir / "commits" / "parents" / f"{index}-{parent_sha}.json", commit)

    commit_shas = git_info.get("commit_shas", [])
    association_limit_reached = len(commit_shas) > MAX_COMMIT_ASSOCIATIONS
    for sha in commit_shas[:MAX_COMMIT_ASSOCIATIONS]:
        if sha in association_by_sha:
            associations = association_by_sha[sha]
        else:
            associations = api.associated_pulls(sha)
            association_by_sha[sha] = associations
        _write_raw_api(assoc_dir / "introduced-commits" / f"{sha}.json", associations)
    if association_limit_reached:
        warnings.append(
            {
                "category": "commit_association_limit",
                "message": (
                    f"Commit associations collected for the first {MAX_COMMIT_ASSOCIATIONS} "
                    f"of {len(commit_shas)} commits"
                ),
            }
        )

    candidate_numbers: set[int] = set()
    group_ref_number = parse_queue_pr_number(group.get("head_ref"))
    environment_ref_number = parse_queue_pr_number(environment.get("GITHUB_REF"))
    if group_ref_number is not None:
        candidate_numbers.add(group_ref_number)
    if environment_ref_number is not None:
        candidate_numbers.add(environment_ref_number)
    candidate_numbers.update(_pull_numbers(head_associations))
    commit_message = None
    if isinstance(head_commit, dict):
        commit_message = (
            head_commit.get("commit", {}).get("message")
            if isinstance(head_commit.get("commit"), dict)
            else None
        )
    if commit_message is None:
        commit_message = _git(
            repo,
            ["show", "-s", "--format=%B", str(head_sha)],
            warnings,
            label="read candidate commit message",
        )
    candidate_numbers.update(message_pr_numbers(commit_message))

    composition_numbers: set[int] = set()
    composition_evidence: dict[int, dict[str, Any]] = {}
    for sha, associations in association_by_sha.items():
        numbers = _pull_numbers(associations)
        is_parent = sha in parent_associations
        for number in numbers:
            composition_numbers.add(number)
            entry = composition_evidence.setdefault(
                number,
                {"commit_shas": [], "in_parent": False, "in_candidate_history": False},
            )
            if sha in commit_shas or sha == head_sha:
                entry["commit_shas"].append(sha)
                entry["in_candidate_history"] = True
            if is_parent:
                entry["in_parent"] = True

    all_numbers = candidate_numbers | composition_numbers
    rest_metadata = _pr_metadata(api, all_numbers, output_dir, warnings)
    stacks = _stack_records(all_numbers, rest_metadata, output_dir)
    identity = derive_candidate_identity(
        ref_signals=[
            ("GITHUB_REF_queue_ref", environment.get("GITHUB_REF")),
            ("merge_group.head_ref", group.get("head_ref")),
        ],
        head_associated_pulls=head_associations,
        commit_message=commit_message,
        pull_requests=rest_metadata,
    )
    identity["pr_metadata"] = {
        str(number): _metadata_summary(rest_metadata.get(number), stacks[number])
        for number in sorted(all_numbers)
    }
    parent_numbers = sorted(
        {
            number
            for associations in parent_associations.values()
            for number in _pull_numbers(associations)
        }
    )
    introduced_associations = {
        sha: _pull_numbers(association_by_sha.get(sha))
        for sha in commit_shas[:MAX_COMMIT_ASSOCIATIONS]
    }
    identity["signals"].extend(
        [
            {
                "source": "merge_group.head_sha",
                "raw_value": group.get("head_sha"),
                "candidate_prs": [],
                "interpretation": "candidate commit identity; does not itself name the owning PR",
            },
            {
                "source": "GITHUB_SHA",
                "raw_value": environment.get("GITHUB_SHA"),
                "candidate_prs": [],
                "interpretation": "workflow commit identity; compare with merge_group.head_sha",
            },
            {
                "source": "parent_commit_associations",
                "raw_value": parent_associations,
                "candidate_prs": parent_numbers,
                "interpretation": (
                    "PR associations on candidate parents; evidence of changes already "
                    "present before this candidate"
                ),
            },
            {
                "source": "introduced_commit_associations",
                "raw_value": introduced_associations,
                "candidate_prs": sorted(composition_numbers),
                "interpretation": "PR associations on commits in merge_group.base_sha..candidate",
            },
            {
                "source": "git_ancestry",
                "raw_value": {
                    "base_sha": git_info.get("base_sha"),
                    "merge_base": git_info.get("merge_base"),
                    "commit_shas": commit_shas,
                },
                "candidate_prs": sorted(composition_numbers),
                "interpretation": (
                    "Git commit ancestry identifies candidate contents, not the PR that "
                    "owns the candidate"
                ),
            },
            {
                "source": "pull_request_stack_metadata",
                "raw_value": identity["pr_metadata"],
                "candidate_prs": sorted(
                    number
                    for number, stack in stacks.items()
                    if stack.get("stack_status") == "stack_member"
                ),
                "interpretation": "Native stack membership and position from REST/GraphQL metadata",
            },
        ]
    )
    ancestry_cache: dict[tuple[str, str], bool | None] = {}

    def is_ancestor(ancestor: Any, descendant: Any) -> bool | None:
        if not isinstance(ancestor, str) or not isinstance(descendant, str):
            return None
        key = (ancestor, descendant)
        if key not in ancestry_cache:
            ancestry_cache[key] = _git_is_ancestor(
                repo,
                ancestor,
                descendant,
                warnings,
                history_shallow=git_info.get("history_shallow"),
            )
        return ancestry_cache[key]

    for number in sorted(composition_numbers):
        pr = rest_metadata.get(number)
        head = pr.get("head") if isinstance(pr, dict) else None
        pr_head_sha = head.get("sha") if isinstance(head, dict) else None
        in_candidate = is_ancestor(pr_head_sha, git_info.get("head_sha"))
        in_base = is_ancestor(pr_head_sha, git_info.get("base_sha"))
        parent_presence = [
            is_ancestor(pr_head_sha, parent_sha) for parent_sha in parent_shas
        ]
        if any(value is True for value in parent_presence):
            in_parent = True
        elif parent_presence and all(value is False for value in parent_presence):
            in_parent = False
        else:
            in_parent = None
        composition_evidence[number].update(
            {
                "pr_head_sha": pr_head_sha,
                "pr_head_is_ancestor_of_candidate": in_candidate,
                "pr_head_is_ancestor_of_base": in_base,
                "pr_head_is_ancestor_of_candidate_parent": in_parent,
                "pr_head_introduced_by_candidate": (
                    in_candidate and not in_base
                    if in_candidate is not None and in_base is not None
                    else None
                ),
            }
        )
    identity["signals"].append(
        {
            "source": "associated_pr_head_ancestry",
            "raw_value": {
                str(number): composition_evidence[number]
                for number in sorted(composition_numbers)
            },
            "candidate_prs": sorted(composition_numbers),
            "interpretation": (
                "Git ancestry checks show whether each associated PR head is present "
                "in the candidate, its base, or a candidate parent"
            ),
        }
    )
    selected_stack = (
        stacks.get(identity["candidate_pr"])
        if isinstance(identity.get("candidate_pr"), int)
        else None
    )
    if selected_stack is None:
        stack_summary = {
            "stack_status": "unresolved_candidate",
            "stack_id": None,
            "stack_number": None,
            "stack_position": None,
            "stack_size": None,
            "stack_base_ref": None,
            "stack_base_sha": None,
            "is_stack_member": None,
            "is_stack_head": None,
            "evidence": ["candidate PR did not resolve to one PR"],
        }
    else:
        stack_summary = selected_stack
    identity["stack"] = stack_summary

    candidate_metadata = rest_metadata.get(identity.get("candidate_pr")) if isinstance(identity.get("candidate_pr"), int) else None
    queue_branch_info = resolve_queue_branch(
        merge_group_base_ref=group.get("base_ref"),
        stack=stack_summary,
        pull_request=_metadata_summary(candidate_metadata, selected_stack or {})
        if isinstance(candidate_metadata, dict)
        else None,
    )

    queue_observation = _collect_queue_observation(
        api,
        identity.get("candidate_pr"),
        queue_branch_info["queue_branch"],
        queue_branch_info["queue_branch_source"] or "unresolved",
        output_dir,
        warnings,
    )
    checks_dir = api_dir / "checks"
    check_runs, commit_statuses, _, _ = _api_check_pages(
        api, str(head_sha or ""), checks_dir, warnings
    ) if head_sha else ([], [], [], [])
    check_metrics = derive_check_metrics(check_runs, commit_statuses)

    actions_runs, raw_run_pages = _api_run_pages(
        api, str(head_sha or ""), api_dir / "actions-runs-pages", warnings
    ) if head_sha else ([], [])
    save_json(
        api_dir / "actions-runs.json",
        {"pages": len(raw_run_pages), "workflow_runs": actions_runs},
    )
    siblings = [
        run
        for run in actions_runs
        if run.get("event") == "merge_group"
        and run.get("head_sha") == head_sha
        and run.get("name") != TELEMETRY_WORKFLOW_NAME
    ]
    latest_by_name = _latest_runs_by_name(siblings)
    complete = all(
        isinstance(latest_by_name.get(name), dict)
        and latest_by_name[name].get("status") == "completed"
        for name in TARGET_WORKFLOWS
    )

    composition = []
    for number in sorted(composition_numbers):
        composition.append(
            {
                "pull_request": _metadata_summary(rest_metadata.get(number), stacks[number]),
                "evidence": composition_evidence[number],
            }
        )
    queue_timing = derive_queue_timing(
        utc_now(),
        queue_observation.get("entry"),
        siblings,
        candidate_sha=str(head_sha) if isinstance(head_sha, str) else None,
        check_metrics=check_metrics,
        lifecycle=queue_observation.get("lifecycle"),
    )
    summary = {
        "mode": "merge_group_snapshot",
        "collected_at": utc_now(),
        "event_action": event.get("action"),
        "merge_group": {
            "head_ref": group.get("head_ref"),
            "head_sha": group.get("head_sha"),
            "base_ref": group.get("base_ref"),
            "base_sha": group.get("base_sha"),
            "all_fields": group,
        },
        "git": git_info,
        "identity": identity,
        "candidate_composition": {
            "pull_requests": composition,
            "association_commit_count": min(len(commit_shas), MAX_COMMIT_ASSOCIATIONS),
            "total_candidate_commit_count": len(commit_shas),
            "association_limit_reached": association_limit_reached,
        },
        "queue": queue_observation,
        "queue_timing": queue_timing,
        "checks": {
            "check_runs": check_runs,
            "commit_statuses": commit_statuses,
            "metrics": check_metrics,
        },
        "sibling_workflows": siblings,
        "timing_status": "complete" if complete else "partial",
        "known_target_workflows": list(TARGET_WORKFLOWS),
        "warnings": warnings,
    }
    _write_api_errors(api, output_dir)
    save_json(output_dir / "summary.json", summary)
    save_json(output_dir / "warnings.json", warnings)
    _write_snapshot_summary(summary, step_summary)
    return summary


def _latest_runs_by_name(runs: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for run in runs:
        name = run.get("name") or run.get("workflow_name")
        if isinstance(name, str):
            grouped.setdefault(name, []).append(run)
    latest: dict[str, dict[str, Any]] = {}
    for name, candidates in grouped.items():
        latest[name] = max(
            candidates,
            key=lambda item: (
                item.get("created_at") or "",
                int(item.get("run_number") or 0),
                int(item.get("run_attempt") or 0),
                int(item.get("id") or 0),
            ),
        )
    return latest


def _safe_markdown(value: Any) -> str:
    text = "—" if value is None or value == "" else str(value)
    return (
        text.replace("\\", "\\\\")
        .replace("|", "\\|")
        .replace("\r", " ")
        .replace("\n", " ")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )

def _format_duration(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "—"
    return f"{value:.1f}s"


def _warning_text(warnings: list[dict[str, str]]) -> str:
    if not warnings:
        return "None"
    return "; ".join(
        _safe_markdown(warning.get("message"))
        for warning in warnings[:5]
    )


def _write_snapshot_summary(summary: dict[str, Any], destination: Path | None) -> None:
    if destination is None:
        return
    group = summary["merge_group"]
    git_info = summary["git"]
    identity = summary["identity"]
    stack = identity["stack"]
    candidate = identity.get("candidate_pr")
    candidate_display = f"#{candidate}" if candidate is not None else "unresolved"
    evidence = ", ".join(identity.get("candidate_pr_evidence", [])) or identity.get(
        "candidate_pr_resolution", "unresolved"
    )
    if stack.get("stack_status") == "not_a_stack":
        stack_display = "none"
    elif stack.get("stack_status") == "stack_member":
        role = "head" if stack.get("is_stack_head") is True else "member"
        stack_display = (
            f"#{stack.get('stack_number')} (stack id {stack.get('stack_id')}, {role})"
        )
    else:
        stack_display = "unresolved"
    if stack.get("stack_position") is not None and stack.get("stack_size") is not None:
        position = f"{stack['stack_position']} / {stack['stack_size']}"
    else:
        position = "—"
    pr_summaries = identity.get("pr_metadata", {})
    associated_display = ", ".join(f"#{number}" for number in sorted(
        int(value) for value in pr_summaries
    )) or "none observed"
    parents = ", ".join(git_info.get("parent_shas", [])) or "unavailable"
    queue = summary.get("queue", {})
    entry = queue.get("entry") or {}
    merge_queue = queue.get("merge_queue") or {}
    queue_config = merge_queue.get("configuration") or {}
    queue_timing = summary.get("queue_timing", {})
    enqueued_at = entry.get("enqueued_at") or queue.get("lifecycle", {}).get(
        "latest_queue_admission_at"
    )

    lines = [
        "## Merge queue telemetry",
        "",
        f"- **Merge-group SHA:** `{_safe_markdown(group.get('head_sha'))}`",
        f"- **Queue ref:** `{_safe_markdown(group.get('head_ref'))}`",
        f"- **Base:** `{_safe_markdown(group.get('base_ref'))}` / "
        f"`{_safe_markdown(group.get('base_sha'))}`",
        f"- **Candidate PR:** {candidate_display}",
        f"- **Candidate evidence:** {_safe_markdown(evidence)}",
        f"- **Stack:** {_safe_markdown(stack_display)}",
        f"- **Position:** {_safe_markdown(position)}",
        f"- **Associated PRs observed:** {_safe_markdown(associated_display)}",
        f"- **Git parents:** `{_safe_markdown(parents)}`",
        f"- **Tree SHA:** `{_safe_markdown(git_info.get('tree_sha'))}`",
        "",
        "### Queue state",
        "",
        f"- **Entry status:** {_safe_markdown(queue.get('queue_entry_status'))}",
        f"- **Effective candidate entry source:** "
        f"{_safe_markdown(queue.get('effective_candidate_queue_entry_source'))}",
        f"- **Candidate entry identity conflict:** "
        f"{_safe_markdown(queue.get('candidate_entry_identity_conflict'))}",
        f"- **Candidate entry observation drift:** "
        f"{_safe_markdown(queue.get('candidate_entry_observation_drift_fields'))}",
        f"- **Entry observations collected:** "
        f"{_safe_markdown(queue.get('candidate_entry_from_pull_request_observed_at'))} / "
        f"{_safe_markdown(queue.get('candidate_entry_from_repository_queue_observed_at'))}",
        f"- **Enqueued at:** {_safe_markdown(enqueued_at)}",
        f"- **Queue position / depth:** "
        f"{_safe_markdown(entry.get('queue_entry_position'))} / "
        f"{_safe_markdown(merge_queue.get('total_count'))}",
        f"- **Entry state:** {_safe_markdown(entry.get('state'))}",
        f"- **Estimated time to merge:** "
        f"{_format_duration(entry.get('estimated_time_to_merge_seconds'))}",
        f"- **Queue age:** {_format_duration(queue_timing.get('queue_age_at_snapshot_seconds'))}",
        f"- **Queue → first target workflow:** "
        f"{_format_duration(queue_timing.get('queue_to_first_target_workflow_created_seconds'))}",
        "",
        "### Queue configuration",
        "",
        f"- **Strategy:** {_safe_markdown(queue_config.get('merging_strategy'))}",
        f"- **Build concurrency:** {_safe_markdown(queue_config.get('maximum_entries_to_build'))}",
        f"- **Maximum entries to merge:** "
        f"{_safe_markdown(queue_config.get('maximum_entries_to_merge'))}",
        f"- **Minimum entries / wait:** "
        f"{_safe_markdown(queue_config.get('minimum_entries_to_merge'))} / "
        f"{_safe_markdown(queue_config.get('minimum_entries_to_merge_wait_time_minutes'))} min",
        f"- **Check timeout:** "
        f"{_safe_markdown(queue_config.get('check_response_timeout_minutes'))} min",
        f"- **Merge method:** {_safe_markdown(queue_config.get('merge_method'))}",
        f"- **Queue branch:** {_safe_markdown(queue.get('queue_branch'))} "
        f"({_safe_markdown(queue.get('queue_branch_source'))})",
        "",
        "### Sibling workflows currently observed",
        "",
        "| Workflow | Run id | Status | Conclusion |",
        "| --- | ---: | --- | --- |",
    ]
    for run in summary.get("sibling_workflows", []):
        lines.append(
            "| "
            + " | ".join(
                _safe_markdown(run.get(key))
                for key in ("name", "id", "status", "conclusion")
            )
            + " |"
        )
    if not summary.get("sibling_workflows"):
        lines.append("| none observed yet | — | — | — |")
    lines.extend(
        [
            "",
            f"- **Telemetry status:** {summary.get('timing_status')}",
            f"- **Warnings:** {_warning_text(summary.get('warnings', []))}",
            "",
        ]
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text("\n".join(lines), encoding="utf-8")


def _write_finalization_summary(summary: dict[str, Any], destination: Path | None) -> None:
    if destination is None:
        return
    final_merge = summary.get("final_merge", {})
    lifecycle = summary.get("queue", {}).get("lifecycle", {})
    queue_timing = summary.get("queue_timing", {})
    lines = [
        "## Merge queue finalization telemetry",
        f"- **Status:** {_safe_markdown(summary.get('status', 'collected'))}",
        "",
        f"- **PR:** #{_safe_markdown(summary.get('pull_request_number'))}",
        f"- **Merged at:** {_safe_markdown(final_merge.get('merged_at'))}",
        f"- **Merge commit:** `{_safe_markdown(final_merge.get('merge_commit'))}`",
        f"- **Latest queue admission:** "
        f"{_safe_markdown(lifecycle.get('latest_queue_admission_at'))}",
        f"- **Queue → merge (authoritative):** {_format_duration(queue_timing.get('queue_to_merge_seconds'))}",
        f"- **Queue → merge (inferred):** {_format_duration(queue_timing.get('queue_to_merge_inferred_seconds'))}",
        f"- **Queue-to-merge pairing:** "
        f"{_safe_markdown(lifecycle.get('queue_to_merge_pairing_basis'))} "
        f"({_safe_markdown(lifecycle.get('queue_to_merge_pairing_confidence'))})",
        f"- **Queue → latest observed merge-commit check completion:** "
        f"{_format_duration((summary.get('merge_commit_checks', {}).get('timing') or {}).get('queue_to_latest_observed_merge_commit_check_complete_seconds'))}",
        f"- **Latest observed merge-commit check completed at:** "
        f"{_safe_markdown((summary.get('merge_commit_checks', {}).get('timing') or {}).get('latest_observed_merge_commit_check_completed_at'))}",
        f"- **Merge-commit check snapshot:** "
        f"{(summary.get('merge_commit_checks', {}).get('metrics') or {}).get('observed_check_run_count', 0)} check runs observed; "
        f"{(summary.get('merge_commit_checks', {}).get('metrics') or {}).get('completed_check_run_count', 0)} completed; "
        f"{(summary.get('merge_commit_checks', {}).get('metrics') or {}).get('pending_check_run_count', 0)} pending",
        f"- **Admissions / removals / requeues:** {lifecycle.get('queue_admission_count', '—')} / "
        f"{lifecycle.get('queue_removal_count', '—')} / {lifecycle.get('requeue_count', '—')}",
        f"- **Lifecycle:** {_safe_markdown(lifecycle.get('lifecycle_status'))}",
        f"- **Queue trunk:** {_safe_markdown((summary.get('queue') or {}).get('queue_branch'))} "
        f"({_safe_markdown((summary.get('queue') or {}).get('queue_branch_source'))})",
        f"- **Skip reason:** {_safe_markdown(summary.get('skip_reason'))}",
        "- **Merge-commit checks are a point-in-time post-merge snapshot; "
        "they do not prove that all future checks have completed.",
        f"- **Warnings:** {_warning_text(summary.get('warnings', []))}",
        "",
    ]
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text("\n".join(lines), encoding="utf-8")


def _finalize(
    event_path: Path, output_dir: Path, step_summary: Path | None
) -> dict[str, Any]:
    """Record a merged PR and close a master queue cycle when evidence permits.

    Finalization receives every merged PR closure, including stack members with
    non-master direct bases. Webhook stack targets, native stack metadata, and
    direct PR bases are resolved in that order; non-master or unresolved trunks
    produce a successful skipped observation. Merge-commit checks are collected
    once as a point-in-time snapshot and are never treated as candidate checks.
    """
    warnings: list[dict[str, str]] = []
    event = copy_event(event_path, output_dir)
    environment = safe_environment()
    save_json(output_dir / "environment.json", environment)
    pull_request_event = event.get("pull_request")
    if not isinstance(pull_request_event, dict):
        raise ValueError("pull_request event payload did not include pull_request")
    if pull_request_event.get("merged") is not True:
        return {
            "mode": "pull_request_finalization",
            "status": "skipped_unmerged",
            "warnings": [],
        }
    number = pull_request_event.get("number")
    if not isinstance(number, int):
        raise ValueError("pull_request event is missing its number")

    output_api = output_dir / "api"
    api = GitHubAPI(warnings)
    rest_pr = api.pull_request(number)
    _write_raw_api(output_api / "pr.json", rest_pr)
    stack_response = api.stack_graphql(number)
    _write_raw_api(output_api / "pr-graphql.json", stack_response)
    _graphql_response_warning(stack_response, warnings, label="finalization stack lookup")
    stack = classify_stack(stack_response, rest_pr if isinstance(rest_pr, dict) else {})
    queue_branch_info = resolve_queue_branch(
        webhook_stack_base_ref=webhook_stack_target(pull_request_event),
        webhook_base_ref=(
            pull_request_event.get("base", {}).get("ref")
            if isinstance(pull_request_event.get("base"), dict)
            else None
        ),
        stack=stack,
        pull_request=_metadata_summary(rest_pr, stack) if isinstance(rest_pr, dict) else None,
    )
    trunk_decision = finalization_trunk_decision(queue_branch_info.get("queue_branch"))
    if trunk_decision != "master":
        skipped_reason = (
            "resolved queue trunk is not master"
            if trunk_decision == "non_master"
            else "queue trunk could not be resolved; master classification is unsafe"
        )
        summary = {
            "mode": "pull_request_finalization",
            "status": f"skipped_{trunk_decision}_trunk",
            "collected_at": utc_now(),
            "pull_request_number": number,
            "pull_request": rest_pr,
            "identity": {
                "candidate_pr": number,
                "candidate_pr_confidence": "high",
                "stack": stack,
            },
            "queue": {
                "queue_branch": queue_branch_info.get("queue_branch"),
                "queue_branch_source": queue_branch_info.get("queue_branch_source"),
            },
            "final_merge": {
                "merged": True,
                "merged_at": pull_request_event.get("merged_at"),
                "merge_commit": pull_request_event.get("merge_commit_sha"),
            },
            "skip_reason": skipped_reason,
            "warnings": warnings,
        }
        _write_api_errors(api, output_dir)
        save_json(output_dir / "summary.json", summary)
        save_json(output_dir / "warnings.json", warnings)
        _write_finalization_summary(summary, step_summary)
        return summary

    queue_observation = _collect_queue_observation(
        api,
        number,
        queue_branch_info["queue_branch"],
        queue_branch_info["queue_branch_source"] or "unresolved",
        output_dir,
        warnings,
    )
    rest_merged_at = rest_pr.get("merged_at") if isinstance(rest_pr, dict) else None
    rest_merge_sha = rest_pr.get("merge_commit_sha") if isinstance(rest_pr, dict) else None
    merge_fields = queue_observation.get("merge_fields", {})
    if rest_merged_at is not None:
        merge_fields["merged_at"] = rest_merged_at
    if rest_merge_sha is not None:
        merge_fields["merge_commit"] = rest_merge_sha
    merge_fields["merged"] = True
    queue_observation["merge_fields"] = merge_fields
    queue_observation["lifecycle"] = derive_queue_lifecycle(
        queue_observation.get("timeline", {}),
        current_entry_status=queue_observation.get("queue_entry_status", "absent"),
        merged_at=merge_fields.get("merged_at"),
        merge_commit=merge_fields.get("merge_commit"),
    )

    check_runs: list[dict[str, Any]] = []
    commit_statuses: list[dict[str, Any]] = []
    merge_sha = merge_fields.get("merge_commit")
    if isinstance(merge_sha, str) and merge_sha:
        check_runs, commit_statuses, _, _ = _api_check_pages(
            api, merge_sha, output_api / "merge-commit-checks", warnings
        )
    merge_snapshot_collected_at = utc_now()
    merge_commit_check_metrics = derive_check_metrics(
        check_runs,
        commit_statuses,
        snapshot_collected_at=merge_snapshot_collected_at,
    )
    lifecycle = queue_observation.get("lifecycle", {})
    queue_enqueued_at = (
        (queue_observation.get("entry") or {}).get("enqueued_at")
        or lifecycle.get("latest_queue_admission_at")
    )
    queue_timing = derive_queue_timing(
        utc_now(),
        queue_observation.get("entry"),
        [],
        check_metrics=None,
        lifecycle=queue_observation.get("lifecycle"),
    )
    merge_commit_check_timing = derive_merge_commit_check_timing(
        queue_enqueued_at
        if lifecycle.get("queue_to_merge_seconds") is not None
        else None,
        merge_commit_check_metrics,
    )
    summary = {
        "mode": "pull_request_finalization",
        "collected_at": merge_snapshot_collected_at,
        "pull_request_number": number,
        "pull_request": rest_pr,
        "identity": {
            "candidate_pr": number,
            "candidate_pr_confidence": "high",
            "stack": stack,
        },
        "queue": queue_observation,
        "queue_timing": queue_timing,
        "final_merge": {
            "merged": True,
            "merged_at": merge_fields.get("merged_at"),
            "merge_commit": merge_fields.get("merge_commit"),
        },
        "merge_commit_checks": {
            "check_runs": check_runs,
            "commit_statuses": commit_statuses,
            "metrics": merge_commit_check_metrics,
            "timing": merge_commit_check_timing,
        },
        "merge_commit_check_timing": merge_commit_check_timing,
        "warnings": warnings,
    }
    _write_api_errors(api, output_dir)
    save_json(output_dir / "summary.json", summary)
    save_json(output_dir / "warnings.json", warnings)
    _write_finalization_summary(summary, step_summary)
    return summary


def _timing(
    event_path: Path, output_dir: Path, step_summary: Path | None
) -> dict[str, Any]:
    """Collect merge-group sibling runs and every reported attempt through N."""
    warnings: list[dict[str, str]] = []
    event = copy_event(event_path, output_dir)
    environment = safe_environment()
    save_json(output_dir / "environment.json", environment)
    source = event.get("workflow_run")
    if not isinstance(source, dict):
        raise ValueError("workflow_run event payload did not include workflow_run")

    source_event = {
        key: source.get(key)
        for key in (
            "name",
            "workflow_id",
            "id",
            "run_number",
            "run_attempt",
            "event",
            "head_branch",
            "head_sha",
            "status",
            "conclusion",
            "created_at",
            "run_started_at",
            "updated_at",
            "html_url",
            "check_suite_id",
        )
    }
    if source.get("event") != "merge_group":
        raise ValueError("timing collector received a non-merge_group source run")

    output_api = output_dir / "api"
    api = GitHubAPI(warnings)
    run_id = source.get("id")
    attempt = source.get("run_attempt") or 1
    head_sha = source.get("head_sha")
    if not isinstance(run_id, int) or not isinstance(head_sha, str) or not head_sha:
        raise ValueError("workflow_run event is missing its run id or head SHA")

    run_path = f"repos/{urllib.parse.quote(api.repo_name, safe='/')}/actions/runs/{run_id}"
    source_run_api = api.rest(run_path, warning_label=f"source workflow run {run_id}")
    _write_raw_api(output_api / "source-run.json", source_run_api)
    source_attempt = api.workflow_run_attempt(run_id, int(attempt))
    _write_raw_api(output_api / "source-run-attempt.json", source_attempt)
    source_jobs, source_job_pages, source_jobs_complete = _api_job_pages(
        api,
        run_id,
        int(attempt),
        output_api / "jobs" / f"run-{run_id}-attempt-{attempt}",
        warnings,
    )
    save_json(
        output_api / "source-jobs.json",
        {"pages": len(source_job_pages), "jobs": source_jobs},
    )

    raw_run_list, run_pages = _api_run_pages(
        api, head_sha, output_api / "sibling-run-pages", warnings
    )
    sibling_runs = [
        run
        for run in raw_run_list
        if run.get("event") == "merge_group"
        and run.get("head_sha") == head_sha
        and run.get("name") != TELEMETRY_WORKFLOW_NAME
    ]
    # The completion webhook is a fresh observation and may arrive before the
    # workflow-runs listing reflects its run. Preserve and include its payload.
    source_copy = dict(source)
    if not any(
        run.get("id") == run_id and (run.get("run_attempt") or 1) == int(attempt)
        for run in sibling_runs
    ):
        sibling_runs.append(source_copy)
    save_json(
        output_api / "sibling-runs.json",
        {"pages": len(run_pages), "workflow_runs": sibling_runs},
    )
    preloaded_key = (run_id, int(attempt))
    run_metrics, jobs_by_run = _collect_workflow_attempts(
        api,
        sibling_runs,
        source,
        output_api,
        warnings,
        preloaded_details={preloaded_key: source_attempt},
        preloaded_jobs={preloaded_key: source_jobs},
        preloaded_jobs_complete={preloaded_key: source_jobs_complete},
    )
    save_json(output_api / "workflow-attempts.json", run_metrics)
    latest_by_name = _latest_runs_by_name(run_metrics)

    for jobs in jobs_by_run.values():
        for job in jobs:
            if runner_metadata_missing_unexpectedly(job):
                warnings.append(
                    {
                        "category": "runner_metadata_missing",
                        "message": (
                            f"Runner metadata missing for job "
                            f"{job.get('name') or job.get('id')}"
                        ),
                    }
                )

    commit = api.commit(head_sha)
    _write_raw_api(output_api / "commit" / "head.json", commit)
    parent_shas = []
    if isinstance(commit, dict) and isinstance(commit.get("parents"), list):
        parent_shas = [
            parent["sha"]
            for parent in commit["parents"]
            if isinstance(parent, dict) and isinstance(parent.get("sha"), str)
        ]
    associated_head = api.associated_pulls(head_sha)
    _write_raw_api(output_api / "associated-pulls" / "head.json", associated_head)
    parent_associations: dict[str, Any] = {}
    for index, parent_sha in enumerate(parent_shas):
        parent_associations[parent_sha] = api.associated_pulls(parent_sha)
        _write_raw_api(
            output_api / "associated-pulls" / "parents" / f"{index}-{parent_sha}.json",
            parent_associations[parent_sha],
        )
        parent_commit = api.commit(parent_sha)
        _write_raw_api(
            output_api / "commit" / "parents" / f"{index}-{parent_sha}.json",
            parent_commit,
        )

    message = None
    if isinstance(commit, dict) and isinstance(commit.get("commit"), dict):
        message = commit["commit"].get("message")
    ref_signals = [("workflow_run.head_branch", source.get("head_branch"))]
    identity_numbers = set(_pull_numbers(associated_head))
    ref_number = parse_queue_pr_number(source.get("head_branch"))
    if ref_number is not None:
        identity_numbers.add(ref_number)
    for associations in parent_associations.values():
        identity_numbers.update(_pull_numbers(associations))
    identity_numbers.update(message_pr_numbers(message))
    rest_metadata = _pr_metadata(api, identity_numbers, output_dir, warnings)
    stacks = _stack_records(identity_numbers, rest_metadata, output_dir)
    identity = derive_candidate_identity(
        ref_signals=ref_signals,
        head_associated_pulls=associated_head,
        commit_message=message,
        pull_requests=rest_metadata,
    )
    identity["parent_associated_pulls"] = {
        sha: _pull_numbers(associations)
        for sha, associations in parent_associations.items()
    }
    identity["pr_metadata"] = {
        str(number): _metadata_summary(rest_metadata.get(number), stacks[number])
        for number in sorted(identity_numbers)
    }
    selected_stack = (
        stacks.get(identity["candidate_pr"])
        if isinstance(identity.get("candidate_pr"), int)
        else None
    )
    identity["stack"] = selected_stack or {
        "stack_status": "unresolved_candidate",
        "stack_id": None,
        "stack_number": None,
        "stack_position": None,
        "stack_size": None,
        "stack_base_ref": None,
        "stack_base_sha": None,
        "is_stack_member": None,
        "is_stack_head": None,
        "evidence": ["candidate PR did not resolve to one PR"],
    }

    queue_ref_branch = queue_branch_from_queue_ref(source.get("head_branch"))
    candidate_metadata = (
        rest_metadata.get(identity.get("candidate_pr"))
        if isinstance(identity.get("candidate_pr"), int)
        else None
    )
    queue_branch_info = (
        {"queue_branch": queue_ref_branch, "queue_branch_source": "workflow_run.head_branch"}
        if queue_ref_branch
        else resolve_queue_branch(
            stack=identity["stack"],
            pull_request=_metadata_summary(candidate_metadata, selected_stack or {})
            if isinstance(candidate_metadata, dict)
            else None,
        )
    )

    queue_observation = _collect_queue_observation(
        api,
        identity.get("candidate_pr"),
        queue_branch_info["queue_branch"],
        queue_branch_info["queue_branch_source"] or "unresolved",
        output_dir,
        warnings,
    )
    checks_dir = output_dir / "api" / "checks"
    check_runs, commit_statuses, _, _ = _api_check_pages(
        api, head_sha, checks_dir, warnings
    )
    check_metrics = derive_check_metrics(check_runs, commit_statuses)

    run_names = {
        (run.get("id"), int(run.get("run_attempt") or 1)): run.get("name")
        for run in run_metrics
        if isinstance(run.get("id"), int)
    }
    jobs_by_run = {
        key: [dict(job, workflow_name=run_names.get(key)) for job in jobs]
        for key, jobs in jobs_by_run.items()
    }
    timing = derive_timing_metrics(latest_by_name, jobs_by_run, run_metrics)
    queue_timing = derive_queue_timing(
        utc_now(),
        queue_observation.get("entry"),
        run_metrics,
        candidate_sha=head_sha,
        check_metrics=check_metrics,
        lifecycle=queue_observation.get("lifecycle"),
    )
    summary = {
        "mode": "workflow_completion_timing",
        "collected_at": utc_now(),
        "source_workflow_run_event": source_event,
        "candidate_head_sha": head_sha,
        "candidate_queue_ref": source.get("head_branch"),
        "identity": identity,
        "queue": queue_observation,
        "queue_timing": queue_timing,
        "checks": {
            "check_runs": check_runs,
            "commit_statuses": commit_statuses,
            "metrics": check_metrics,
        },
        "observed_sibling_workflows": run_metrics,
        "timing": timing,
        "warnings": warnings,
    }
    _write_api_errors(api, output_dir)
    save_json(output_dir / "summary.json", summary)
    save_json(output_dir / "warnings.json", warnings)
    _write_timing_summary(summary, step_summary)
    return summary


def _write_timing_summary(summary: dict[str, Any], destination: Path | None) -> None:
    if destination is None:
        return
    timing = summary["timing"]
    queue = summary.get("queue", {})
    entry = queue.get("entry") or {}
    queue_timing = summary.get("queue_timing", {})
    check_metrics = summary.get("checks", {}).get("metrics", {})
    stack = summary["identity"]["stack"]
    if stack.get("stack_status") == "not_a_stack":
        stack_label = "none"
    elif stack.get("stack_status") == "stack_member":
        role = "head" if stack.get("is_stack_head") is True else "member"
        stack_label = (
            f"#{stack.get('stack_number')} (stack id {stack.get('stack_id')}, {role}) "
            f"position {stack.get('stack_position')} / {stack.get('stack_size')}"
        )
    else:
        stack_label = "unresolved"
    candidate_pr = summary["identity"].get("candidate_pr") or "unresolved"
    candidate_evidence = ", ".join(
        summary["identity"].get("candidate_pr_evidence", [])
    ) or summary["identity"].get("candidate_pr_resolution")

    lines = [
        "## Merge queue completion telemetry",
        "",
        f"- **Merge-group SHA:** `{_safe_markdown(summary.get('candidate_head_sha'))}`",
        f"- **Queue ref:** `{_safe_markdown(summary.get('candidate_queue_ref'))}`",
        f"- **Candidate PR:** {_safe_markdown(candidate_pr)}",
        f"- **Candidate evidence:** {_safe_markdown(candidate_evidence)}",
        f"- **Stack:** {_safe_markdown(stack_label)}",
        f"- **Timing view:** {timing.get('timing_status')}",
        f"- **Queue entry:** {_safe_markdown(queue.get('queue_entry_status'))}",
        f"- **Effective candidate entry source:** "
        f"{_safe_markdown(queue.get('effective_candidate_queue_entry_source'))}",
        f"- **Queue position:** {_safe_markdown(entry.get('queue_entry_position'))}",
        f"- **Queue age:** {_format_duration(queue_timing.get('queue_age_at_snapshot_seconds'))}",
        f"- **Queue → first target workflow:** "
        f"{_format_duration(queue_timing.get('queue_to_first_target_workflow_created_seconds'))}",
        "",
        "### Workflow attempts observed",
        "",
        "| Workflow | Run id | Attempt | Status | Conclusion |",
        "| --- | ---: | ---: | --- | --- |",
    ]
    for run in summary.get("observed_sibling_workflows", []):
        lines.append(
            "| "
            + " | ".join(
                _safe_markdown(run.get(key))
                for key in ("name", "id", "run_attempt", "status", "conclusion")
            )
            + " |"
        )
    if not summary.get("observed_sibling_workflows"):
        lines.append("| none observed yet | — | — | — | — |")
    lines.extend(
        [
            "",
            "### Workflow timings",
            "",
            "| Workflow | Latest run / attempt | Workflow start delay | Runtime | Conclusion |",
            "| --- | --- | ---: | ---: | --- |",
        ]
    )
    for row in timing.get("workflow_rows", []):
        lines.append(
            "| "
            + " | ".join(
                [
                    _safe_markdown(row.get("workflow_name")),
                    _safe_markdown(
                        f"{row.get('run_id')} / {row.get('run_attempt')}"
                        if row.get("run_id") is not None
                        else None
                    ),
                    _format_duration(row.get("workflow_created_to_run_started_seconds")),
                    _format_duration(row.get("runtime_seconds")),
                    _safe_markdown(row.get("conclusion") or row.get("status")),
                ]
            )
            + " |"
        )
    last_workflow = timing.get("last_completing_workflow") or {}
    last_job = timing.get("last_completing_job") or {}
    lines.extend(
        [
            "",
            "### Jobs",
            "",
            "| Job | Run / attempt | Runner | Runtime |",
            "| --- | --- | --- | ---: |",
        ]
    )
    for row in timing.get("job_rows", []):
        runner = row.get("runner_name") or row.get("runner_classification")
        platform = row.get("runner_platform")
        if platform and platform != "unknown":
            runner = f"{runner} ({platform})"
        lines.append(
            "| "
            + " | ".join(
                [
                    _safe_markdown(
                        f"{row.get('workflow_name') or 'workflow'} / "
                        f"{row.get('job_name') or 'job'}"
                    ),
                    _safe_markdown(
                        f"{row.get('run_id')} / {row.get('run_attempt')}"
                    ),
                    _safe_markdown(runner),
                    _format_duration(row.get("runtime_seconds")),
                ]
            )
            + " |"
        )
    if not timing.get("job_rows"):
        lines.append("| no completed jobs observed | — | — | — |")
    runner_seconds = timing.get("runner_time_seconds", {})
    longest_workflow = timing.get("longest_workflow") or {}
    longest_job = timing.get("longest_job") or {}
    lines.extend(
        [
            "",
            f"- **Candidate wall time:** "
            f"{_format_duration(timing.get('candidate_wall_time_seconds'))}",
            f"- **Runner time:** self-hosted "
            f"{_format_duration(runner_seconds.get('self-hosted'))}; "
            f"GitHub-hosted {_format_duration(runner_seconds.get('github-hosted'))}; "
            f"unknown {_format_duration(runner_seconds.get('unknown'))}",
            f"- **Runner-cost accounting:** "
            f"{_safe_markdown(timing.get('runner_time_status'))}",
            f"- **Longest workflow:** {_safe_markdown(longest_workflow.get('workflow_name'))} "
            f"({_format_duration(longest_workflow.get('runtime_seconds'))})",
            f"- **Longest job:** {_safe_markdown(longest_job.get('job_name'))} "
            f"({_format_duration(longest_job.get('runtime_seconds'))})",
            f"- **Last completing workflow:** "
            f"{_safe_markdown(last_workflow.get('workflow_name'))}",
            f"- **Last completing job:** "
            f"{_safe_markdown(last_job.get('job_name'))}",
            f"- **Last completing observed check:** "
            f"{_safe_markdown((check_metrics.get('last_completing_observed_check') or {}).get('name'))}",
            f"- **Observed check runs:** {_safe_markdown(check_metrics.get('observed_check_run_count'))}",
            f"- **Observed legacy status contexts:** {_safe_markdown(check_metrics.get('observed_status_context_count'))}",
            f"- **Warnings:** {_warning_text(summary.get('warnings', []))}",
            "",
        ]
    )
    missing_runner_cost = timing.get("runner_time_missing_attempts", [])
    if missing_runner_cost:
        missing = ", ".join(
            f"{_safe_markdown(item.get('workflow_name') or 'workflow')} run "
            f"{_safe_markdown(item.get('run_id'))} / attempt "
            f"{_safe_markdown(item.get('run_attempt'))} "
            f"({_safe_markdown(item.get('reason'))})"
            for item in missing_runner_cost[:5]
        )
        lines.insert(-1, f"- **Missing runner-cost evidence:** {missing}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text("\n".join(lines), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    """Dispatch the snapshot, timing, or finalization collector mode."""
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="mode", required=True)
    for mode in ("snapshot", "timing", "finalize"):
        subparser = subparsers.add_parser(mode)
        subparser.add_argument("--event", required=True, type=Path)
        subparser.add_argument("--output", required=True, type=Path)
        if mode == "snapshot":
            subparser.add_argument("--repo", required=True, type=Path)
    args = parser.parse_args(argv)
    summary_path = (
        Path(os.environ["GITHUB_STEP_SUMMARY"])
        if os.environ.get("GITHUB_STEP_SUMMARY")
        else None
    )

    if args.mode == "snapshot":
        result = _snapshot(args.event, args.repo, args.output, summary_path)
    elif args.mode == "timing":
        result = _timing(args.event, args.output, summary_path)
    else:
        result = _finalize(args.event, args.output, summary_path)
    print(
        f"Merge queue telemetry {args.mode} completed: "
        f"{result.get('timing_status', 'snapshot captured')}; "
        f"{len(result.get('warnings', []))} warning(s)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
