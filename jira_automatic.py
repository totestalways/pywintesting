# -*- coding: utf-8 -*-
"""
Create Sub-tasks under multiple parent issues in Jira (Cloud or Server/DC)
using atlassian-python-api, naming each as: "<SUBTASK_PREFIX> <Parent Issue Summary>".

Requires:
    pip install atlassian-python-api
"""

from typing import List, Optional, Dict
from atlassian import Jira
import urllib3

# ==========
# INPUTS — edit these
# ==========

JIRA_BASE_URL = "https://your-domain.atlassian.net"  # or your Server/DC URL
JIRA_USERNAME_OR_EMAIL = "you@example.com"           # Cloud: email; Server/DC: username (or email if supported)
JIRA_API_TOKEN_OR_PASSWORD = "YOUR_API_TOKEN_OR_PASSWORD"

# SSL verification (set False if you must, but prefer a proper CA bundle)
VERIFY_SSL = True  # set to False to skip SSL verification
if not VERIFY_SSL:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

PROJECT_KEY = "ABC"
PARENT_ISSUE_KEYS: List[str] = ["ABC-123", "ABC-456"]

SUBTASK_PREFIX = "[Prep]"               # user-defined prefix
SUBTASK_ISSUE_TYPE_NAME = "Sub-task"    # adjust if customized in your project
SUBTASK_DESCRIPTION = "Auto-created sub-task. Please review and complete."

# Optional fields
LABELS: List[str] = ["auto-created"]
COMPONENT_NAMES: List[str] = []         # must already exist in project, or leave empty

# Assignee:
# For Jira Cloud you must use accountId; for Server/DC use username.
ASSIGNEE_ACCOUNT_ID_CLOUD: Optional[str] = None  # e.g. "557058:abcd..."
ASSIGNEE_NAME_SERVER: Optional[str] = None       # e.g. "jdoe"

# ========== END OF INPUTS ==========


def get_client() -> Jira:
    """
    Create a Jira client. For Cloud, pass your Atlassian email + API token.
    For Server/DC, pass username + password or PAT (if configured).
    """
    return Jira(
        url=JIRA_BASE_URL,
        username=JIRA_USERNAME_OR_EMAIL,
        password=JIRA_API_TOKEN_OR_PASSWORD,
        verify_ssl=VERIFY_SSL,
        # cloud=True  # optional; older versions sometimes require this for Cloud
    )


def parent_summary(jira: Jira, parent_key: str) -> str:
    """
    Fetch the parent's summary using atlassian-python-api.
    Returns the key if the summary can't be read.
    """
    try:
        issue = jira.issue(parent_key)  # dict
        return issue.get("fields", {}).get("summary") or parent_key
    except Exception as e:
        print(f"[WARN] Could not fetch summary for {parent_key}: {e}")
        return parent_key


def build_fields(parent_key: str, parent_sum: str) -> Dict:
    """
    Build the fields payload for the sub-task.
    """
    summary = f"{SUBTASK_PREFIX} {parent_sum}".strip()
    fields: Dict = {
        "project": {"key": PROJECT_KEY},
        "parent": {"key": parent_key},
        "issuetype": {"name": SUBTASK_ISSUE_TYPE_NAME},
        "summary": summary,
        "description": SUBTASK_DESCRIPTION,
    }

    if LABELS:
        fields["labels"] = LABELS
    if COMPONENT_NAMES:
        fields["components"] = [{"name": c} for c in COMPONENT_NAMES]

    # Assignee handling
    if ASSIGNEE_ACCOUNT_ID_CLOUD:
        fields["assignee"] = {"accountId": ASSIGNEE_ACCOUNT_ID_CLOUD}
    elif ASSIGNEE_NAME_SERVER:
        fields["assignee"] = {"name": ASSIGNEE_NAME_SERVER}

    return fields


def create_subtask(jira: Jira, parent_key: str) -> Optional[str]:
    """
    Create a single sub-task under the given parent.
    Returns the created issue key, or None on failure.
    """
    try:
        ps = parent_summary(jira, parent_key)
        fields = build_fields(parent_key, ps)
        created = jira.create_issue(fields=fields)  # dict with key, id, self...
        key = created.get("key") or str(created)
        print(f"[OK] Created Sub-task {key} under {parent_key}: {fields['summary']}")
        return key
    except Exception as e:
        print(f"[ERROR] Failed to create Sub-task under {parent_key}: {e}")
        return None


def main():
    jira = get_client()

    created_keys = []
    for k in PARENT_ISSUE_KEYS:
        out = create_subtask(jira, k)
        if out:
            created_keys.append(out)

    print(f"\nDone. Created {len(created_keys)} Sub-task(s): {created_keys}")


if __name__ == "__main__":
    main()
