# -*- coding: utf-8 -*-
"""
Create Sub-tasks under multiple parent issues in Jira (Cloud or Server/DC),
naming each sub-task as: "<SUBTASK_PREFIX> <Parent Issue Summary>".

Requires:
    pip install jira
"""

from typing import List, Optional, Dict
from jira import JIRA
from jira.exceptions import JIRAError

# ==========
# INPUTS — edit these
# ==========
USE_CLOUD = True  # False for Jira Server/DC

# Jira Cloud auth
JIRA_BASE_URL = "https://your-domain.atlassian.net"
JIRA_EMAIL = "you@example.com"
JIRA_API_TOKEN = "YOUR_API_TOKEN_HERE"

# Jira Server/DC auth (only if USE_CLOUD=False)
JIRA_USERNAME = "your_jira_username"
JIRA_PASSWORD = "your_password_or_pat"

PROJECT_KEY = "ABC"
PARENT_ISSUE_KEYS: List[str] = ["ABC-123", "ABC-456"]

SUBTASK_ISSUE_TYPE_NAME = "Sub-task"   # adjust if your instance renamed it
SUBTASK_PREFIX = "[Prep]"              # <<< user-defined prefix

# Optional fields
LABELS = ["auto-created"]
COMPONENT_NAMES: List[str] = []        # must already exist in the project

# Assignee (choose one depending on hosting)
ASSIGNEE_ACCOUNT_ID_CLOUD: Optional[str] = None   # e.g., "557058:abcd..."
ASSIGNEE_NAME_SERVER: Optional[str] = None        # e.g., "jdoe"

# Optional description (kept constant for all)
SUBTASK_DESCRIPTION = (
    "Auto-created sub-task. Please review and complete."
)

# ========== END OF INPUTS ==========


def get_jira_client() -> JIRA:
    if USE_CLOUD:
        return JIRA(server=JIRA_BASE_URL, basic_auth=(JIRA_EMAIL, JIRA_API_TOKEN))
    return JIRA(server=JIRA_BASE_URL, basic_auth=(JIRA_USERNAME, JIRA_PASSWORD))


def build_fields(parent_key: str, parent_summary: str) -> Dict:
    """Build fields for a Sub-task under a given parent, naming with prefix + parent summary."""
    summary = f"{SUBTASK_PREFIX} {parent_summary}".strip()
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
    if USE_CLOUD and ASSIGNEE_ACCOUNT_ID_CLOUD:
        fields["assignee"] = {"accountId": ASSIGNEE_ACCOUNT_ID_CLOUD}
    elif not USE_CLOUD and ASSIGNEE_NAME_SERVER:
        fields["assignee"] = {"name": ASSIGNEE_NAME_SERVER}
    return fields


def create_subtask_for_parent(jira: JIRA, parent_key: str) -> Optional[str]:
    try:
        parent = jira.issue(parent_key)
        parent_summary = parent.fields.summary or parent_key
        if parent.fields.project.key != PROJECT_KEY:
            print(f"[WARN] Parent {parent_key} is in project {parent.fields.project.key}, "
                  f"but PROJECT_KEY is {PROJECT_KEY}. Proceeding.")
        fields = build_fields(parent_key, parent_summary)
        issue = jira.create_issue(fields=fields)
        print(f"[OK] Created Sub-task {issue.key} under {parent_key}: {fields['summary']}")
        return issue.key
    except JIRAError as e:
        print(f"[ERROR] {parent_key}: {getattr(e, 'text', str(e))}")
    except Exception as e:
        print(f"[ERROR] {parent_key}: {e}")
    return None


def main():
    jira = get_jira_client()
    # Optional: check sub-task issue type exists (may be restricted by perms)
    try:
        meta = jira.createmeta(projectKeys=PROJECT_KEY, expand="projects.issuetypes.fields")
        issue_types = {it["name"] for p in meta.get("projects", []) for it in p.get("issuetypes", [])}
        if SUBTASK_ISSUE_TYPE_NAME not in issue_types:
            print(f"[WARN] Issue type '{SUBTASK_ISSUE_TYPE_NAME}' not visible in create meta; "
                  f"ensure it's enabled for the project.")
    except Exception:
        pass

    created = []
    for key in PARENT_ISSUE_KEYS:
        out = create_subtask_for_parent(jira, key)
        if out:
            created.append(out)
    print(f"\nDone. Created {len(created)} Sub-task(s): {created}")


if __name__ == "__main__":
    main()
