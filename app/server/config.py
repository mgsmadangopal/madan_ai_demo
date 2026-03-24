"""Dual-mode authentication for Databricks Apps and local development."""
import os
from databricks.sdk import WorkspaceClient

IS_DATABRICKS_APP = bool(os.environ.get("DATABRICKS_APP_NAME"))
WORKSPACE_HOST = "https://e2-demo-field-eng.cloud.databricks.com"
MAS_ENDPOINT = "mas-9520a98b-endpoint"
DASHBOARD_ID = "01f11d227fc917f0994dd67e5cf99167"
DASHBOARD_EMBED_URL = f"{WORKSPACE_HOST}/embed/dashboardsv3/{DASHBOARD_ID}?o=984752964297111"


def get_workspace_client() -> WorkspaceClient:
    """Get WorkspaceClient configured for current environment."""
    if IS_DATABRICKS_APP:
        return WorkspaceClient()
    profile = os.environ.get("DATABRICKS_PROFILE", "e2-demo-west")
    return WorkspaceClient(profile=profile)


def get_workspace_host() -> str:
    """Get workspace host URL with https:// prefix."""
    if IS_DATABRICKS_APP:
        host = os.environ.get("DATABRICKS_HOST", "")
        if host and not host.startswith("http"):
            host = f"https://{host}"
        return host or WORKSPACE_HOST
    return WORKSPACE_HOST


def get_token() -> str:
    """Get OAuth token for API calls."""
    client = get_workspace_client()
    # Try direct token first
    if client.config.token:
        return client.config.token
    # Fall back to authenticate() for OAuth/U2M auth
    auth_headers = client.config.authenticate()
    if auth_headers and "Authorization" in auth_headers:
        return auth_headers["Authorization"].replace("Bearer ", "")
    raise RuntimeError("Unable to obtain authentication token")
