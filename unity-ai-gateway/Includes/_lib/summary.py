# _lib/summary.py
#
# Renders an HTML summary card of the workspace assets created during setup.
# Pure string builder — no notebook globals (displayHTML/dbutils) — so it stays
# testable outside a notebook. The notebook calls displayHTML() on the result.

from html import escape
from typing import Optional


def _link(url: str) -> str:
    """A clickable, new-tab link cell (URL is the visible text)."""
    safe = escape(url, quote=True)
    return (
        f'<a href="{safe}" target="_blank" rel="noopener noreferrer" '
        f'style="color:#2272B4;text-decoration:none;font-weight:600">{escape(url)} &#8599;</a>'
    )


def _status_badge(state: str) -> str:
    """Color-coded app status: green RUNNING, red CRASHED, amber otherwise."""
    s = (state or "").upper()
    color = {"RUNNING": "#00A972", "CRASHED": "#98102A"}.get(s, "#DC6222")
    return f'<span style="color:{color};font-weight:700">{escape(s or "UNKNOWN")}</span>'


def _copy_button(value: str) -> str:
    """A small "Copy" button that copies ``value`` to the clipboard.

    displayHTML renders inside a sandboxed iframe where the async Clipboard API is
    usually blocked, so we use a hidden <textarea> + document.execCommand('copy'),
    which works in that context. The raw value is JS-string-escaped and embedded
    in the onclick handler.
    """
    js_val = (
        value.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n").replace("\r", "")
    )
    onclick = (
        "var t=document.createElement('textarea');"
        f"t.value='{js_val}';"
        "document.body.appendChild(t);t.select();"
        "try{{document.execCommand('copy');}}catch(e){{}}"
        "document.body.removeChild(t);"
        "var b=this;var o=b.innerText;b.innerText='Copied';"
        "setTimeout(function(){{b.innerText=o;}},1200);"
    )
    return (
        f'<button onclick="{escape(onclick, quote=True)}" title="Copy" '
        'style="flex:0 0 auto;padding:2px 9px;font-size:11px;font-family:inherit;'
        "color:#1B5162;background:#F9F7F4;border:1px solid #EEEDE9;border-radius:5px;"
        'cursor:pointer">Copy</button>'
    )


def build_setup_summary_html(env: dict, workspace_host: Optional[str] = None) -> str:
    """Build an HTML card listing the workspace assets created during setup.

    Parameters
    ----------
    env : dict
        The dict returned by ``setup_demo_environment``.
    workspace_host : str, optional
        Workspace URL (e.g. ``https://<ws>.cloud.databricks.com``). Used to build
        the clickable MLflow experiment link. When absent, the experiment row is
        shown as a plain id (or omitted if there's no id).

    Returns
    -------
    str
        HTML for ``displayHTML(...)``. Rows whose value is missing are skipped,
        so the card stays clean across configs (e.g. no volume, no app). Each row
        (except status) shows a Copy button that copies the raw value.
    """
    app = env.get("agent_app") or {}
    host = (workspace_host or "").rstrip("/")
    exp_id = app.get("experiment_id")
    exp_url = f"{host}/ml/experiments/{exp_id}" if (exp_id and host) else None

    # Registered system prompt (name + version + alias), shown as "name (v3 @champion)".
    prompt_name = app.get("system_prompt_name")
    prompt_version = app.get("system_prompt_version")
    prompt_alias = app.get("system_prompt_alias") or "champion"
    prompt_display = (
        f"{prompt_name} (v{prompt_version} @{prompt_alias})"
        if prompt_name and prompt_version
        else prompt_name
    )

    # (label, display_html, copy_value) — assets only; None-valued rows dropped.
    # display_html is pre-built markup (link/badge) or None to auto-render the
    # copy_value as escaped monospace text. copy_value None => no Copy button.
    rows: list[tuple[str, Optional[str], Optional[str]]] = [
        ("Catalog", None, env.get("catalog_name")),
        ("Schema", None, env.get("schema_name")),
        ("Volume", None, env.get("volume_path")),
        ("Table", None, env.get("table_name")),
        ("Agent app", None, app.get("app_name")),
        ("App URL", _link(app["app_url"]) if app.get("app_url") else None, app.get("app_url")),
        ("App status", _status_badge(app["app_status"]) if app.get("app_status") else None, None),
        (
            "MLflow experiment",
            _link(exp_url) if exp_url else None,
            exp_url or (str(exp_id) if exp_id else None),
        ),
        ("System prompt", None, prompt_display),
    ]

    body = ""
    for label, display_html, copy_value in rows:
        # A row shows if it has display markup or a value to render.
        if display_html is None and (copy_value is None or copy_value == ""):
            continue
        cell = display_html if display_html is not None else escape(str(copy_value))
        button = _copy_button(str(copy_value)) if copy_value else ""
        body += (
            "<tr>"
            f'<td style="padding:8px 14px;white-space:nowrap;border-bottom:1px solid #EEEDE9;'
            f'font-weight:600;color:#0b2026;width:220px;vertical-align:middle">{escape(label)}</td>'
            f'<td style="padding:8px 14px;border-bottom:1px solid #EEEDE9;color:#0b2026;'
            f'font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:13px">'
            f'<div style="display:flex;align-items:center;justify-content:space-between;gap:12px">'
            f'<span>{cell}</span>{button}</div></td>'
            "</tr>"
        )

    return (
        '<div style="font-family:-apple-system,BlinkMacSystemFont,\'Segoe UI\',Roboto,sans-serif;'
        'max-width:820px;margin:12px 0;border:1px solid #EEEDE9;border-radius:10px;overflow:hidden;'
        'box-shadow:0 2px 8px rgba(27,49,57,0.06)">'
        '<div style="background:#1B5162;color:#fff;padding:12px 16px;font-size:15px;font-weight:700">'
        "Workspace Assets Created</div>"
        '<table style="width:100%;border-collapse:collapse;font-size:14px">'
        f"{body}"
        "</table></div>"
    )
