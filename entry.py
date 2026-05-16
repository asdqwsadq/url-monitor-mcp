#!/usr/bin/env python3
"""url-monitor-mcp: URL变化监控 MCP stdio server"""

import asyncio
import json
import sys
import hashlib
import difflib
import time
from typing import Any

import httpx

# ---------------------------------------------------------------------------
# STDIO MCP Protocol helpers
# ---------------------------------------------------------------------------

def send_json(obj: dict[str, Any]) -> None:
    line = json.dumps(obj, ensure_ascii=False)
    sys.stdout.write(line + "\n")
    sys.stdout.flush()


def recv_json() -> dict[str, Any] | None:
    line = sys.stdin.readline()
    if not line:
        return None
    return json.loads(line)


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

_url_cache: dict[str, str] = {}  # url -> last_content_hash
_url_content: dict[str, str] = {}  # url -> last_content (for diff)


async def fetch_url(url: str) -> str | None:
    """Fetch a URL and return its text content, or None on failure."""
    try:
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.text
    except Exception as e:
        return None


def compute_diff(old_text: str, new_text: str) -> list[str]:
    """Return unified diff lines between old and new text."""
    old_lines = old_text.splitlines(keepends=True)
    new_lines = new_text.splitlines(keepends=True)
    diff = list(
        difflib.unified_diff(
            old_lines, new_lines,
            fromfile="previous", tofile="current",
            lineterm="",
        )
    )
    return diff


async def check_urls(urls: list[str], interval_seconds: int) -> dict[str, Any]:
    """Check URLs once and return results."""
    results: dict[str, Any] = {
        "checked_at": time.time(),
        "total": len(urls),
        "changed": [],
        "unchanged": [],
        "errors": [],
    }

    for url in urls:
        content = await fetch_url(url)
        if content is None:
            results["errors"].append({"url": url, "error": "Failed to fetch"})
            continue

        content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
        previous_hash = _url_cache.get(url)

        if previous_hash is None:
            # First time seeing this URL – store but don't report as change
            _url_cache[url] = content_hash
            _url_content[url] = content
            results["unchanged"].append({
                "url": url,
                "status": "initialized",
                "message": "First check – content cached, no diff available",
            })
        elif previous_hash != content_hash:
            old_content = _url_content.get(url, "")
            diff_lines = compute_diff(old_content, content)
            _url_cache[url] = content_hash
            _url_content[url] = content
            results["changed"].append({
                "url": url,
                "status": "changed",
                "diff": "".join(diff_lines),
                "diff_line_count": len(diff_lines),
            })
        else:
            results["unchanged"].append({
                "url": url,
                "status": "unchanged",
            })

    return results


async def monitor_loop(urls: list[str], interval_seconds: int, max_checks: int | None = None):
    """Run monitoring loop and stream results."""
    check_count = 0
    while True:
        if max_checks is not None and check_count >= max_checks:
            break
        result = await check_urls(urls, interval_seconds)
        send_json({
            "type": "monitor_update",
            "data": result,
        })
        check_count += 1
        if max_checks is not None and check_count >= max_checks:
            break
        await asyncio.sleep(interval_seconds)


# ---------------------------------------------------------------------------
# MCP Tool definitions
# ---------------------------------------------------------------------------

TOOL_CHECK_URLS = {
    "name": "check_urls",
    "description": "检查一个或多个URL的内容是否发生变化",
    "inputSchema": {
        "type": "object",
        "properties": {
            "urls": {
                "type": "array",
                "items": {"type": "string"},
                "description": "要监控的URL列表",
            },
        },
        "required": ["urls"],
    },
}

TOOL_START_MONITOR = {
    "name": "start_monitor",
    "description": "启动URL变化持续监控（按指定间隔检查）",
    "inputSchema": {
        "type": "object",
        "properties": {
            "urls": {
                "type": "array",
                "items": {"type": "string"},
                "description": "要监控的URL列表",
            },
            "interval": {
                "type": "integer",
                "description": "检查间隔（秒），默认60",
                "default": 60,
            },
            "max_checks": {
                "type": "integer",
                "description": "最大检查次数，不指定则持续运行",
            },
        },
        "required": ["urls"],
    },
}

TOOL_GET_DIFF = {
    "name": "get_diff",
    "description": "手动获取某个URL的详细变化对比（基于上次缓存的内容）",
    "inputSchema": {
        "type": "object",
        "properties": {
            "url": {
                "type": "string",
                "description": "要检查的URL",
            },
            "refresh_cache": {
                "type": "boolean",
                "description": "是否刷新缓存（获取最新内容作为基准）",
                "default": False,
            },
        },
        "required": ["url"],
    },
}


async def handle_tool_call(tool_name: str, args: dict[str, Any]) -> dict[str, Any]:
    match tool_name:
        case "check_urls":
            urls = args["urls"]
            result = await check_urls(urls, 0)
            # Build a readable summary
            summary_parts = []
            if result["changed"]:
                for c in result["changed"]:
                    summary_parts.append(f"🔴 {c['url']}: 内容已变化（{c['diff_line_count']}行差异）")
            if result["unchanged"]:
                for u in result["unchanged"]:
                    summary_parts.append(f"🟢 {u['url']}: {u.get('message', '内容未变化')}")
            if result["errors"]:
                for e in result["errors"]:
                    summary_parts.append(f"⚠️  {e['url']}: {e['error']}")
            return {
                "content": [
                    {
                        "type": "text",
                        "text": "\n".join(summary_parts) if summary_parts else "没有URL被检查",
                    },
                    {
                        "type": "text",
                        "text": json.dumps(result, ensure_ascii=False, indent=2),
                    },
                ]
            }

        case "start_monitor":
            urls = args["urls"]
            interval = args.get("interval", 60)
            max_checks = args.get("max_checks")
            # Start the monitor in background (non-blocking for the MCP protocol)
            asyncio.create_task(monitor_loop(urls, interval, max_checks))
            return {
                "content": [
                    {
                        "type": "text",
                        "text": f"✅ 监控已启动: {len(urls)}个URL, 间隔{interval}秒"
                        + (f", 最多检查{max_checks}次" if max_checks else ""),
                    }
                ]
            }

        case "get_diff":
            url = args["url"]
            refresh = args.get("refresh_cache", False)
            content = await fetch_url(url)
            if content is None:
                return {
                    "isError": True,
                    "content": [{"type": "text", "text": f"❌ 无法获取URL: {url}"}],
                }

            old_content = _url_content.get(url)
            if old_content is None or refresh:
                _url_content[url] = content
                _url_cache[url] = hashlib.sha256(content.encode("utf-8")).hexdigest()
                return {
                    "content": [
                        {
                            "type": "text",
                            "text": f"📥 已缓存 {url} 的最新内容（共{len(content)}字符）\n"
                            f"  下次检查时会基于此版本对比差异。",
                        }
                    ],
                }

            diff_lines = compute_diff(old_content, content)
            if not diff_lines:
                return {
                    "content": [{"type": "text", "text": f"✅ {url} 内容未发生变化"}],
                }

            _url_content[url] = content
            _url_cache[url] = hashlib.sha256(content.encode("utf-8")).hexdigest()
            return {
                "content": [
                    {
                        "type": "text",
                        "text": f"🔴 {url} 内容发生变化（{len(diff_lines)}行差异）:\n```diff\n"
                        + "".join(diff_lines)
                        + "\n```",
                    }
                ],
            }

        case _:
            return {
                "isError": True,
                "content": [{"type": "text", "text": f"Unknown tool: {tool_name}"}],
            }


# ---------------------------------------------------------------------------
# Main MCP event loop
# ---------------------------------------------------------------------------

async def main():
    while True:
        msg = recv_json()
        if msg is None:
            break

        method = msg.get("method")
        msg_id = msg.get("id")

        match method:
            case "initialize":
                send_json({
                    "id": msg_id,
                    "result": {
                        "protocolVersion": "2024-11-05",
                        "capabilities": {
                            "tools": {},
                        },
                        "serverInfo": {
                            "name": "url-monitor-mcp",
                            "version": "1.0.0",
                        },
                    },
                })

            case "tools/list":
                send_json({
                    "id": msg_id,
                    "result": {
                        "tools": [TOOL_CHECK_URLS, TOOL_START_MONITOR, TOOL_GET_DIFF],
                    },
                })

            case "tools/call":
                tool_name = msg["params"]["name"]
                tool_args = msg["params"].get("arguments", {})
                try:
                    result = await handle_tool_call(tool_name, tool_args)
                    send_json({
                        "id": msg_id,
                        "result": result,
                    })
                except Exception as e:
                    send_json({
                        "id": msg_id,
                        "result": {
                            "isError": True,
                            "content": [{"type": "text", "text": f"Error: {e}"}],
                        },
                    })

            case "notifications/initialized":
                # Ignore, just continue
                pass

            case _:
                send_json({
                    "id": msg_id,
                    "result": {"error": f"Unknown method: {method}"},
                })


if __name__ == "__main__":
    asyncio.run(main())
