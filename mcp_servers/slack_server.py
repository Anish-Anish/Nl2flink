"""
MCP server: Slack notifications.

Tools
  send_slack_message   post to a channel with the workspace bot token
  send_slack_webhook   post to a webhook URL declared by a pipeline's Slack sink

Run:  python mcp_servers/slack_server.py
"""
from mcp.server.fastmcp import FastMCP

from backend.services.stack_clients import slack

server = FastMCP("slack")


@server.tool()
def send_slack_message(channel: str, message: str) -> bool:
    """Post a message to a Slack channel. Returns True when Slack accepted it."""
    return slack.post_to_channel(channel, message)


@server.tool()
def send_slack_webhook(webhook_url: str, message: str) -> bool:
    """Post a message to a Slack incoming webhook."""
    return slack.post_to_webhook(webhook_url, message)


if __name__ == "__main__":
    server.run()
