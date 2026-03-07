#!/usr/bin/env python3
"""
MailIA Local Agent — runs on your PC, bridges local Ollama to the MailIA server.

Usage:
    python local_agent.py --server wss://mailia.expert-presta.com/ws/ai-bridge --token YOUR_JWT

The agent:
1. Connects to the MailIA server via WebSocket
2. Receives AI processing requests
3. Forwards them to local Ollama
4. Returns results to the server
"""
import argparse
import asyncio
import json
import logging
import sys

import httpx
import websockets

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("mailia-agent")


async def forward_to_ollama(request: dict, ollama_url: str) -> dict:
    """Forward a chat request to local Ollama."""
    async with httpx.AsyncClient(timeout=120.0) as client:
        response = await client.post(
            f"{ollama_url}/api/chat",
            json={
                "model": request.get("model", "mistral:latest"),
                "messages": request.get("messages", []),
                "stream": False,
            },
        )
        response.raise_for_status()
        data = response.json()

    return {
        "content": data["message"]["content"],
        "model": data.get("model", ""),
        "tokens_used": data.get("eval_count", 0),
    }


async def run_agent(server_url: str, token: str, ollama_url: str):
    """Main agent loop."""
    while True:
        try:
            logger.info(f"Connecting to {server_url}...")
            async with websockets.connect(server_url) as ws:
                # Authenticate
                await ws.send(json.dumps({"token": token}))
                auth_response = json.loads(await ws.recv())

                if auth_response.get("error"):
                    logger.error(f"Auth failed: {auth_response['error']}")
                    return

                logger.info(f"Connected as user {auth_response.get('user_id')}")

                # Process requests
                async for message in ws:
                    try:
                        request = json.loads(message)
                        if request.get("type") == "chat":
                            logger.info(f"Processing chat request (model: {request.get('model')})")
                            result = await forward_to_ollama(request, ollama_url)
                            await ws.send(json.dumps(result))
                            logger.info("Response sent")
                        else:
                            logger.warning(f"Unknown request type: {request.get('type')}")
                    except Exception as e:
                        logger.error(f"Error processing request: {e}")
                        await ws.send(json.dumps({"error": str(e)}))

        except (websockets.ConnectionClosed, ConnectionRefusedError, OSError) as e:
            logger.warning(f"Connection lost: {e}. Reconnecting in 10s...")
            await asyncio.sleep(10)


def main():
    parser = argparse.ArgumentParser(description="MailIA Local AI Agent")
    parser.add_argument("--server", default="wss://mailia.expert-presta.com/ws/ai-bridge",
                        help="MailIA WebSocket server URL")
    parser.add_argument("--token", required=True, help="Your MailIA JWT token")
    parser.add_argument("--ollama", default="http://localhost:11434",
                        help="Local Ollama URL")
    args = parser.parse_args()

    asyncio.run(run_agent(args.server, args.token, args.ollama))


if __name__ == "__main__":
    main()
