"""WebSocket endpoint for local AI bridge."""
import logging
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from src.security import decode_access_token
from src.ai.providers.local_bridge import register_agent, unregister_agent

logger = logging.getLogger(__name__)
router = APIRouter()


@router.websocket("/ai-bridge")
async def ai_bridge(ws: WebSocket):
    """WebSocket endpoint for local AI agents.

    The local agent connects and authenticates with a JWT token,
    then receives AI processing requests and returns results.
    """
    await ws.accept()

    # First message must be auth
    try:
        auth_msg = await ws.receive_json()
        token = auth_msg.get("token")
        if not token:
            await ws.send_json({"error": "Missing token"})
            await ws.close()
            return

        payload = decode_access_token(token)
        if not payload:
            await ws.send_json({"error": "Invalid token"})
            await ws.close()
            return

        user_id = int(payload["sub"])
        register_agent(user_id, ws)
        await ws.send_json({"status": "connected", "user_id": user_id})

        # Keep connection alive — requests are sent by the AI router
        while True:
            await ws.receive_text()  # keepalive pings

    except WebSocketDisconnect:
        if "user_id" in locals():
            unregister_agent(user_id)
            logger.info(f"Local agent disconnected for user {user_id}")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        if "user_id" in locals():
            unregister_agent(user_id)
