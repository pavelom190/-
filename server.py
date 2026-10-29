import asyncio
import os
from collections import deque
from datetime import datetime, timezone

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import PlainTextResponse
import uvicorn

app = FastAPI(title="MyMessenger Server")

clients: dict[WebSocket, str] = {}
history = deque(maxlen=100)


@app.get("/")
async def home():
    return PlainTextResponse("MyMessenger server is running.")


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


async def broadcast(data: dict):
    dead = []
    for ws in list(clients):
        try:
            await ws.send_json(data)
        except Exception:
            dead.append(ws)
    for ws in dead:
        clients.pop(ws, None)


@app.websocket("/ws")
async def chat_socket(ws: WebSocket):
    await ws.accept()
    name = None

    try:
        first = await asyncio.wait_for(ws.receive_json(), timeout=30)
        if first.get("type") != "join":
            await ws.close(code=1008, reason="Send join first")
            return

        name = str(first.get("name", "")).strip()[:30]
        if not name:
            await ws.send_json({"type": "error", "text": "Name is required"})
            await ws.close(code=1008)
            return

        clients[ws] = name
        await ws.send_json({"type": "history", "messages": list(history)})
        await broadcast({
            "type": "system",
            "text": f"{name} приєднався до чату",
            "time": datetime.now(timezone.utc).isoformat()
        })

        while True:
            data = await ws.receive_json()
            if data.get("type") != "message":
                continue

            text = str(data.get("text", "")).strip()[:2000]
            if not text:
                continue

            message = {
                "type": "message",
                "name": name,
                "text": text,
                "time": datetime.now(timezone.utc).isoformat()
            }
            history.append(message)
            await broadcast(message)

    except WebSocketDisconnect:
        pass
    except asyncio.TimeoutError:
        try:
            await ws.close(code=1008, reason="Join timed out")
        except Exception:
            pass
    except Exception as exc:
        print(f"Client error: {exc}")
    finally:
        if name and ws in clients:
            clients.pop(ws, None)
            await broadcast({
                "type": "system",
                "text": f"{name} вийшов із чату",
                "time": datetime.now(timezone.utc).isoformat()
            })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    uvicorn.run("server:app", host="0.0.0.0", port=port)
