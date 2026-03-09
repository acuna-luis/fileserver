import os
import asyncio
from urllib.parse import quote

from starlette.applications import Starlette
from starlette.responses import HTMLResponse, PlainTextResponse, FileResponse
from starlette.routing import Route
import uvicorn

CURRENT_DIR = os.getcwd()


async def index(request):
    entries = sorted(os.listdir(CURRENT_DIR))
    items = []

    for name in entries:
        full_path = os.path.join(CURRENT_DIR, name)
        if os.path.isfile(full_path):
            href = "/" + quote(name)
            items.append(f'<li><a href="{href}">{name}</a></li>')

    html = f"""<!doctype html>
<html>
<head>
    <meta charset="utf-8">
    <title>File server</title>
</head>
<body>
    <h1>Serving: {CURRENT_DIR}</h1>
    <ul>
        {''.join(items)}
    </ul>
</body>
</html>
"""
    return HTMLResponse(html)


async def download(request):
    path = request.path_params["path"]
    full_path = os.path.abspath(os.path.join(CURRENT_DIR, path))

    # Evitar path traversal
    if not full_path.startswith(os.path.abspath(CURRENT_DIR) + os.sep):
        return PlainTextResponse("403 Forbidden", status_code=403)

    if not os.path.isfile(full_path):
        return PlainTextResponse("404 Not Found", status_code=404)

    try:
        return FileResponse(full_path, filename=os.path.basename(full_path))
    except (asyncio.CancelledError, BrokenPipeError, ConnectionResetError, OSError):
        # Cliente desconectado en mitad de la transferencia
        return PlainTextResponse("Client disconnected", status_code=499)


app = Starlette(routes=[
    Route("/", index),
    Route("/{path:path}", download),
])

if __name__ == "__main__":
    print(f"Serving directory: {CURRENT_DIR}")
    print("Server running on http://0.0.0.0:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000, access_log=True)

