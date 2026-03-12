import os
import asyncio
import mimetypes
from urllib.parse import quote

from starlette.applications import Starlette
from starlette.responses import (
    FileResponse,
    HTMLResponse,
    PlainTextResponse,
    Response,
    StreamingResponse,
)
from starlette.routing import Route
import uvicorn

CURRENT_DIR = os.getcwd()
BASE_DIR = os.path.abspath(CURRENT_DIR)
CHUNK_SIZE = 1024 * 1024


def _resolve_path(path: str) -> str:
    return os.path.abspath(os.path.join(CURRENT_DIR, path))


def _is_forbidden(full_path: str) -> bool:
    return not (
        full_path == BASE_DIR or full_path.startswith(BASE_DIR + os.sep)
    )


def _build_common_headers(full_path: str, file_size: int) -> dict:
    filename = os.path.basename(full_path)
    return {
        "Accept-Ranges": "bytes",
        "Content-Length": str(file_size),
        "Content-Disposition": f"attachment; filename*=UTF-8''{quote(filename)}",
    }


def _parse_range_header(range_header: str, file_size: int) -> tuple[int, int]:
    if not range_header.startswith("bytes="):
        raise ValueError("Unsupported range unit")

    spec = range_header[6:].strip()
    if "," in spec or "-" not in spec:
        raise ValueError("Multiple ranges are not supported")

    start_text, end_text = spec.split("-", 1)

    if not start_text:
        if not end_text:
            raise ValueError("Invalid suffix range")
        length = int(end_text)
        if length <= 0:
            raise ValueError("Invalid suffix range length")
        start = max(file_size - length, 0)
        end = file_size - 1
    else:
        start = int(start_text)
        end = int(end_text) if end_text else file_size - 1
        if start >= file_size:
            raise ValueError("Range start exceeds file size")
        if end < start:
            raise ValueError("Range end precedes range start")
        end = min(end, file_size - 1)

    return start, end


def _iter_file_range(full_path: str, start: int, end: int):
    with open(full_path, "rb") as infile:
        infile.seek(start)
        remaining = end - start + 1

        while remaining > 0:
            chunk = infile.read(min(CHUNK_SIZE, remaining))
            if not chunk:
                return

            remaining -= len(chunk)
            yield chunk


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
    full_path = _resolve_path(path)

    # Evitar path traversal
    if _is_forbidden(full_path):
        return PlainTextResponse("403 Forbidden", status_code=403)

    if not os.path.isfile(full_path):
        return PlainTextResponse("404 Not Found", status_code=404)

    file_size = os.path.getsize(full_path)
    media_type = mimetypes.guess_type(full_path)[0] or "application/octet-stream"
    common_headers = _build_common_headers(full_path, file_size)
    range_header = request.headers.get("range")

    try:
        if not range_header:
            if request.method == "HEAD":
                return Response(status_code=200, headers=common_headers, media_type=media_type)

            return FileResponse(
                full_path,
                filename=os.path.basename(full_path),
                headers={"Accept-Ranges": "bytes"},
                media_type=media_type,
            )

        try:
            start, end = _parse_range_header(range_header, file_size)
        except ValueError:
            return PlainTextResponse(
                "416 Range Not Satisfiable",
                status_code=416,
                headers={
                    "Accept-Ranges": "bytes",
                    "Content-Range": f"bytes */{file_size}",
                },
            )

        content_length = end - start + 1
        headers = {
            **common_headers,
            "Content-Length": str(content_length),
            "Content-Range": f"bytes {start}-{end}/{file_size}",
        }

        if request.method == "HEAD":
            return Response(status_code=206, headers=headers, media_type=media_type)

        return StreamingResponse(
            _iter_file_range(full_path, start, end),
            status_code=206,
            headers=headers,
            media_type=media_type,
        )
    except (asyncio.CancelledError, BrokenPipeError, ConnectionResetError, OSError):
        # Cliente desconectado en mitad de la transferencia
        return PlainTextResponse("Client disconnected", status_code=499)


app = Starlette(routes=[
    Route("/", index),
    Route("/{path:path}", download, methods=["GET", "HEAD"]),
])

if __name__ == "__main__":
    print(f"Serving directory: {CURRENT_DIR}")
    print("Server running on http://0.0.0.0:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000, access_log=True)
