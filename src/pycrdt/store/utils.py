from pathlib import Path

import anyio


async def get_new_path(path: str) -> str:
    p = Path(path)
    ext = p.suffix
    p_noext = p.with_suffix("")
    i = 1
    while True:
        new_path = f"{p_noext}({i}){ext}"
        if not await anyio.Path(new_path).exists():
            break
        i += 1
    return new_path
