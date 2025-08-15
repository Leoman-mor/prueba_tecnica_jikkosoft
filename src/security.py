import os
from fastapi import Header, HTTPException
from typing import Optional

# Configuración de API Key
API_KEY_ENV = "API_KEY"

# Guard para API Key
async def api_key_guard(x_api_key: Optional[str] = Header(default=None)):
    expected = os.getenv(API_KEY_ENV)
    if not expected:
        return True
    if x_api_key != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return True
