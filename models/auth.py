from pydantic import BaseModel
from typing import Optional, Dict


class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    success: bool
    user: Optional[Dict] = None
    message: Optional[str] = None
