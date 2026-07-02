"""Persona sign-in. Two fixed users, matching the two sides of the product."""
from fastapi import APIRouter

from backend.api_models import LoginRequest, UserSession

router = APIRouter(prefix="/api/v1/auth", tags=["auth"])

_USERS = {
    "business": UserSession(user_id="priya_001", name="Priya", role="CEO",
                            persona="business", token="token_priya"),
    "engineer": UserSession(user_id="arjun_001", name="Arjun", role="Data Engineer",
                            persona="engineer", token="token_arjun"),
}


@router.post("/login", response_model=UserSession)
def login(body: LoginRequest) -> UserSession:
    """Return the identity for a persona."""
    return _USERS[body.persona]
