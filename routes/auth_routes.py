from fastapi import APIRouter, HTTPException
from models.auth import LoginRequest, LoginResponse
from services import auth_service

router = APIRouter(tags=["auth"])


@router.post("/login", response_model=LoginResponse)
def login(credentials: LoginRequest):
    print(credentials)
    try:
        success, user_data, message = auth_service.authenticate_user(
            credentials.username,
            credentials.password
        )

        return LoginResponse(
            success=success,
            user=user_data,
            message=message
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error en login: {str(e)}"
        )
