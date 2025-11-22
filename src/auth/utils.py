import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Callable
from itsdangerous import URLSafeTimedSerializer

import jwt, logging
from passlib.context import CryptContext
from src.config import settings
from src.errors import InvalidCredentials, InvalidToken, UnAuthenticated, UserNotFound
from fastapi import Request
import secrets
from fastapi.security import OAuth2PasswordBearer
from fastapi import Depends
from src.config import settings
from fastapi.encoders import jsonable_encoder
from .schemas import LoginResponseModel, TokenUser
from typing import Optional
from fastapi.security import HTTPBearer

passwd_context = CryptContext(schemes=["bcrypt"])
ACCESS_TOKEN_EXPIRE_MINUTES = 30
logger = logging.getLogger(__name__)

class OptionalOAuth2Scheme(OAuth2PasswordBearer):
    async def __call__(self, request: Request) -> Optional[str]:
        try:
            return await super().__call__(request)
        except Exception:
            return None

# Replace with the optional version
optional_oauth2_scheme = OptionalOAuth2Scheme(tokenUrl="token")


def generate_passwd_hash(password: str) -> str:
    hash = passwd_context.hash(password)

    return hash


def verify_password(password: str, hash: str) -> bool:
    return passwd_context.verify(password, hash)

def get_password_hash(password: str):
    return passwd_context.hash(password)


# def create_access_token(
#     user_data: dict, expiry: timedelta = None, refresh: bool = False
# ):
#     payload = {}

#     payload["user"] = user_data
#     payload["exp"] = datetime.now() + (
#         expiry if expiry is not None else timedelta(seconds=ACCESS_TOKEN_EXPIRE_MINUTES)
#     )
#     payload["jti"] = str(uuid.uuid4())

#     payload["refresh"] = refresh

#     token = jwt.encode(
#         payload=payload, key=settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM
#     )

#     return token


def decode_token(token: str) -> dict:
    try:
        token_data = jwt.decode(
            jwt=token, key=settings.JWT_SECRET, algorithms=[settings.JWT_ALGORITHM]
        )

        return token_data

    except jwt.PyJWTError as e:
        logging.exception(e)
        return None

serializer = URLSafeTimedSerializer(
    secret_key=settings.JWT_SECRET, salt="email-configuration"
)

def create_url_safe_token(data: dict):

    token = serializer.dumps(data)

    return token

def decode_url_safe_token(token:str):
    try:
        token_data = serializer.loads(token)

        return token_data
    
    except Exception as e:
        logging.error(str(e))

ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 300


def create_access_token(user, expires_delta: timedelta | None = None):
    to_encode = {
        "sub": user.email,
        "id": str(user.id),
        "is_verified": user.is_verified,
        "full_name": user.full_name,
        "exp": datetime.now(timezone.utc) + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    }
    return jwt.encode(to_encode, settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)


async def get_current_user(
    request: Request,
    token: Optional[str] = Depends(optional_oauth2_scheme),
):
    # First, check Authorization header (OAuth2)
    access_token = token or request.cookies.get("access_token")

    if not access_token:
        raise UnAuthenticated()

    try:
        payload = jwt.decode(access_token, settings.JWT_SECRET, algorithms=[settings.JWT_ALGORITHM])
        email = payload.get("sub")
        user_id = payload.get("id")
        full_name = payload.get("full_name")

        if not email or not user_id:
            raise UserNotFound()

        return TokenUser(
            full_name=full_name,
            email=email,
            id=user_id,
            is_verified=payload.get("is_verified"),
            token_type="bearer"
        )

    except jwt.ExpiredSignatureError:
        raise InvalidToken()
    except jwt.PyJWTError as e:
        logger.warning(f"Token validation error: {str(e)}")
        raise UnAuthenticated()


def verify_email_response(user, access_token: str, response):
    print("This is the user", user)

    user_data = {
        "id": str(user.id),
        "full_name": user.full_name,
        "email": user.email,
        "is_verified": user.is_verified,
        "created_at": user.created_at,
        "updated_at": user.updated_at,
    }

    response.set_cookie(
        key="access_token",
        value=access_token,
        httponly=True, 
        max_age=18000, 
        samesite="none",
        secure=True,
    )


    return LoginResponseModel(
        status=True,
        message="User created successfully",
        data=user_data
    )
