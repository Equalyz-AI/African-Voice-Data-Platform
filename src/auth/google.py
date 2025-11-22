from fastapi.security import OAuth2PasswordBearer
import requests
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Annotated
from src.db.db import get_session
from fastapi import APIRouter, Depends, HTTPException, status, Query, Request
from fastapi.responses import RedirectResponse
from src.auth.routes import validate
from src.config import settings


google_login = APIRouter()
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# Replace these with your own values from the Google Developer Console
GOOGLE_CLIENT_ID = settings.GOOGLE_CLIENT_ID
GOOGLE_CLIENT_SECRET = settings.GOOGLE_CLIENT_SECRET
GOOGLE_REDIRECT_URI = settings.GOOGLE_REDIRECT_URI


@google_login.get("/login/google")
async def login_google():
    google_auth_url = f"https://accounts.google.com/o/oauth2/auth?response_type=code&client_id={GOOGLE_CLIENT_ID}&redirect_uri={GOOGLE_REDIRECT_URI}&scope=openid%20profile%20email&access_type=offline"
    return RedirectResponse(url=google_auth_url)

@google_login.get("/google")
async def auth_google(
    code: str, 
    request: Request, 
    response: RedirectResponse, 
    session: Annotated[AsyncSession, Depends(get_session)],
):
    """
    Handle the callback from Google after user authentication.
    """
    token_url = "https://accounts.google.com/o/oauth2/token"
    data = {
        "code": code,
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "redirect_uri": GOOGLE_REDIRECT_URI,
        "grant_type": "authorization_code",
    }
    response = requests.post(token_url, data=data)
    access_token = response.json().get("access_token")
    user_info = requests.get("https://www.googleapis.com/oauth2/v1/userinfo", headers={"Authorization": f"Bearer {access_token}"})
    print(user_info.json())

    token = await validate(user_data=user_info.json(), request=request, session=session)

    redirect = RedirectResponse(url="http://localhost:3000/test", status_code=302)

    response.set_cookie(
        key="access_token",
        value=token,
        httponly=True,
        max_age=18000,
        samesite="none",
        secure=True,
    )
    return redirect

# @google_login.get("/token")
# async def get_token(token: str = Depends(oauth2_scheme)):
#     return jwt.decode(token, GOOGLE_CLIENT_SECRET, algorithms=["HS256"])
