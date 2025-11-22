from fastapi import APIRouter, Depends, HTTPException, status, Query, Request
from fastapi.responses import RedirectResponse
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession
from src.db.db import get_session
from src.db.models import User
from src.errors import UserAlreadyExists, InvalidCredentials, InvalidToken, ResetPasswordFailed, EmailAlreadyVerified, UserLoggedOut, UserNotFound
from .schemas import UserCreateModel, GetTokenRequest, UserLoginModel, ResetPasswordSchemaResponseModel,FeedbackResponseModel,FeedbackCreateModel,  ForgotPasswordModel, ResetPasswordModel, LoginResponseModel, DeleteResponseModel, RegisterResponseModel, TokenUser, VerificationMailSchemaResponse
from .service import UserService
from .utils import create_access_token, get_current_user
from typing import Annotated
from src.auth.utils import create_access_token, get_current_user
from fastapi.encoders import jsonable_encoder
from fastapi import Response, Depends
import uuid
import jwt
from .utils import verify_email_response
from .schemas import GooglePayload
from src.config import settings
from datetime import timedelta
from google.oauth2 import id_token
from google.auth.transport import requests
import httpx
from starlette.requests import Request
from typing import Optional

auth_router = APIRouter()


@auth_router.post("/signup", response_model=RegisterResponseModel)
async def register_user(
    user: UserCreateModel,
    session: Annotated[AsyncSession, Depends(get_session)]
):
    """Register a new user."""
    user_service = UserService()
    try:
        new_user = await user_service.create_user(user, session, is_google=False)
        return RegisterResponseModel(
            status=True,
            message="User created successfully. Please check your mail to verify your account.",
            data=new_user
        )
    except Exception as e:
        print("The error from the register function is", e)
        raise e
    

@auth_router.post("/signin", response_model=LoginResponseModel)
async def login(
    form_data: UserLoginModel,
    session: Annotated[AsyncSession, Depends(get_session)],
    response: Response 
):
    """Login user and return access token."""
    user_service = UserService()

    user = await user_service.authenticate_user(
        form_data.email, 
        form_data.password, 
        session
    )
    access_token = create_access_token(user=user)

    # Set the access token as a cookie
    response.set_cookie(
        key="access_token",
        value=access_token,
        httponly=True,
        max_age=18000,
        samesite="none",
        secure=True
    )

    return LoginResponseModel(
        status=True,
        message="Login successful",
        data=user
    )



@auth_router.post("/verify-email/")
async def verify_email(
    session: Annotated[AsyncSession, Depends(get_session)],
    response: Response,
    token: str = Query(..., description="Verification token from email"),
):
    """Verify user's email using the provided token."""
    
    # Initialize UserService instance
    user_service = UserService()
    
    # Retrieve user based on the verification token
    user = await user_service.verify_token(token, session)

    if user.is_verified:
        raise EmailAlreadyVerified()

    # Update user verification status
    user.is_verified = True
    user.verification_token = None
    
    # Commit changes to the database
    await session.commit()
    await session.refresh(user)


    print("The user from ", user)
    
    # Generate access token for the verified user
    access_token = create_access_token(user=user)
    
    # Prepare response
    response = verify_email_response(user, access_token, response)
    
    return response


# Logout user
@auth_router.post("/logout", response_model=DeleteResponseModel)
async def logout(
    request: Request,
    response: Response,
    session: Annotated[AsyncSession, Depends(get_session)]
):
    """Logout user and clear access token."""
    # Clear the access token cookie
    # check if the cookie exists
    if "access_token" not in request.cookies:
        raise UserLoggedOut()
    response.delete_cookie(key="access_token")
    return DeleteResponseModel(
        status=True,
        message="Logout successful",
    )


@auth_router.get("/users/me", response_model=TokenUser)
async def read_users_me(
    current_user: Annotated[TokenUser, Depends(get_current_user)]
):
    """Get details of the current user."""
    return current_user


@auth_router.post("/google-token", response_model=LoginResponseModel, include_in_schema=True)
async def token(
    form_data: GetTokenRequest,
    session: Annotated[AsyncSession, Depends(get_session)],
    request: Request,
    response: Response,
):
    """
        This is responsible for exchanging the google code for an access token and validating the token.
        Send the user data to the user and sets access token in cookies.
    """
    print("The code is", form_data.code)
    google_token = form_data.code

    try:
        user_data = jwt.decode(google_token, options={"verify_signature": False})
        print("The decoded token is", user_data)
    
    except jwt.ExpiredSignatureError:
        raise InvalidToken()

    response = await validate(user_data, request, response, session)

    return response
   

# refresh token
@auth_router.get("/refresh-token", response_model=TokenUser)
async def refresh_token(
    current_user: Annotated[TokenUser, Depends(get_current_user)]
):
    """Refresh the access token for the current user."""
    access_token_expires = timedelta(minutes=300)
    access_token = create_access_token(
        user=current_user, expires_delta=access_token_expires
    )
    return TokenUser(
        full_name=current_user.full_name,
        email=current_user.email,
        id=str(current_user.id),
        is_verified=current_user.is_verified,
        access_token=access_token,
        token_type="bearer"
    )

# resend verification token
@auth_router.post("/resend-verification-token", response_model=VerificationMailSchemaResponse)
async def resend_verification_token(
    session: Annotated[AsyncSession, Depends(get_session)],
    response: Response,
    email: str = Query(..., description="Email of the user to resend verification token"),
):
    """Resend the verification token to the user's email."""
    user_service = UserService()
    response = await user_service.resend_verification_email(email, session)

    return response


# Reset password
@auth_router.post("/forgot-password")
async def forgot_password(
    payload: ForgotPasswordModel,
    session: Annotated[AsyncSession, Depends(get_session)],
):
    """Reset the password for the user."""
    user_service = UserService()
    response = await user_service.forgot_password(payload, session)
    return response


@auth_router.post("/reset-password/", response_model=ResetPasswordSchemaResponseModel)
async def reset_password_redirect(
    session: Annotated[AsyncSession, Depends(get_session)],
    payload: ResetPasswordModel,
    token: str = Query(..., description="Verification token from email"),
):
    """Verify user's email using the provided token."""
    
    # Initialize UserService instance
    user_service = UserService()
    
    # Retrieve user based on the verification token
    user = await user_service.verify_token(token, session)
    
    if not user:
        raise InvalidToken()
    
    # Update user verification status
    user.is_verified = True
    user.verification_token = None
    
    # Commit changes to the database
    await session.commit()
    await session.refresh(user)
    
    #  Reset the password
    response = await user_service.reset_password(user, payload, session)
            
    return response



# feedback
@auth_router.post("/feedback", response_model=FeedbackResponseModel)
async def create_feedback(
    feedback_data: FeedbackCreateModel,
    session: Annotated[AsyncSession, Depends(get_session)],
):
    """Create feedback from the user."""
    user_service = UserService()
    response = await user_service.create_feedback(feedback_data, session)

    print("The response from the feedback function is", response)
    return FeedbackResponseModel(
        status=True,
        message="Feedback sent successfully",
        data=response
    )


# delete user
@auth_router.delete("/delete-user", response_model=DeleteResponseModel)
async def delete_user(
    current_user: Annotated[TokenUser, Depends(get_current_user)],
    response: Response,
    session: Annotated[AsyncSession, Depends(get_session)],
):
    user_service = UserService()
    try:
        await user_service.delete_user(current_user, session)
        response.delete_cookie(key="access_token")
    except Exception as e:
        raise e

    return DeleteResponseModel(
        status=True,
        message="User deleted successfully and access token cleared",
    )
 

async def validate(user_data: dict, request:  Optional[Request] = None , response: Optional[Response] = None, session: Optional[AsyncSession] = None):
    print("The user data from the Google payload:", user_data)
    user_service = UserService()
    email = user_data.get("email")
    print("Checking for user with email:", email)

    try:
        user = await user_service.get_user_by_email(email, session)
        print("User exists:", user)
    
        if user is None:
            print("User not found. Creating new user...")

            user_model = UserCreateModel(
                full_name=user_data.get("name"),
                email=user_data.get("email"),
                password="password"
            )

            user = await user_service.create_user(user_model, session, is_google=True)
            print("User created successfully:", user)


    except Exception as e:
        print("Unexpected error:", e)
        raise e

    # Now generate the access token
    access_token_expires = timedelta(minutes=300)
    print("Creating access token for user:", user)
    access_token = create_access_token(user=user, expires_delta=access_token_expires)


    result = verify_email_response(user, access_token, response)
    return result
