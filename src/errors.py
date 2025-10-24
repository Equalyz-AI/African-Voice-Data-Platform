from typing import Any, Callable
from fastapi.requests import Request
from fastapi.responses import JSONResponse
from fastapi import FastAPI, status
from sqlalchemy.exc import SQLAlchemyError

class AfricanVoicesStorageException(Exception):
    """Base class for all AI for Governance platform-related exceptions."""
    
    def __init__(self, message: str = "An error occurred", error_code: str = "error"):
        self.message = message
        self.error_code = error_code
        super().__init__(self.message)

    
class DatabaseError(AfricanVoicesStorageException):
    """An error occurred while interacting with the database."""
    def __init__(self, message: str = "Database error occurred", error_code: str = "database_error"):
        super().__init__(message=message, error_code=error_code)
        self.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR


class InvalidToken(AfricanVoicesStorageException):
    """User has provided an invalid or expired token."""
    pass

class UserLoggedOut(AfricanVoicesStorageException):
    """User has logged out and the token is no longer valid."""
    pass

class ResetPasswordFailed(AfricanVoicesStorageException):
    """User has provided an invalid or expired token."""
    pass

class RevokedToken(AfricanVoicesStorageException):
    """User has provided a token that has been revoked."""
    pass


class AccessTokenRequired(AfricanVoicesStorageException):
    """User has provided a refresh token when an access token is needed."""
    pass


class RefreshTokenRequired(AfricanVoicesStorageException):
    """User has provided an access token when a refresh token is needed."""
    pass


class UserAlreadyExists(AfricanVoicesStorageException):
    """User is trying to register with an email that already exists."""
    def __init__(self, message: str = "User with this email already exists"):
        super().__init__(message=message, error_code="user_exists")

class EmailAlreadyVerified(AfricanVoicesStorageException):
    """User is trying to register with an email that has already been verified."""
    def __init__(self, message: str = "Email already verified"):
        super().__init__(message=message, error_code="email_already_verified")

class EmailNotVerified(AfricanVoicesStorageException):
    """User has not verified their email."""
    def __init__(self, message: str = "Email not verified"):
        super().__init__(message=message, error_code="email_not_verified")

class InvalidCredentials(AfricanVoicesStorageException):
    """User has provided incorrect login details."""
    pass

class UnAuthenticated(AfricanVoicesStorageException):
    """User is not authenticated."""
    pass


class InsufficientPermission(AfricanVoicesStorageException):
    """User does not have the necessary permissions to perform an action."""
    pass


class UserNotFound(AfricanVoicesStorageException):
    """User not found in the system."""
    pass


class AccountNotVerified(AfricanVoicesStorageException):
    """User account has not been verified yet."""
    pass





def create_exception_handler(
    status_code: int,
    initial_detail: dict = None
) -> Callable[[Request, AfricanVoicesStorageException], JSONResponse]:

    initial_detail = initial_detail or {}

    async def exception_handler(request: Request, exc: AfricanVoicesStorageException):
        return JSONResponse(
            status_code=status_code,
            content={
                "message": exc.message or initial_detail.get("message", "An unexpected error occurred"),
                "error_code": exc.error_code or initial_detail.get("error_code", "unknown_error"),
                "resolution": initial_detail.get("resolution", "Please try again later"),
            }
        )

    return exception_handler






def register_all_errors(app: FastAPI):
    """Registers all exception handlers in the FastAPI app."""
    app.add_exception_handler(
        DatabaseError,
        create_exception_handler(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            initial_detail={
                "message": "Database error occurred",
                "resolution": "Please try again later",
                "error_code": "database_error",
            },
        ),
    )

    app.add_exception_handler(
        UserAlreadyExists,
        create_exception_handler(
            status_code=status.HTTP_403_FORBIDDEN,
            initial_detail={
                "message": "User with this email already exists",
                "resolution": "Please use a different email",
                "error_code": "user_exists",
            },
        ),
    )

    app.add_exception_handler(
        EmailAlreadyVerified,
        create_exception_handler(
            status_code=status.HTTP_403_FORBIDDEN,
            initial_detail={
                "message": "Email already verified",
                "error_code": "email_already_verified",
            },
        ),
    )

    app.add_exception_handler(
        UserLoggedOut,
        create_exception_handler(
            status_code=status.HTTP_403_FORBIDDEN,
            initial_detail={
                "message": "User is logged out",
                "resolution": "You are already logged out. Please log in if you want to",
                "error_code": "user_logged_out",
            },
        ),
    )

    app.add_exception_handler(
        EmailNotVerified,
        create_exception_handler(
            status_code=status.HTTP_403_FORBIDDEN,
            initial_detail={
                "message": "Email not verified",
                "resolution": "Please verify your email and check for verification details",
                "error_code": "email_not_verified",
            },
        ),
    )

    app.add_exception_handler(
        UserNotFound,
        create_exception_handler(
            status_code=status.HTTP_404_NOT_FOUND,
            initial_detail={
                "message": "User not found",
                "resolution": "Please check the user credentials",
                "error_code": "user_not_found",
            },
        ),
    )
    
  
    app.add_exception_handler(
        ResetPasswordFailed,
        create_exception_handler(
            status_code=status.HTTP_400_BAD_REQUEST,
            initial_detail={
                "message": "Reset password failed",
                "resolution": "Please check the reset token and try again",
                "error_code": "reset_password_failed",
            },
        ),
    )

    app.add_exception_handler(
        InvalidCredentials,
        create_exception_handler(
            status_code=status.HTTP_400_BAD_REQUEST,
            initial_detail={
                "message": "Invalid email or password",
                "resolution": "Please check your credentials and try again",
                "error_code": "invalid_credentials",
            },
        ),
    )

    app.add_exception_handler(
        UnAuthenticated,
        create_exception_handler(
            status_code=status.HTTP_401_UNAUTHORIZED,
            initial_detail={
                "message": "User not authenticated.",
                "resolution": "Please request a new token or signin.",
                "error_code": "unauthenticated",
            },
        ),
    )

    app.add_exception_handler(
        InvalidToken,
        create_exception_handler(
            status_code=status.HTTP_401_UNAUTHORIZED,
            initial_detail={
                "message": "Token is invalid or expired",
                "resolution": "Please request a new token",
                "error_code": "invalid_token",
            },
        ),
    )

    app.add_exception_handler(
        RevokedToken,
        create_exception_handler(
            status_code=status.HTTP_401_UNAUTHORIZED,
            initial_detail={
                "message": "Token has been revoked",
                "resolution": "Please request a new token",
                "error_code": "token_revoked",
            },
        ),
    )

    app.add_exception_handler(
        AccessTokenRequired,
        create_exception_handler(
            status_code=status.HTTP_401_UNAUTHORIZED,
            initial_detail={
                "message": "Access token required",
                "resolution": "Please provide a valid access token",
                "error_code": "access_token_required",
            },
        ),
    )

    app.add_exception_handler(
        RefreshTokenRequired,
        create_exception_handler(
            status_code=status.HTTP_403_FORBIDDEN,
            initial_detail={
                "message": "Refresh token required",
                "resolution": "Please provide a valid refresh token",
                "error_code": "refresh_token_required",
            },
        ),
    )

    app.add_exception_handler(
        InsufficientPermission,
        create_exception_handler(
            status_code=status.HTTP_403_FORBIDDEN,
            initial_detail={
                "message": "Insufficient permissions",
                "resolution": "Please check your permissions",
                "error_code": "insufficient_permissions",
            },
        ),
    )

    app.add_exception_handler(
        AccountNotVerified,
        create_exception_handler(
            status_code=status.HTTP_403_FORBIDDEN,
            initial_detail={
                "message": "Account not verified",
                "error_code": "account_not_verified",
                "resolution": "Please check your email for verification details",
            },
        ),
    )

    @app.exception_handler(500)
    async def internal_server_error(request, exc):
        return JSONResponse(
            content={
                "message": "Oops! Something went wrong",
                "resolution": "Please try again later",
                "error_code": "server_error",
            },
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )

    @app.exception_handler(SQLAlchemyError)
    async def database_error(request, exc):
        print(str(exc))
        return JSONResponse(
            content={
                "message": "Database error occurred",
                "resolution": "Please try again later",
                "error_code": "database_error",
            },
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
