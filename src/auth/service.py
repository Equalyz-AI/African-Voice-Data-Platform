from sqlmodel import select
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Any, Optional
from sqlmodel import select
from src.db.models import User, Feedback
from src.errors import (
    UserAlreadyExists,
    InvalidCredentials,
    EmailAlreadyVerified,
    EmailNotVerified,
    UserNotFound,
    InvalidToken,
)
from .schemas import (
    UserCreateModel,
    VerificationMailSchemaResponse,
    FeedbackCreateModel,
    TokenUser,
    FeedbackResponseModel,
    ResetPasswordSchemaResponseModel,
    ResetPasswordModel,
    ForgotPasswordModel, 
)
from .utils import generate_passwd_hash, verify_password
from .mail import send_verification_email, send_reset_password_email
import uuid


class UserService:
    async def get_user_by_email(
        self, email: str, session: AsyncSession
    ) -> Optional[User]:
        """Retrieve a user by their email address."""
        statement = select(User).where(User.email == email)
        user = await session.execute(statement)
        user = user.scalar_one_or_none()
        if not user:
            print("This is the error of not finding the user")
            return None
        return user
     
    async def user_exists(self, email: str, session: AsyncSession) -> bool:
        """Check if a user with the given email already exists."""
        user = await self.get_user_by_email(email, session)
        return user is not None

    async def create_user(
        self,
        user_data: UserCreateModel,
        session: AsyncSession,
        is_google: Optional[bool] = False,
    ):
        """Create a new user in the database."""
        if await self.user_exists(user_data.email, session):
            raise UserAlreadyExists(
                message="A user with this email already exists."
            )
        # if is_google else False,
        print("The data coming in: ", user_data)

        try:
            verification_token = str(uuid.uuid4())
            hash_password = generate_passwd_hash(user_data.password)

            new_user = User(
                full_name=user_data.full_name,
                email=user_data.email,
                password=hash_password,
                is_verified=False if not is_google else True,
                verification_token = verification_token if not is_google else None,
            )

            session.add(new_user)
            await session.commit()

            if not is_google:
                send_verification_email(new_user.email, str(user_data.full_name), verification_token)

            return new_user
        

        except Exception as e:
            await session.rollback()
            raise e

    async def verify_token(self, token: str, session: AsyncSession) -> User:
        """Verify the token and retrieve the associated user."""
        if token is None:
            raise InvalidToken(
                message="The token is invalid. Please try again."
            )
        
        result = await session.execute(
            select(User).where(User.verification_token == token)
        )

        user = result.scalars().first()
        if not user:
            raise UserNotFound(
                message="The user with this token does not exist"
            )
        return user

    async def authenticate_user(
        self, 
        email: str, 
        password: Optional[str], 
        session: AsyncSession
    ) -> User:
        """Authenticate a user by email and password."""
        user = await self.get_user_by_email(email, session)
        if user is None:
            print("The user is not found")
            raise UserNotFound(
                message="The user with this email does not exist"
            )
        if not verify_password(password, user.password):
            print("The password is not correct")
            raise InvalidCredentials(
                message="The email or password is not correct"
            )
        if not user.is_verified:
            raise EmailNotVerified(
                message="The email is not verified"
            )
        await session.refresh(user)
        return user

    async def update_user(
        self, user: User, user_data: dict, session: AsyncSession
    ) -> User:
        """Update a user's information in the database."""
        for k, v in user_data.items():
            setattr(user, k, v)
        session.add(user)
        await session.commit()
        await session.refresh(user)
        return user

    # reset password
    async def forgot_password(
        self, email: ForgotPasswordModel, session: AsyncSession
    ) -> ResetPasswordSchemaResponseModel:
        """Initiate a password reset process by sending an email with a reset link."""
        user = await self.get_user_by_email(email.email, session)
        if not user:
            raise InvalidCredentials(
                message="The email or password is not correct."
            )

        # Generate and assign a new reset token
        reset_token = str(uuid.uuid4())
        user.verification_token = reset_token

        session.add(user)
        await session.commit()
        await session.refresh(user)

        # Send the reset password email
        send_reset_password_email(user.email, reset_token)

        return ResetPasswordSchemaResponseModel(
            status=True, message="A password reset link has been sent to your email."
        )

    async def reset_password(
        self, user: User, payload: ResetPasswordModel, session: AsyncSession
    ) -> ResetPasswordSchemaResponseModel:
        """Reset the user's password"""
        # Update the user's password
        user.password = generate_passwd_hash(payload.password)
        user.verification_token = None
        session.add(user)
        await session.commit()
        await session.refresh(user)

        return ResetPasswordSchemaResponseModel(
            status=True, message="Password reset successfully."
        )

    #  Feedback from users
    async def create_feedback(
        self,
        feedback_data: FeedbackCreateModel,
        session: AsyncSession,
    ) -> FeedbackResponseModel:
        """Create a new feedback entry in the database."""
        try:
            new_feedback = Feedback(
                fullname=feedback_data.fullname,
                email=feedback_data.email,
                feedback_text=feedback_data.feedback,
            )

            session.add(new_feedback)
            await session.commit()
            await session.refresh(new_feedback)
            return new_feedback

        except Exception as e:
            await session.rollback()
            raise e

    async def delete_user(self, user: TokenUser, session: AsyncSession) -> None:
        """Delete a user from the database."""
        statement = select(User).where(User.id == user.id)
        result = await session.execute(statement)
        db_user = result.scalar_one_or_none()

        if db_user:
            await session.delete(db_user)
            await session.commit()
            return None
        else:
            raise UserNotFound(
                message="User not found"
            )

    # resend verification email
    async def resend_verification_email(
        self, email: str, session: AsyncSession
    ) -> VerificationMailSchemaResponse:
        """Resend the verification email to the user."""
        try:
            user = await self.get_user_by_email(email, session)
            if user is None:
                raise UserNotFound(
                    message="The user with this email does not exist"
                )
            if user:
                if user.is_verified:
                    raise EmailAlreadyVerified(
                        message="The email is already verified."
                    )
                verification_token = str(uuid.uuid4())
                user.verification_token = verification_token
                session.add(user)
                await session.commit()
                send_verification_email(user.email, user.full_name, verification_token)

            return VerificationMailSchemaResponse(
                status=True,
                message="Verification email sent successfully",
                verification_token=verification_token,
            )
        except Exception as e:
            await session.rollback()
            raise e
