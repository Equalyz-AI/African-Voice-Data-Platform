from datetime import datetime
from typing import Optional, Any
from pydantic import BaseModel, Field, EmailStr
from sympy import O


class UserCreateModel(BaseModel):
    full_name: str
    email: EmailStr
    password: str

    model_config = {
        "json_schema_extra": {
            "example": {
                "full_name": "John Doe",
                "email": "johndoe123@co.com",
                "password": "testpass123",
            }
        }
    }

class UserLoginModel(BaseModel):
    email: EmailStr
    password: str

    model_config = {
        "json_schema_extra": {
            "example": {
                "email": "johndoe123@co.com",
                "password": "testpass123",
            }
        }
    }

class ForgotPasswordModel(BaseModel):
    email: EmailStr


class ResetPasswordModel(BaseModel):
    password: str

class ResetPasswordSchemaResponseModel(BaseModel):
    status: bool
    message: str

    model_config = {
        "json_schema_extra": {
            "example": {
                "status": True,
                "message": "Password reset email sent successfully."
            }
        }
    }



class FeedbackCreateModel(BaseModel):
    fullname: str
    email: EmailStr
    feedback: str

    model_config = {
        "json_schema_extra" : {
            "example": {
                "fullname": "John Doe",
                "email": "johndoe123@co.com",
                "feedback": "This is a feedback message."
            }
        }
    }


class FeedbackReadModel(BaseModel):
    id: str
    user_id: Optional[str] = None
    fullname: str
    email: EmailStr
    feedback_text: str
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True

class FeedbackResponseModel(BaseModel):
    status: bool
    message: str

    data: Optional[FeedbackReadModel] = None

class UserRead(BaseModel):
    id: str
    full_name: str
    email: EmailStr
    is_verified: bool
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True
        extra = "ignore" 

class UserCreateRead(BaseModel):
    id: str
    full_name: str
    email: EmailStr
    verification_token: Optional[str] = None
    is_verified: bool
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True 

class LoginResponseModel(BaseModel):
    status: bool
    message: str
    data: UserRead

class DeleteResponseModel(BaseModel):
    status: bool
    message: str

class RegisterResponseModel(BaseModel):
    status: bool
    message: str
    data: UserCreateRead


class TokenUser(BaseModel):
    full_name: str
    email: str
    id: str
    is_verified: bool
    access_token: Optional[str] = "Sorry, We can not send the access token in the response"
    token_type: Optional[str] = "bearer"

    class Config:
        from_attributes = True


class VerificationMailSchemaResponse(BaseModel):
    status: bool
    message: str
    verification_token: str

    class Config:
        from_attributes = True

class GooglePayload(BaseModel):
    sub: Optional[Any] = None
    name: str
    email: str
    picture: str
    verification_token: Optional[str] = None
    is_verified: bool

class GetTokenRequest(BaseModel):
    code: str
