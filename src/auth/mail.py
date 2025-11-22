import resend
from src.config import settings


resend.api_key = settings.RESEND_API_KEY 

def send_verification_email(to_email: str, name: str, verification_token: str):
    verification_link = f"{settings.FRONTEND_URL}/auth/verify-email?token={verification_token}"

    params: resend.Emails.SendParams = {
        "from": f"African Voices <noreply@genaigov.ai>",
        "to": [to_email],
        "subject": "Verify Your Email Address",
        "html": f"""
            <html>
                <body>
                    <h2>Welcome to the African Voices Platform</h2>
                    <h4>Email Verification</h4>
                    <p>Hello {name}, Click the link below to verify your email address:</p>
                    <a href="{verification_link}">Verify Email</a>
                    <p>If you did not request this, please ignore this email.</p>
                </body>
            </html>
        """
    }

    try:
        email = resend.Emails.send(params)
        print(email)
    except Exception as e:
        print(f"Error sending verification email: {e}")

def send_reset_password_email(to_email: str, reset_token: str):
    reset_link = f"{settings.FRONTEND_URL}/auth/reset-password?token={reset_token}"

    params: resend.Emails.SendParams = {
        "from": f"African Voices <oreply@genaigov.ai>",
        "to": [to_email],
        "subject": "Reset Your Password",
        "html": f"""
            <html>
                <body>
                    <h2>Password Reset Request</h2>
                    <p>We received a request to reset your password. Click the link below to continue:</p>
                    <a href="{reset_link}">Reset Password</a>
                    <p>This link will expire after a short period.</p>
                    <p>If you didn’t request a password reset, you can safely ignore this email.</p>
                </body>
            </html>
        """
    }

    try:
        email = resend.Emails.send(params)
        print(email)
    except Exception as e:
        print(f"Error sending password reset email: {e}")
