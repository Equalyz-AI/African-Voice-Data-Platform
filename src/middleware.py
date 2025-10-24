from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, Request, HTTPException
from starlette.responses import Response
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from colorlog import ColoredFormatter
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from sqlalchemy.exc import IntegrityError
import time, json, logging, traceback




# Formatter for console
console_formatter = ColoredFormatter(
    "%(log_color)s%(asctime)s - %(levelname)s - %(message)s",
    log_colors={
        'DEBUG': 'cyan',
        'INFO': 'green',
        'WARNING': 'yellow',
        'ERROR': 'red',
        'CRITICAL': 'bold_red',
    },
)

# Formatter for file (no color)
file_formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(message)s"
)

# Console handler (with color)
console_handler = logging.StreamHandler()
console_handler.setFormatter(console_formatter)

# File handler (without color)
file_handler = logging.FileHandler("app.log")
file_handler.setFormatter(file_formatter)

# Logger setup
logger = logging.getLogger("aiforgov.middleware")
logger.setLevel(logging.INFO)
logger.addHandler(console_handler)
logger.addHandler(file_handler)


allowed_origins = [
    "http://localhost:3000",
    "https://ai4governance.onrender.com",
    "https://accounts.google.com",
    "https://ai-for-governance.vercel.app",
    "https://www.genaigov.ai",
    "https://genaigov.ai",
    "https://api.genaigov.ai",
    "https://africanvoices.io",
    "https://main.d1wrs85izeeycs.amplifyapp.com",
]


def register_middleware(app: FastAPI):

     # Middleware to handle exceptions
    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException):
        logger.error(f"HTTPException: {exc.detail} at {request.method} {request.url.path}")
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

    @app.exception_handler(ValidationError)
    async def validation_exception_handler(request: Request, exc: ValidationError):
        # Log full validation errors
        logger.error(f"Validation error at {request.method} {request.url.path}: {exc.errors()}")
        return JSONResponse(status_code=422, content={"detail": exc.errors()})

    @app.exception_handler(IntegrityError)
    async def db_integrity_error_handler(request: Request, exc: IntegrityError):
        # Log full DB error with traceback
        logger.error(f"Database integrity error at {request.method} {request.url.path}: {str(exc)}")
        logger.error(traceback.format_exc())
        return JSONResponse(status_code=400, content={"detail": "Database error occurred"})

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception):
        # Log full traceback for unexpected errors
        logger.error(f"Unhandled exception at {request.method} {request.url.path}: {str(exc)}")
        logger.error(traceback.format_exc())
        return JSONResponse(status_code=500, content={"detail": "Internal Server Error"})



    def get_status_color(status_code: int) -> str:
        if 200 <= status_code < 300:
            return "\033[92m"  # Green
        elif 400 <= status_code < 500:
            return "\033[93m"  # Yellow
        elif 500 <= status_code < 600:
            return "\033[91m"  # Red
        else:
            return "\033[0m"   # Default

    @app.middleware("http")
    async def log_requests(request: Request, call_next):
        start_time = time.time()
        try:
            response = await call_next(request)
        except Exception as exc:
            logger.exception(f"Unhandled error for {request.method} {request.url.path}")
            raise exc
        process_time = time.time() - start_time

        status_color = get_status_color(response.status_code)
        reset_color = "\033[0m"

        log_msg = (
            f"{request.client.host}:{request.client.port} - {request.method} {request.url.path} - "
            f"Status: {status_color}{response.status_code}{reset_color} - Time: {process_time:.2f}s"
        )

        if response.status_code >= 400:
            body = b""
            async for chunk in response.body_iterator:
                body += chunk
            response = Response(
                content=body,
                status_code=response.status_code,
                headers=dict(response.headers),
                media_type=response.media_type,
            )
            try:
                error_content = json.loads(body.decode())
                reason = error_content.get("detail", error_content)
                log_msg += f" - Reason: {reason}"
            except Exception:
                log_msg += f" - Reason: {body.decode(errors='ignore')}"

        logger.info(log_msg)
        return response


    app.add_middleware(CORSMiddleware, allow_origins=allowed_origins, allow_methods=["*"], allow_headers=["*"], allow_credentials=True,)
    from fastapi import FastAPI
from fastapi.middleware.trustedhost import TrustedHostMiddleware

app = FastAPI()

# Add middleware
app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=[
        "localhost",
        "127.0.0.1",
        "0.0.0.0",
        "ai4governance.onrender.com",
        "*.onrender.com",
        "51.20.123.49",
        "ec2-51.20.123.49.eu-north-1.compute.amazonaws.com",
        "51-20-193-205",
        "ec2-51-20-193-205.eu-north-1.compute.amazonaws.com",
        "api.genaigov.ai",
        "genaigov.ai",
        "www.genaigov.ai",
        "africanvoices.io",
        "main.d1wrs85izeeycs.amplifyapp.com"
    ]
)
