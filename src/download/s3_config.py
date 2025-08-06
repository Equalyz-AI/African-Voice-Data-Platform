import boto3
from dotenv import load_dotenv
from src.config import settings

load_dotenv()


s3 = boto3.client(
    's3',
    aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
    region_name=settings.AWS_REGION,
    endpoint_url=settings.AWS_ENDPOINT_URL
)

BUCKET = settings.S3_BUCKET_NAME


SUPPORTED_LANGUAGES = {"Naija", "Yoruba", "Hausa", "Igbo"}
#  "yoruba", "hausa", "pidgin", "igbo"

COLUMNS=[
    "transcript", "sample_rate", "snr",
    "transcript_id", "speaker_id", "category",
    "audio_path", "language", "gender", "duration", "age"
]

VALID_PERCENTAGES = {5, 20, 40, 50, 60, 80, 100}
VALID_CATEGORIES = {"read", "spontaneous", "read_as_spontaneous"}



def create_presigned_url(audio_path: str, expiration: int = 3600) -> str:
    try:
        response = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": BUCKET, "Key": audio_path},
            ExpiresIn=expiration
        )
        return response
    except Exception as e:
        raise Exception(f"Failed to generate URL: {e}")
