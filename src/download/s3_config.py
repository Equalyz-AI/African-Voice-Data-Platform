import boto3
from dotenv import load_dotenv
from botocore.client import Config
from src import config
from src.config import settings
from typing import Optional
from urllib.parse import urlparse
import hmac, hashlib, base64, time, urllib.parse

load_dotenv()


s3_aws = boto3.client(
    's3',
    aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
    region_name=settings.AWS_REGION,
    endpoint_url=settings.AWS_ENDPOINT_URL,
    config=Config(
        s3={'addressing_style': 'path'},
        max_pool_connections=50
    )
)


s3_obs = boto3.client(
    "s3",
    aws_access_key_id=settings.OBS_ACCESS_KEY_ID,
    aws_secret_access_key=settings.OBS_SECRET_ACCESS_KEY,
    endpoint_url="https://obsv3.cn-global-1.gbbcloud.com", 
    region_name="cn-global-1",
    config=Config(
        s3={'addressing_style': 'virtual'},
        signature_version='s3v4',
        max_pool_connections=50
    ),

)

BUCKET_OBS = settings.OBS_BUCKET_NAME
BUCKET_AWS = settings.S3_BUCKET_NAME
print(f"Using bucket: {BUCKET_OBS}")


SUPPORTED_LANGUAGES = {"Naija", "Yoruba", "Hausa", "Igbo"}
COLUMNS=[
    "transcript", "sample_rate", "snr",
    "transcript_id", "speaker_id", "category",
    "audio_path", "language", "gender", "duration", "age"
]
VALID_PERCENTAGES = {5, 20, 40, 50, 60, 80, 100}
VALID_CATEGORIES = {"read", "spontaneous", "read_with_spontaneous"}



def create_presigned_url(audio_path: str, expiration: int = 3600, bucket: str = BUCKET_OBS) -> str:
    try:
        response = s3_obs.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": audio_path},
            ExpiresIn=expiration
        )
        return response
    except Exception as e:
        raise Exception(f"Failed to generate URL: {e}")



def generate_obs_signed_url(language: str, category: str, filename: str, storage_link: Optional[str] = None, expiration: int = 3600) -> str:
    """
    Generate a signed OBS URL for a specific file.
    
    Parameters:
        language: e.g. "naija", "yoruba", "igbo"
        category: e.g. "read", "spontaneous", "read_with_spontaneous"
        filename: e.g. "pcm_AG_001_000002_R2.wav"
        expiration: time in seconds for URL expiry (default 3600)
    
    Returns:
        Fully signed OBS URL that matches OBS Share link format.
    """
    bucket = "dsn"  # your OBS bucket
    access_key = settings.OBS_ACCESS_KEY_ID
    secret_key = settings.OBS_SECRET_ACCESS_KEY

    
    from src.tasks.export_worker import map_category_to_folder

    print(f"This is the category and the language from generate_obs_signed_url: {category}, {language}\n\n\n")

    folder = map_category_to_folder(language, category)
    key = f"{language}-test/{folder}/{filename}"

    # Expiry timestamp (Unix time)
    expires = int(time.time()) + expiration

    # String to sign (OBS uses AWS Signature V2 style)
    string_to_sign = f"GET\n\n\n{expires}\n/{bucket}/{key}"

    # Compute HMAC-SHA1 signature
    signature_bytes = hmac.new(secret_key.encode("utf-8"),
                               string_to_sign.encode("utf-8"),
                               hashlib.sha1).digest()
    signature = base64.b64encode(signature_bytes).decode("utf-8")

    # URL encode signature properly
    signature_enc = urllib.parse.quote(signature, safe='')

    # Build final URL (OBS Share format)
    url = f"https://{bucket}.obsv3.cn-global-1.gbbcloud.com:443/{key}?AccessKeyId={access_key}&Expires={expires}&Signature={signature_enc}"
    
    return url




def map_sentence_id_to_transcript_obs(sentence_id: str, language: Optional[str] = None, category: Optional[str] = None, sentence: Optional[str] = None) -> str:
    if category.lower() in ["spontaneous"]:
        transcript_url_obs = generate_obs_signed_url(
            language=language.lower(),
            category=category + "_transcripts",
            filename=f"{sentence_id}.docx",
        )
        print("This is the transcript URL for OBS:", transcript_url_obs)
        print("The category is from map_sentence_id_to_transcript_obs:", category)
        return transcript_url_obs
    return sentence
