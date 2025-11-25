# audio_lookup.py

from fastapi import HTTPException
from src.utils.audio_mappings import HAUSA_AUDIO_MAP, YORUBA_AUDIO_MAP, NAIJA_AUDIO_MAP

def get_audio_filename(language: str) -> str:
    language = language.lower()

    if language == "yoruba":
        return YORUBA_AUDIO_MAP

    if language == "naija":
        return NAIJA_AUDIO_MAP
    
    # if language == "hausa":
    #     return HAUSA_AUDIO_MAP
    
    else:
        raise HTTPException(status_code=404, detail=f"Language '{language}' not supported.")
