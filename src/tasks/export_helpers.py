import io
import datetime
from typing import List, Optional
import pandas as pd
from src.db.models import AudioSample

def generate_metadata_buffer(samples: List[AudioSample], as_excel=True):
    """Create metadata buffer in either Excel or CSV."""
    print(
        f"The samples from the dataset have been selected for metadata export.", 
        f"The metadata will be generated in {'Excel' if as_excel else 'CSV'} format."
        f"The number of samples selected is {len(samples)}"
        f"{samples}"
    )
    df = pd.DataFrame([{
        "speaker_id": s.speaker_id,
        "transcript_id": s.sentence_id,
        "transcript": s.sentence or "",
        "audio_path": f"audio/{s.sentence_id}.wav",
        "gender": s.gender,
        "age_group": s.age_group,
        "edu_level": s.edu_level,
        "durations": s.duration,
        "language": s.language,
        "snr": s.snr,
        "domain": s.domain,
    } for s in samples])

    if as_excel:
        buf = io.BytesIO()
        df.to_excel(buf, index=False)
        return buf, "metadata.xlsx"
    else:
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        return io.BytesIO(buf.getvalue().encode()), "metadata.csv"

def generate_readme(language: str, pct: int, as_excel: bool, num_samples: int, sentence_id: Optional[str]=None) -> str:
    # ... (Your exact generate_readme function from the prompt) ...
    return f"""
        📘 Dataset Export Summary
        =========================
        Language         : {language.upper()}
        Percentage       : {pct}%
        Total Samples    : {num_samples}
        File Format      : {"Excel (.xlsx)" if as_excel else "CSV (.csv)"}
        Date             : {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

        📁 Folder Structure
        ===================
        {language}_{pct}pct_<date>/
        ├── metadata.{"xlsx" if as_excel else "csv"}   - Tabular data with metadata
        ├── README.txt                       - This file
        └── audio/                           - Folder with audio clips
            ├── {sentence_id}.wav
            ├── ...

        📌 Notes
        ========
        - All audio filenames match the metadata rows.
        - Use Excel or CSV-compatible software to open metadata.

        ✅ Contact
        ==========
        For feedback or support, reach out to the dataset team.
        """