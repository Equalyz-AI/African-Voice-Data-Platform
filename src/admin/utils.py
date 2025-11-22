import pandas as pd
import io
from fastapi.responses import Response
from src.download.s3_config import COLUMNS


async def generate_excel_template() -> Response:
    
    df = pd.DataFrame(columns=COLUMNS)
    
    buffer = io.BytesIO()
    df.to_excel(buffer, index=False, engine="openpyxl")
    buffer.seek(0)
    
    headers = {
        "Content-Disposition": 'attachment; filename="upload_template.xlsx"'
    }
    return Response(
        content=buffer.getvalue(),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers
    )
