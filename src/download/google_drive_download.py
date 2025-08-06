import gdown
import zipfile
import os

def download_from_gdrive(file_id, dest_path):
    url = f'https://drive.google.com/uc?id={file_id}'
    gdown.download(url, dest_path, quiet=False)

def create_zip_from_drive(samples, zip_path):
    with zipfile.ZipFile(zip_path, 'w') as z:
        for idx, s in enumerate(samples):
            # Assuming s.audio_path is the Google Drive file ID
            local_filename = f"/tmp/{idx+1:04d}_clip.wav"
            download_from_gdrive(s.audio_path, local_filename)

            # Add to ZIP archive
            z.write(local_filename, arcname=f"audio/{idx+1:04d}_clip.wav")

            # Clean up
            os.remove(local_filename)
