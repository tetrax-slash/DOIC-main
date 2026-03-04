import os
import requests
import tarfile
from bs4 import BeautifulSoup
from tqdm import tqdm

BASE_URL = "https://dcapswoz.ict.usc.edu/wwwedaic/data/"

DOWNLOAD_FOLDER = "temp_download"
TRANSCRIPT_FOLDER = "transcript_excels"

START_ID = 460  # 🔥 change starting ID here

os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
os.makedirs(TRANSCRIPT_FOLDER, exist_ok=True)

# ==========================
# STEP 1: SCRAPE FILE LIST
# ==========================

print("Fetching file list...")

response = requests.get(BASE_URL)
response.raise_for_status()

soup = BeautifulSoup(response.text, "html.parser")

p_files = []

for link in soup.find_all("a"):
    href = link.get("href")
    if href and href.endswith("_P.tar.gz"):
        subject_id = int(href.split("_")[0])
        if subject_id >= START_ID:
            p_files.append(href)

print(f"Found {len(p_files)} participant archives from ID {START_ID} onwards.")

# ==========================
# STEP 2: PROCESS EACH FILE
# ==========================

for file_name in p_files:

    archive_url = BASE_URL + file_name
    archive_path = os.path.join(DOWNLOAD_FOLDER, file_name)

    subject_id = file_name.split("_")[0]
    expected_csv = os.path.join(
        TRANSCRIPT_FOLDER,
        f"{subject_id}_TRANSCRIPT.csv"
    )

    # Resume-safe skip
    if os.path.exists(expected_csv):
        print(f"⏭ Skipping {file_name} (already processed)")
        continue

    print(f"\nDownloading: {file_name}")

    try:
        # -----------------------
        # Download
        # -----------------------
        response = requests.get(archive_url, stream=True)
        response.raise_for_status()

        total_size = int(response.headers.get("content-length", 0))

        with open(archive_path, "wb") as f, tqdm(
            total=total_size,
            unit="iB",
            unit_scale=True,
            desc=file_name
        ) as bar:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    bar.update(len(chunk))

        # -----------------------
        # Extract transcript CSV
        # -----------------------
        with tarfile.open(archive_path, "r:gz") as tar:
            for member in tar.getmembers():
                if member.name.upper().endswith("TRANSCRIPT.CSV"):
                    print(f"Extracting {member.name}")
                    tar.extract(member, DOWNLOAD_FOLDER)

                    src = os.path.join(DOWNLOAD_FOLDER, member.name)
                    dst = os.path.join(
                        TRANSCRIPT_FOLDER,
                        os.path.basename(member.name)
                    )

                    if os.path.exists(src):
                        os.replace(src, dst)
                        print(f"✅ Saved: {dst}")
                    break

        # Delete archive immediately
        if os.path.exists(archive_path):
            os.remove(archive_path)

    except Exception as e:
        print(f"⚠️ Error processing {file_name}: {e}")
        if os.path.exists(archive_path):
            os.remove(archive_path)

print("\n✅ Processing complete.")