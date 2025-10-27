# 🗣️ African Voices Dataset Platform

**African Voices** is an open-source platform dedicated to the **distribution of African voice datasets**, with support for **Yoruba**, **Pidgin**, **Igbo**, and **Hausa** languages. Built to accelerate **African language AI development**, the platform enables users to **easily access and download curated, community-contributed audio datasets**.

It features structured metadata, secure AWS S3 integration, and authenticated download workflows—empowering **developers, researchers**, and **organizations** with high-quality, open voice data for **speech technologies** and **natural language processing**.

---

## 💡 Use Case

Empowering **researchers, developers**, and **AI organizations** with curated, community-driven African (Yoruba, Igbo, Pidgin, Hausa) voice data for:

- 🗣️ **Speech Recognition**
- 🌍 **Translation**
- 🧠 **Natural Language Understanding**

---

## 🚀 Key Features

- ✅ Crowd-sourced audio downloads with structured metadata  
- 📁 Secure storage in **AWS S3**  
- 📊 Admin dashboard with real-time statistics and feedback tracking  
- 📦 Dataset slicing and ZIP packaging  
- 🔐 **Token-based authentication**  
- 📥 Download-ready Excel metadata templates  
- 🧠 Modular backend using **FastAPI + SQLModel**  
- 🌍 Extensible to other African languages  

---

## 🗂️ Public Dataset Structure (Example)

Users with download access receive a ZIP file like this:

```
yoruba_dataset.zip
├── metadata.xlsx         # Metadata for each audio
├── README.txt            # Licensing and usage guide
└── audio/
    ├── 0001_sample.wav
    ├── 0002_sample.wav
    └── ...
```
---

## 📁 Project Folder Structure

```
.
├── README.md
├── .env
├── Dockerfile
├── main.py
├── requirements.txt
├── src/
│   ├── admin/             # Admin routes & services
│   ├── auth/              # JWT & OAuth logic
│   ├── db/                # SQLModel schemas & migrations
│   ├── download/          # Dataset slicing + ZIP handling
│   ├── config.py
│   ├── errors.py
│   └── utils/             # S3 & Excel utilities
```

---

## ⚙️ Installation Guide

### 📦 Prerequisites

- ✅ Python **3.10+**
- ✅ **PostgreSQL** or compatible database
- ✅ **AWS S3** bucket for storage

### 🔧 Setup Instructions

```bash
# Clone repository
git clone https://github.com/your-org/yoruba-dataset-platform.git
cd yoruba-dataset-platform

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
```

Fill in `.env` with your credentials:

```env
DATABASE_URL=postgresql://user:password@localhost/db
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_S3_BUCKET_NAME=yoruba-voices-bucket
FRONTEND_URL=http://localhost:3000
```

### ▶️ Run the Server

```bash
uvicorn main:app --reload
```

---

## 🔐 Authentication & Access Control

- JWT Bearer Token for all endpoints  
- Admin routes require elevated privileges  
- OAuth login supported (Google/email)  

---

## ⬇️ Download APIs

| Endpoint | Description |
|---------|-------------|
| `POST /download/request` | Request subset of the dataset by percent |
| `GET /download/progress/{user}` | Monitor ZIP generation progress |
| `GET /download/template` | Download Excel metadata template |

---

## ⬆️ Upload APIs (Admin Only)

| Endpoint | Description |
|---------|-------------|
| `POST /admin/upload` | Upload Excel + audio to S3 |
| `GET /admin/stats` | View real-time usage stats |
| `GET /admin/users` | List all registered users |

---

## 📊 Admin Panel Highlights

- 📈 View user engagement and feedback  
- 🗃️ Upload and manage new Yoruba voice samples  
- 📦 Generate batch ZIPs and reports  
- ✅ Track download requests and performance  

---

## 🧾 Metadata Excel Template

**Required columns**:

```text
transcription  sample_rate  snr  audio_path  gender  language
```

**Sample row**:

```csv
E kaaro, 44100, 35, audio/0001_sample.wav, female, yoruba
```

---

## 🧱 Service Modules Overview

### ✅ Download Service (`src/download/`)

- Slice dataset by percentage  
- Bundle into downloadable ZIP  
- Log and track user requests  

### ✅ Admin Service (`src/admin/`)

- Ingest Excel/audio files into AWS  
- Maintain usage logs and feedback  
- Enable dataset curation  

### ✅ Auth Service (`src/auth/`)

- OAuth login and registration  
- JWT token management  
- Access control by role  

### ✅ Utils

- Excel metadata parser  
- AWS S3 utilities  
- Logging and custom error responses  

---

## 🐳 Docker Usage

```bash
docker build -t yoruba-dataset .
docker run -p 8000:8000 yoruba-dataset
```

---

## ✅ Test Suite

> Testing support with **pytest** and **FastAPI TestClient** *(coming soon)*

---

## 📢 Contribution & Contact

- Built by: **Data Science Nigeria**  
- Purpose: Open-sourcing African voices for inclusive AI  
- Contact: [dev.equalyz.ai]  

---

## ✊🏾 Join the Movement

If you care about African language representation in AI:

- 🤝 **Contribute**
- 📣 **Spread the word**
- 🌍 **Localize for your language**

---

> _“When you speak to a child in their language, you speak to their heart.” — Nelson Mandela_