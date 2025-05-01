from pathlib import Path
import sys
import os
from dotenv import load_dotenv
from typing import Dict

# ----------------------------------------------------------
# Setup: Define project root and load environment variables
# ----------------------------------------------------------

# BASE_DIR points to the root of the project (two levels up from this file)
BASE_DIR: Path = Path(__file__).resolve().parent.parent

# Add BASE_DIR to sys.path to ensure all project modules are importable
sys.path.append(str(BASE_DIR))

# Load environment variables from .env file
load_dotenv()

# ----------------------------------------------------------
# Log File Paths
# ----------------------------------------------------------

LOG_FILES: Dict[str, Path] = {
    "enrollment": BASE_DIR / os.getenv("ENROLLMENT_LOGS", ""),
    "instructor": BASE_DIR / os.getenv("INSTRUCTOR_LOGS", ""),
    "course": BASE_DIR / os.getenv("COURSE_LOGS", ""),
    "login": BASE_DIR / os.getenv("LOGIN_LOGS", ""),
}

# ----------------------------------------------------------
# CSV Output Paths
# ----------------------------------------------------------

CSV_FILES: Dict[str, Path] = {
    "enrollment": BASE_DIR / os.getenv("ENROLLMENT_CSV", ""),
    "instructor": BASE_DIR / os.getenv("INSTRUCTOR_CSV", ""),
    "course": BASE_DIR / os.getenv("COURSE_CSV", ""),
    "login": BASE_DIR / os.getenv("LOGIN_CSV", ""),
}

# ----------------------------------------------------------
# Offset File Paths for Log Processing
# ----------------------------------------------------------

OFFSET_FILES: Dict[str, Path] = {
    "enrollment": BASE_DIR / os.getenv("ENROLLMENT_OFFSET", ""),
    "instructor": BASE_DIR / os.getenv("INSTRUCTOR_OFFSET", ""),
    "course": BASE_DIR / os.getenv("COURSE_OFFSET", ""),
    "login": BASE_DIR / os.getenv("LOGIN_OFFSET", ""),
}

# ----------------------------------------------------------
# Database Configuration
# ----------------------------------------------------------

DATABASE: Dict[str, str] = {
    "host": os.getenv("HOST", "localhost"),
    "port": os.getenv("PORT", "9000"),
    "user": os.getenv("DB_USER", "default"),
    "password": os.getenv("PASSWORD", ""),
    "database": os.getenv("DATABASE", "default_db")
}
