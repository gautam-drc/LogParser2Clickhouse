from dotenv import load_dotenv, find_dotenv
import os

dotenv_path = find_dotenv()
print(f"Loading from: {dotenv_path}")
load_dotenv(dotenv_path)

print("DATABASE:", os.getenv("DATABASE"))
