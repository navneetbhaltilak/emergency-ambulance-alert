from dotenv import load_dotenv
import os

load_dotenv()  # reads the .env file
DATABASE_URL = os.getenv("DATABASE_URL")  # pulls the value out
print(DATABASE_URL)