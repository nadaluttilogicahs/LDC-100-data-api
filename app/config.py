import os
from pydantic import BaseModel, Field
from dotenv import load_dotenv

load_dotenv()

class Settings(BaseModel):
    data_base_dir: str = Field(default=os.getenv("DATA_BASE_DIR", "/home/lg58/LDC-100/data"))
    api_key: str = Field(default=os.getenv("API_KEY", "ldc-100-secret-key"))
    host: str = Field(default=os.getenv("HOST", "0.0.0.0"))
    port: int = Field(default=int(os.getenv("PORT", "8000")))

settings = Settings()
