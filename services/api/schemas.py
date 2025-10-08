from pydantic import BaseModel, Field, EmailStr
from typing import Optional, List, Dict, Any

class UserCreate(BaseModel):
    username: str = Field(min_length=3, max_length=50)
    password: str = Field(min_length=6, max_length=128)
    email: Optional[EmailStr] = None

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"

class JobCreate(BaseModel):
    accession: str
    reference_fasta: str
    reference_gtf: Optional[str] = None
    models: List[str] = Field(default_factory=lambda: ["cheui", "m6anet"]) 
    analysis: Dict[str, Any] = Field(default_factory=dict)

class JobStatus(BaseModel):
    id: str
    status: str
    result: Optional[Dict[str, Any]] = None
