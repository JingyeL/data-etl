from pydantic import BaseModel, Field
from typing import Optional


class DBSecret(BaseModel):
    username: str = Field(..., description="Database username")
    password: str = Field(..., description="Database password")
    engine: Optional[str] = Field(None, description="Database engine")
    host: str = Field(..., description="Database host")
    port: int = Field(..., description="Database port")
    dbname: str = Field(..., description="Database name")
    dbInstanceIdentifier: Optional[str] = Field(None, description="Database instance identifier")

    class Config:
        title = "Secret"
        description = "Database connection secret"
        extra = "forbid"
