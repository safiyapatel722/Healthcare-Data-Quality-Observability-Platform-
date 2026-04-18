from pydantic import BaseSettings, validator


class Settings(BaseSettings):
    gcp_project_id: str
    gcs_bucket_name: str
    google_application_credentials: str
    bq_dataset: str = "covid19_open_data"
    bq_table: str = "covid19_open_data"
    bq_public_project: str = "bigquery-public-data"

    @validator("gcp_project_id", "gcs_bucket_name")
    def not_empty(cls, v):
        if not v:
            raise ValueError("Must not be empty")
        return v

    class Config:
        env_file = ".env"

settings = Settings()