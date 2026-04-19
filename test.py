# from google.cloud import storage
# from google.oauth2 import service_account

# creds = service_account.Credentials.from_service_account_file(
#     r"F:\self learning DE\Healthcare-Data-Quality-Observability-Platform-\covid-data-observability-a7e8265daeba.json"
# )
# client = storage.Client(credentials=creds)
# print("Verified project:", client.project)

from config.settings import settings
print(settings.gcp_project_id)
print(settings.gcs_bucket_name)