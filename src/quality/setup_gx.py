import great_expectations as gx

context = gx.get_context(mode="file", project_root_dir=".")
print("✅ GX context created successfully")
print("GX version:", gx.__version__)