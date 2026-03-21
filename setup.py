from setuptools import setup, find_packages

setup(
    name="llm-feedback-pipeline",
    version="0.1.0",
    description="GCP LLM Feedback Data Pipeline",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=[
        "pyspark>=3.0.0",
        "google-cloud-storage>=1.42.0",
        "google-cloud-bigquery>=2.20.0",
        "pandas>=1.3.0",
        "pydantic>=1.8.0",
        "pyyaml>=5.4.0",
        "fastapi>=0.68.0",
        "uvicorn>=0.15.0",
    ],
)
