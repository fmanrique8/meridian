from pathlib import Path

from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project


class TestKedroRun:
    def test_kedro_run_default_pipeline(self, monkeypatch):
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test-key")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
        monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-2")
        monkeypatch.setenv("S3_BUCKET_NAME", "meridian-test")
        monkeypatch.setenv("FRED_API_KEY", "fred-test-key")

        project_path = Path(__file__).resolve().parents[1]
        bootstrap_project(project_path)

        with KedroSession.create(project_path=project_path) as session:
            session.run()
