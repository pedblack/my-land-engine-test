import os

os.environ.setdefault("GOOGLE_API_KEY", "test")

import pandas as pd
import pytest

# Ensure the genai client has an API key during import so it doesn't raise
from backbone_crawler import P4NScraper


def make_row(pid, ts):
    return {
        "p4n_id": str(pid),
        "title": f"place-{pid}",
        "last_scraped": ts,
    }


def test_upsert_and_save_normal(tmp_path):
    out = tmp_path / "out.csv"

    # existing CSV with one row
    existing = pd.DataFrame([make_row(100, "2026-01-01 00:00:00")])
    existing.to_csv(out, index=False)

    scraper = P4NScraper(is_dev=True)
    # point scraper at our test file and inject existing_df
    scraper.csv_file = str(out)
    scraper.existing_df = pd.read_csv(scraper.csv_file)

    # processed batch contains a new row
    scraper.processed_batch = [make_row(200, "2026-01-22 00:00:00")]

    # run save
    scraper._upsert_and_save()

    df = pd.read_csv(out)
    ids = set(df["p4n_id"].astype(str).tolist())
    assert "100" in ids and "200" in ids


def test_upsert_fallback_on_concat_error(tmp_path, monkeypatch):
    out = tmp_path / "out2.csv"

    # start without existing CSV
    scraper = P4NScraper(is_dev=True)
    scraper.csv_file = str(out)
    scraper.existing_df = pd.DataFrame()

    scraper.processed_batch = [make_row(300, "2026-01-22 01:00:00")]

    # force pd.concat to raise so main path fails and fallback append is used
    def fake_concat(*args, **kwargs):
        raise RuntimeError("forced concat error")

    monkeypatch.setattr(pd, "concat", fake_concat)

    # call save; should not raise
    scraper._upsert_and_save()

    # fallback should have created the file with our row
    assert out.exists()
    df = pd.read_csv(out)
    assert "300" in df["p4n_id"].astype(str).tolist()
    assert "300" in df["p4n_id"].astype(str).tolist()
    assert "300" in df["p4n_id"].astype(str).tolist()
