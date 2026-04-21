"""Ensure HTTP source logging never emits credential query parameters."""

from parsimony.transport import _redact_params_for_logging


def test_redact_api_key_and_apikey() -> None:
    out = _redact_params_for_logging(
        {
            "series_id": "UNRATE",
            "api_key": "super-secret",
            "apikey": "other-secret",
            "file_type": "json",
        }
    )
    assert out["series_id"] == "UNRATE"
    assert out["file_type"] == "json"
    assert out["api_key"] == "***REDACTED***"
    assert out["apikey"] == "***REDACTED***"
    assert "super-secret" not in str(out)
    assert "other-secret" not in str(out)


def test_redact_hyphenated_and_token_suffix() -> None:
    out = _redact_params_for_logging({"api-key": "x", "foo_token": "y", "dataset_key": "ECB-YC"})
    assert out["api-key"] == "***REDACTED***"
    assert out["foo_token"] == "***REDACTED***"
    assert out["dataset_key"] == "ECB-YC"
