"""Tests for :mod:`parsimony.registry`.

Covers: a valid remote fetch, every fallback trigger (network failure,
non-2xx, malformed JSON, schema mismatch, unsupported ``schema_version``),
the visible warning + ``source`` marker on fallback, a corrupt/missing
bundled snapshot escalating to :class:`RegistryError`, and that the
snapshot actually shipped in this tree (``parsimony/_data/connectors.json``)
is itself valid — the pytest-side half of the release gate described in
``parsimony/_data/README.md``.
"""

from __future__ import annotations

import json

import httpx
import pytest

from parsimony import registry


def _manifest_json(connectors: list[dict[str, object]], *, schema_version: int = 1) -> str:
    return json.dumps(
        {
            "schema_version": schema_version,
            "generated_at": "2026-07-22",
            "connectors": connectors,
        }
    )


_VALID_ROWS = [
    {
        "package": "parsimony-sdmx",
        "provider": "SDMX protocol (ECB, Eurostat, IMF, World Bank)",
        "entry_point": "sdmx",
        "connector_count": 4,
        "keyless": True,
    },
    {
        "package": "parsimony-fred",
        "provider": "FRED (Federal Reserve Economic Data)",
        "entry_point": "fred",
        "connector_count": 2,
        "keyless": False,
    },
]


def _ok_response(body: str) -> httpx.Response:
    return httpx.Response(200, text=body, request=httpx.Request("GET", registry.CANONICAL_MANIFEST_URL))


# ---------------------------------------------------------------------------
# remote success
# ---------------------------------------------------------------------------


def test_list_available_remote_success(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(httpx, "get", lambda url, timeout: _ok_response(_manifest_json(_VALID_ROWS)))

    result = registry.list_available()

    assert result.source == "remote"
    assert result.generated_at == "2026-07-22"
    assert [c.package for c in result.connectors] == ["parsimony-fred", "parsimony-sdmx"]  # sorted
    fred = next(c for c in result.connectors if c.package == "parsimony-fred")
    assert fred.provider == "FRED (Federal Reserve Economic Data)"
    assert fred.entry_point == "fred"
    assert fred.connector_count == 2
    assert fred.keyless is False


def test_list_available_uses_canonical_url_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, object] = {}

    def fake_get(url: str, timeout: float) -> httpx.Response:
        seen["url"] = url
        return _ok_response(_manifest_json(_VALID_ROWS))

    monkeypatch.setattr(httpx, "get", fake_get)

    registry.list_available()

    assert seen["url"] == "https://parsimony.dev/connectors.json" == registry.CANONICAL_MANIFEST_URL


def test_list_available_respects_url_override(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, object] = {}

    def fake_get(url: str, timeout: float) -> httpx.Response:
        seen["url"] = url
        return _ok_response(_manifest_json(_VALID_ROWS))

    monkeypatch.setattr(httpx, "get", fake_get)

    registry.list_available(url="https://example.test/connectors.json")

    assert seen["url"] == "https://example.test/connectors.json"


# ---------------------------------------------------------------------------
# fallback triggers — each falls through to the bundled snapshot
# ---------------------------------------------------------------------------


def test_list_available_falls_back_on_network_error(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def fake_get(url: str, timeout: float) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    monkeypatch.setattr(httpx, "get", fake_get)
    monkeypatch.setattr(
        registry,
        "_load_bundled_manifest",
        lambda: registry._Manifest.model_validate_json(_manifest_json(_VALID_ROWS)),
    )

    with caplog.at_level("WARNING", logger="parsimony.registry"):
        result = registry.list_available()

    assert result.source == "bundled"
    assert len(result.connectors) == 2
    assert any(registry.CANONICAL_MANIFEST_URL in r.getMessage() for r in caplog.records)


def test_list_available_falls_back_on_non_2xx(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_get(url: str, timeout: float) -> httpx.Response:
        return httpx.Response(503, request=httpx.Request("GET", url))

    monkeypatch.setattr(httpx, "get", fake_get)
    monkeypatch.setattr(
        registry,
        "_load_bundled_manifest",
        lambda: registry._Manifest.model_validate_json(_manifest_json(_VALID_ROWS)),
    )

    result = registry.list_available()

    assert result.source == "bundled"


def test_list_available_falls_back_on_malformed_json(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(httpx, "get", lambda url, timeout: _ok_response("not json{{{"))
    monkeypatch.setattr(
        registry,
        "_load_bundled_manifest",
        lambda: registry._Manifest.model_validate_json(_manifest_json(_VALID_ROWS)),
    )

    result = registry.list_available()

    assert result.source == "bundled"


def test_list_available_falls_back_on_schema_mismatch(monkeypatch: pytest.MonkeyPatch) -> None:
    # Old-shape envelope: "providers" key instead of "connectors", no schema_version.
    monkeypatch.setattr(
        httpx, "get", lambda url, timeout: _ok_response(json.dumps({"generated_at": "x", "providers": []}))
    )
    monkeypatch.setattr(
        registry,
        "_load_bundled_manifest",
        lambda: registry._Manifest.model_validate_json(_manifest_json(_VALID_ROWS)),
    )

    result = registry.list_available()

    assert result.source == "bundled"


def test_list_available_falls_back_on_unsupported_schema_version(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(httpx, "get", lambda url, timeout: _ok_response(_manifest_json(_VALID_ROWS, schema_version=2)))
    monkeypatch.setattr(
        registry,
        "_load_bundled_manifest",
        lambda: registry._Manifest.model_validate_json(_manifest_json(_VALID_ROWS)),
    )

    result = registry.list_available()

    assert result.source == "bundled"


def test_list_available_falls_back_on_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_get(url: str, timeout: float) -> httpx.Response:
        raise httpx.ReadTimeout("timed out")

    monkeypatch.setattr(httpx, "get", fake_get)
    monkeypatch.setattr(
        registry,
        "_load_bundled_manifest",
        lambda: registry._Manifest.model_validate_json(_manifest_json(_VALID_ROWS)),
    )

    result = registry.list_available()

    assert result.source == "bundled"


# ---------------------------------------------------------------------------
# both remote AND bundled fail -> RegistryError, preserving both causes
# ---------------------------------------------------------------------------


def test_list_available_raises_registry_error_when_bundled_also_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_get(url: str, timeout: float) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    def fake_bundled() -> registry._Manifest:
        raise FileNotFoundError("no such file")

    monkeypatch.setattr(httpx, "get", fake_get)
    monkeypatch.setattr(registry, "_load_bundled_manifest", fake_bundled)

    with pytest.raises(registry.RegistryError) as excinfo:
        registry.list_available()

    assert isinstance(excinfo.value.remote_error, httpx.ConnectError)
    assert isinstance(excinfo.value.bundled_error, FileNotFoundError)
    assert registry.CANONICAL_MANIFEST_URL in str(excinfo.value)


def test_list_available_raises_registry_error_when_bundled_snapshot_is_corrupt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_get(url: str, timeout: float) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    monkeypatch.setattr(httpx, "get", fake_get)
    monkeypatch.setattr(
        registry, "_load_bundled_manifest", lambda: (_ for _ in ()).throw(ValueError("corrupt snapshot"))
    )

    with pytest.raises(registry.RegistryError) as excinfo:
        registry.list_available()

    assert isinstance(excinfo.value.bundled_error, ValueError)


# ---------------------------------------------------------------------------
# strict schema rejection at the row level
# ---------------------------------------------------------------------------


def test_list_available_rejects_row_missing_required_field(monkeypatch: pytest.MonkeyPatch) -> None:
    bad_rows = [{"package": "parsimony-x", "provider": "X", "entry_point": "x"}]  # missing connector_count/keyless
    monkeypatch.setattr(httpx, "get", lambda url, timeout: _ok_response(_manifest_json(bad_rows)))
    monkeypatch.setattr(
        registry,
        "_load_bundled_manifest",
        lambda: registry._Manifest.model_validate_json(_manifest_json(_VALID_ROWS)),
    )

    result = registry.list_available()

    # Falls back rather than propagating the pydantic ValidationError.
    assert result.source == "bundled"


# ---------------------------------------------------------------------------
# the shipped bundled snapshot itself
# ---------------------------------------------------------------------------


def test_bundled_manifest_is_valid() -> None:
    """The real ``parsimony/_data/connectors.json`` in this tree parses under the current schema.

    The pytest-side half of the release gate in ``parsimony/_data/README.md``
    — ``verify-bundled-manifest`` in CI checks it matches upstream; this
    checks it is at least well-formed and non-empty on every test run.
    """
    manifest = registry._load_bundled_manifest()

    assert manifest.schema_version == registry._MANIFEST_SCHEMA_VERSION
    assert manifest.connectors
    packages = [c.package for c in manifest.connectors]
    assert len(packages) == len(set(packages)), "duplicate package in bundled manifest"


def test_list_available_bundled_fallback_end_to_end(monkeypatch: pytest.MonkeyPatch) -> None:
    """No mocking of ``_load_bundled_manifest`` — exercises the real shipped file."""

    def fake_get(url: str, timeout: float) -> httpx.Response:
        raise httpx.ConnectError("network disabled for this test")

    monkeypatch.setattr(httpx, "get", fake_get)

    result = registry.list_available()

    assert result.source == "bundled"
    assert result.connectors
