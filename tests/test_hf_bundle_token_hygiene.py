"""Hunt R5: client store code must never touch HF token env vars.

Uses an AST walk (not substring grep) so tricks like string concatenation
``"HF_" "TOKEN"`` or comments can't pass a substring-only check:

- asserts no ``os.environ[...]`` / ``os.environ.get(...)`` / ``os.getenv(...)``
  call references a name containing ``TOKEN`` or ``AUTH``
- asserts every ``snapshot_download(...)`` call in the store has
  ``token=False`` as a literal keyword argument

We also validate the redactor on realistic token-shaped inputs.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

STORE_PATH = Path(__file__).resolve().parent.parent / "parsimony" / "stores" / "hf_bundle" / "store.py"


def _env_target_name(node: ast.AST) -> str | None:
    """Return the string argument to an os.environ/os.getenv access, or None."""
    if isinstance(node, ast.Subscript) and _is_os_environ(node.value):
        sl = node.slice
        if isinstance(sl, ast.Constant) and isinstance(sl.value, str):
            return sl.value
    if isinstance(node, ast.Call):
        fn = node.func
        if (
            isinstance(fn, ast.Attribute)
            and fn.attr in {"get", "pop"}
            and _is_os_environ(fn.value)
            and node.args
            and isinstance(node.args[0], ast.Constant)
        ):
            v = node.args[0].value
            return v if isinstance(v, str) else None
        if (
            isinstance(fn, ast.Attribute)
            and fn.attr == "getenv"
            and isinstance(fn.value, ast.Name)
            and fn.value.id == "os"
            and node.args
            and isinstance(node.args[0], ast.Constant)
        ):
            v = node.args[0].value
            return v if isinstance(v, str) else None
    return None


def _is_os_environ(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Attribute)
        and node.attr == "environ"
        and isinstance(node.value, ast.Name)
        and node.value.id == "os"
    )


def test_store_does_not_read_token_env_vars():
    tree = ast.parse(STORE_PATH.read_text(encoding="utf-8"))
    forbidden_substrings = ("TOKEN", "AUTH")
    offending: list[str] = []
    for node in ast.walk(tree):
        name = _env_target_name(node)
        if name is None:
            continue
        upper = name.upper()
        for needle in forbidden_substrings:
            if needle in upper:
                offending.append(name)
    assert not offending, (
        f"{STORE_PATH.name} reads token-like env vars {offending!r}; the client store path uses anonymous access only."
    )


def test_store_passes_token_false_to_snapshot_download():
    tree = ast.parse(STORE_PATH.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        func_name = None
        if isinstance(func, ast.Name):
            func_name = func.id
        elif isinstance(func, ast.Attribute):
            func_name = func.attr
        if func_name != "snapshot_download":
            continue
        kwargs = {kw.arg: kw.value for kw in node.keywords}
        tok = kwargs.get("token")
        assert isinstance(tok, ast.Constant) and tok.value is False, (
            "snapshot_download(...) must pass token=False as a literal kwarg"
        )


def test_redactor_masks_bearer_token_and_hf_prefix():
    from parsimony.stores.hf_bundle.builder import _scrub_token

    raw = (
        "POST https://api.example.com with Authorization: Bearer hf_abc123def4567890ZZZZ "
        "and x-api-key: secret123456 failed"
    )
    redacted = _scrub_token(raw, "")
    # Every token-shape marker must be gone.
    assert "hf_abc123def4567890ZZZZ" not in redacted
    assert not re.search(r"Bearer\s+\S", redacted, re.IGNORECASE)
    assert not re.search(r"Authorization:\s*\S", redacted, re.IGNORECASE)


def test_format_exc_chain_walks_cause_and_notes():
    from parsimony.stores.hf_bundle.builder import _format_exc_chain, _scrub_token

    try:
        try:
            raise RuntimeError("inner with Bearer hf_secret1234567890ABCD xxx")
        except RuntimeError as inner:
            raise ValueError("outer wrap") from inner
    except ValueError as exc:
        exc.add_note("note with Authorization: Bearer hf_anothertokenXXXXXXXX")
        text = _format_exc_chain(exc)
        red = _scrub_token(text, "")
        assert "hf_secret1234567890ABCD" not in red
        assert "hf_anothertokenXXXXXXXX" not in red
