"""window.apiBaseUrl on the Power Flow page.

It is the path ``{PROXY_BASE_URL}/api``, which the browser resolves against
the page's own scheme, host and port. Forwarded headers must not change it:
when a proxy rewrote X-Forwarded-Proto (Cloudflare Tunnel -> nginx with
``$scheme``), an absolute base built from them was http:// on an https://
page and every data call was blocked as mixed content (#140).
"""
import re

import pytest
from fastapi.testclient import TestClient

import app.main as main_mod
from app.main import app

_API_BASE_RE = re.compile(r'window\.apiBaseUrl = "([^"]*)"')

# What reverse proxies forward, including headers that don't match what the
# browser used (the cases an absolute base got wrong).
FORWARDED = {
    "none": {},
    "nginx": {
        "X-Forwarded-Proto": "https",
        "X-Forwarded-Host": "pw.example.net:8443",
        "X-Forwarded-Port": "8443",
    },
    "proto-rewritten-to-http": {
        "X-Forwarded-Proto": "http",
        "X-Forwarded-Host": "pw.example.net",
        "X-Forwarded-Port": "80",
    },
    "host-without-port": {
        "X-Forwarded-Proto": "http",
        "X-Forwarded-Host": "pw.example.net",
    },
    "internal-host": {
        "X-Forwarded-Proto": "https",
        "X-Forwarded-Host": "pypowerwall:8675",
    },
}


def _page(monkeypatch, proxy_base, headers=None):
    monkeypatch.setattr(main_mod, "_proxy_base", proxy_base)
    client = TestClient(app, base_url="http://localhost:8675")
    resp = client.get("/", headers=headers or {})
    assert resp.status_code == 200
    return resp.text


@pytest.mark.parametrize("proxy_base", ["", "/pypowerwall"])
@pytest.mark.parametrize("headers", FORWARDED.values(), ids=FORWARDED.keys())
def test_api_base_is_a_path_whatever_the_proxy_forwards(
    monkeypatch, proxy_base, headers
):
    html = _page(monkeypatch, proxy_base, headers)
    assert _API_BASE_RE.search(html).group(1) == f"{proxy_base}/api"
    # Nothing from the forwarded headers or the backend address reaches the page
    for value in ("pw.example.net", "pypowerwall:8675", "localhost:8675"):
        assert value not in html


def test_other_prefixed_values_unchanged(monkeypatch):
    html = _page(monkeypatch, "/pypowerwall")
    assert 'window.appPrefix = "/pypowerwall/static/powerflow/"' in html
    assert 'var _BASE = "/pypowerwall"' in html  # fetch prefix wrapper
    assert "'/pypowerwall/console'" in html  # top-window redirect
    assert "{API_BASE_URL}" not in html and "{PROXY_BASE" not in html

    html = _page(monkeypatch, "")
    assert 'window.appPrefix = "/static/powerflow/"' in html
    assert "var _BASE" not in html  # no wrapper without a prefix
