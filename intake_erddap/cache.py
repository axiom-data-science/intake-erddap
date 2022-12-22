"""Caching support."""
import gzip
import hashlib
import json
import time

from pathlib import Path
from typing import Any, Optional, Type, Union

import appdirs
import pandas as pd
import requests


class CacheStore:
    """A caching mechanism to store HTTP responses in a local cache."""

    def __init__(
        self,
        cache_dir: Optional[Path] = None,
        http_client: Optional[Type] = None,
        cache_period: Optional[Union[int, float]] = None,
    ):
        self.cache_dir: Path = cache_dir or Path(
            appdirs.user_cache_dir("intake-erddap", "axds")
        )
        self.http_client = http_client or requests
        if cache_period is not None:
            self.cache_period = cache_period
        else:
            self.cache_period = 500.0

        if not self.cache_dir.exists():
            self.cache_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def hash_url(url: str) -> str:
        """Returns the hash of the URL"""
        return hashlib.sha256(url.encode("utf-8")).hexdigest()

    def cache_file(self, url: str) -> Path:
        """Return the path to the cache file."""
        checksum = self.hash_url(url)
        filename = self.cache_dir / f"{checksum}.gz"
        return filename

    def cache_response(self, url: str, *args, **kwargs):
        """Write the content of the HTTP response to a gzipped cached file."""
        filename = self.cache_file(url)
        with gzip.open(filename, "wb") as f:
            resp = self.http_client.get(url, *args, **kwargs)
            resp.raise_for_status()
            f.write(resp.content)

    def cache_enabled(self) -> bool:
        """Returns true if the store should use the cache."""
        return self.cache_period > 0

    def read_csv(
        self,
        url: str,
        pandas_kwargs: Optional[dict] = None,
        http_kwargs: Optional[dict] = None,
    ) -> pd.DataFrame:
        """Return a pandas data frame read from source or cache."""
        pandas_kwargs = pandas_kwargs or {}
        http_kwargs = http_kwargs or {}
        if not self.cache_enabled():
            return pd.read_csv(url, **pandas_kwargs)
        pth = self.cache_file(url)
        now = time.time()
        allowed_mtime = now - self.cache_period
        if pth.exists():
            if pth.stat().st_mtime < allowed_mtime:
                self.cache_response(url, **http_kwargs)
        else:
            self.cache_response(url, **http_kwargs)

        with gzip.open(pth) as f:
            return pd.read_csv(f, **pandas_kwargs)

    def read_json(self, url: str, http_kwargs: Optional[dict] = None) -> Any:
        """Return the parsed JSON object from source or cache."""
        http_kwargs = http_kwargs or {}
        if not self.cache_enabled():
            resp = self.http_client.get(url, **http_kwargs)
            resp.raise_for_status()
            return resp.json()
        pth = self.cache_file(url)
        now = time.time()
        allowed_mtime = now - self.cache_period
        if pth.exists():
            if pth.stat().st_mtime < allowed_mtime:
                self.cache_response(url, **http_kwargs)
        else:
            self.cache_response(url, **http_kwargs)

        with gzip.open(pth) as f:
            return json.load(f)

    def clear_cache(self, mtime: Optional[Union[int, float]] = None):
        """Removes all cached files."""
        if self.cache_dir.exists():
            if mtime is None:
                self._clear_cache()
            else:
                self._clear_cache_mtime(mtime)

    def _clear_cache(self):
        """Removes all cached files."""
        for cache_file in self.cache_dir.glob("*.gz"):
            cache_file.unlink()

    def _clear_cache_mtime(self, age: Union[int, float]):
        """Removes cached files older than ``age`` seconds."""
        current_time = time.time()
        cutoff = current_time - age
        for cache_file in self.cache_dir.glob("*.gz"):
            mtime = cache_file.stat().st_mtime
            if mtime <= cutoff:
                cache_file.unlink()
