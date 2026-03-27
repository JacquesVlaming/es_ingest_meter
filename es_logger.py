"""
es_logger.py

Shared ECS-formatted Elasticsearch logging handler.
Buffers records and flushes via the bulk API.
Fails silently — a logging failure never crashes the main script.
"""

import json
import logging
import socket
import threading
from datetime import datetime, timezone
from typing import Optional

import requests
from requests.auth import HTTPBasicAuth

_HOSTNAME = socket.gethostname()

_LEVEL_MAP = {
    logging.DEBUG:    "debug",
    logging.INFO:     "info",
    logging.WARNING:  "warn",
    logging.ERROR:    "error",
    logging.CRITICAL: "critical",
}


class ESLoggingHandler(logging.Handler):
    """
    Logging handler that ships records to Elasticsearch as ECS documents.

    Parameters
    ----------
    host        : Elasticsearch base URL
    service     : value for ecs `service.name`
    index       : target index name (default: es-ingest-meter-logs-YYYY.MM.DD)
    username    : basic auth username
    password    : basic auth password
    api_key     : base64-encoded API key
    verify      : TLS verification
    buffer_size : flush after this many records (default: 20)
    """

    def __init__(
        self,
        host: str,
        service: str,
        index: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        api_key: Optional[str] = None,
        verify: bool = True,
        buffer_size: int = 20,
    ):
        super().__init__()
        self._host        = host.rstrip("/")
        self._service     = service
        self._index       = index  # None = date-stamped per flush
        self._verify      = verify
        self._buffer_size = buffer_size
        self._buffer: list[dict] = []
        self._lock        = threading.Lock()

        self._session = requests.Session()
        if username and password:
            self._session.auth = HTTPBasicAuth(username, password)
        if api_key:
            self._session.headers["Authorization"] = f"ApiKey {api_key}"

    # ------------------------------------------------------------------

    def _index_name(self) -> str:
        if self._index:
            return self._index
        today = datetime.now(timezone.utc).strftime("%Y.%m.%d")
        return f"es-ingest-meter-logs-{today}"

    def _to_ecs(self, record: logging.LogRecord) -> dict:
        doc: dict = {
            "@timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "log": {
                "level":  _LEVEL_MAP.get(record.levelno, "info"),
                "logger": record.name,
            },
            "message": record.getMessage(),
            "service": {"name": self._service},
            "host":    {"name": _HOSTNAME},
            "event":   {"dataset": f"{self._service}.log"},
        }

        # Merge only user-supplied `extra` fields — skip all logging internals
        _internal = {
            "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
            "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
            "created", "msecs", "relativeCreated", "thread", "threadName",
            "processName", "process", "taskName", "message", "asctime",
        }
        for key, val in record.__dict__.items():
            if key not in _internal and not key.startswith("_"):
                doc.setdefault("labels", {})[key] = val

        if record.exc_info:
            import traceback
            doc["error"] = {
                "message":    str(record.exc_info[1]),
                "stack_trace": "".join(traceback.format_exception(*record.exc_info)),
            }

        return doc

    # ------------------------------------------------------------------

    def emit(self, record: logging.LogRecord) -> None:
        try:
            doc = self._to_ecs(record)
            with self._lock:
                self._buffer.append(doc)
                if len(self._buffer) >= self._buffer_size:
                    self._flush_locked()
        except Exception:
            pass  # never let logging kill the main script

    def flush(self) -> None:
        with self._lock:
            self._flush_locked()

    def _flush_locked(self) -> None:
        if not self._buffer:
            return
        index = self._index_name()
        meta  = json.dumps({"index": {"_index": index}})
        lines = []
        for doc in self._buffer:
            lines.append(meta)
            lines.append(json.dumps(doc))
        body = "\n".join(lines) + "\n"
        self._buffer.clear()
        try:
            self._session.post(
                f"{self._host}/_bulk",
                data=body,
                headers={"Content-Type": "application/x-ndjson"},
                verify=self._verify,
                timeout=10,
            )
        except Exception:
            pass  # silent — ES being unavailable must not break the tool

    def close(self) -> None:
        self.flush()
        super().close()


# ---------------------------------------------------------------------------
# Convenience factory
# ---------------------------------------------------------------------------

def setup(
    service: str,
    host: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    api_key: Optional[str] = None,
    verify: bool = True,
    level: int = logging.INFO,
) -> logging.Logger:
    """
    Return a logger that writes to both stdout and Elasticsearch.

    Usage:
        log = es_logger.setup("es-ingest-meter", host=args.host, api_key=args.api_key)
        log.info("Run started", extra={"pattern": "*", "window_days": 30})
    """
    logger = logging.getLogger(service)
    logger.setLevel(level)

    if logger.handlers:
        return logger  # already configured (e.g. called twice)

    # stdout handler — plain text, visible in `docker logs`
    stream = logging.StreamHandler()
    stream.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s",
                                          datefmt="%Y-%m-%dT%H:%M:%SZ"))
    logger.addHandler(stream)

    # ES handler — structured ECS documents
    es_handler = ESLoggingHandler(
        host=host,
        service=service,
        username=username,
        password=password,
        api_key=api_key,
        verify=verify,
    )
    logger.addHandler(es_handler)

    return logger
