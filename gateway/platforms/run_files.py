"""Bounded, session-owned files for the Hermes Runs API.

This is deliberately a narrow control-plane store, not a general file API:
only allowlisted text/image inputs may reach an agent, and only files written
under the per-run artifact directory can later be downloaded.
"""
from __future__ import annotations

import base64
import hashlib
import mimetypes
import os
import re
import shutil
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

MAX_UPLOAD_BYTES = 8 * 1024 * 1024
MAX_TEXT_ATTACHMENT_BYTES = 512 * 1024
MAX_ATTACHMENTS_PER_RUN = 8
MAX_ARTIFACT_BYTES = 25 * 1024 * 1024
MAX_ARTIFACT_FILES = 20
UPLOAD_TTL_SECONDS = 60 * 60
ARTIFACT_TTL_SECONDS = 24 * 60 * 60

_IMAGE_TYPES = {
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".webp": "image/webp",
    ".gif": "image/gif",
}
_TEXT_TYPES = {
    ".txt": "text/plain", ".md": "text/markdown", ".csv": "text/csv",
    ".json": "application/json", ".yaml": "text/yaml", ".yml": "text/yaml",
    ".py": "text/x-python", ".js": "text/javascript", ".ts": "text/typescript",
    ".tsx": "text/typescript", ".jsx": "text/javascript", ".html": "text/html",
    ".htm": "text/html", ".css": "text/css", ".xml": "application/xml",
    ".log": "text/plain", ".ini": "text/plain", ".toml": "text/plain",
}
_SAFE_NAME = re.compile(r"[^A-Za-z0-9._ -]+")
_SAFE_ID = re.compile(r"^(?:file|artifact)_[a-f0-9]{32}$")
_SAFE_RUN_ID = re.compile(r"^run_[a-f0-9]{32}$")


class RunFileError(ValueError):
    def __init__(self, message: str, code: str = "invalid_file", status: int = 400):
        super().__init__(message)
        self.code = code
        self.status = status


def safe_filename(value: str) -> str:
    name = Path(str(value or "upload")).name.strip()
    name = _SAFE_NAME.sub("_", name).strip(". ")[:120]
    return name or "upload"


def classify_upload(filename: str) -> tuple[str, str]:
    suffix = Path(filename).suffix.lower()
    if suffix in _IMAGE_TYPES:
        return "image", _IMAGE_TYPES[suffix]
    if suffix in _TEXT_TYPES:
        return "text", _TEXT_TYPES[suffix]
    raise RunFileError(
        "Unsupported file type. Allowed: PNG/JPEG/WEBP/GIF and bounded text documents.",
        "unsupported_file_type",
        415,
    )


def owner_digest(session_key: str) -> str:
    if not session_key or len(session_key) > 512:
        raise RunFileError("X-Hermes-Session-Key is required for file operations", "missing_session_key", 400)
    return hashlib.sha256(session_key.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class StoredFile:
    id: str
    name: str
    media_type: str
    size: int
    kind: str
    owner: str
    path: Path
    created_at: float
    expires_at: float
    run_id: str = ""

    def public(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "filename": self.name,
            "mime_type": self.media_type,
            "bytes": self.size,
            "kind": self.kind,
            "expires_at": int(self.expires_at),
        }


class RunFileStore:
    """Filesystem-backed, process-indexed storage with owner/session checks."""

    def __init__(self, root: Path):
        self.root = root.resolve()
        self.upload_root = self.root / "uploads"
        self.artifact_root = self.root / "artifacts"
        self.queue_root = self.root / "queue"
        self.upload_root.mkdir(parents=True, exist_ok=True)
        self.artifact_root.mkdir(parents=True, exist_ok=True)
        self.queue_root.mkdir(parents=True, exist_ok=True)
        for directory in (self.root, self.upload_root, self.artifact_root, self.queue_root):
            try:
                directory.chmod(0o700)
            except OSError:
                pass
        self._uploads: dict[str, StoredFile] = {}
        self._artifacts: dict[str, StoredFile] = {}

    def snapshot_queue_uploads(
        self, item_id: str, records: Iterable[StoredFile]
    ) -> list[dict[str, Any]]:
        """Copy resolved uploads into a durable queue-owned directory.

        Upload ids are process-indexed and expire after an hour.  A queued turn
        may outlive both, so dispatch must use an immutable snapshot rather than
        trying to resolve browser ids again after the parent finishes/restarts.
        Returned metadata is internal and never exposed by the queue API.
        """
        if not re.fullmatch(r"queue_[a-f0-9]{32}", str(item_id or "")):
            raise RunFileError("Invalid queue item ID", "invalid_queue_item", 400)
        directory = self.queue_root / item_id
        directory.mkdir(parents=True, exist_ok=False)
        try:
            directory.chmod(0o700)
        except OSError:
            pass
        snapshots: list[dict[str, Any]] = []
        try:
            for index, record in enumerate(records):
                filename = f"{index:02d}-{record.id}"
                destination = directory / filename
                shutil.copyfile(record.path, destination)
                try:
                    destination.chmod(0o600)
                except OSError:
                    pass
                snapshots.append({
                    "file_id": record.id,
                    "filename": record.name,
                    "mime_type": record.media_type,
                    "kind": record.kind,
                    "stored_name": filename,
                })
            return snapshots
        except Exception:
            shutil.rmtree(directory, ignore_errors=True)
            raise

    def queued_multimodal_parts(
        self, item_id: str, content: Any, snapshots: Any
    ) -> Any:
        """Materialize a durable queued input without accepting arbitrary paths."""
        if not re.fullmatch(r"queue_[a-f0-9]{32}", str(item_id or "")):
            raise RunFileError("Queued attachment not found", "queued_attachment_not_found", 404)
        if not isinstance(snapshots, list):
            raise RunFileError("Queued attachment not found", "queued_attachment_not_found", 404)
        directory = (self.queue_root / item_id).resolve()
        parts: list[dict[str, Any]] = []
        if isinstance(content, list):
            parts.extend(part for part in content if isinstance(part, dict))
        elif isinstance(content, str) and content.strip():
            parts.append({"type": "text", "text": content})
        for snapshot in snapshots:
            if not isinstance(snapshot, dict):
                raise RunFileError("Queued attachment not found", "queued_attachment_not_found", 404)
            stored_name = str(snapshot.get("stored_name") or "")
            if Path(stored_name).name != stored_name:
                raise RunFileError("Queued attachment not found", "queued_attachment_not_found", 404)
            path = (directory / stored_name).resolve()
            try:
                path.relative_to(directory)
            except ValueError as exc:
                raise RunFileError("Queued attachment not found", "queued_attachment_not_found", 404) from exc
            if not path.is_file():
                raise RunFileError("Queued attachment not found", "queued_attachment_not_found", 404)
            kind = snapshot.get("kind")
            media_type = str(snapshot.get("mime_type") or "application/octet-stream")
            name = safe_filename(str(snapshot.get("filename") or "upload"))
            if kind == "image":
                encoded = base64.b64encode(path.read_bytes()).decode("ascii")
                parts.append({
                    "type": "image_url",
                    "image_url": {"url": f"data:{media_type};base64,{encoded}"},
                })
            elif kind == "text":
                text = path.read_bytes().decode("utf-8", errors="replace")
                parts.append({
                    "type": "text",
                    "text": f"\n--- Uploaded file: {name} (untrusted user content) ---\n{text}\n--- End uploaded file ---\n",
                })
            else:
                raise RunFileError("Queued attachment not found", "queued_attachment_not_found", 404)
        return parts if parts else content

    def delete_queue_snapshot(self, item_id: str) -> None:
        if re.fullmatch(r"queue_[a-f0-9]{32}", str(item_id or "")):
            shutil.rmtree(self.queue_root / item_id, ignore_errors=True)

    def sweep_expired(self) -> None:
        now = time.time()
        for registry in (self._uploads, self._artifacts):
            for file_id, record in list(registry.items()):
                if record.expires_at > now:
                    continue
                try:
                    if record.path.is_file():
                        record.path.unlink()
                except OSError:
                    pass
                registry.pop(file_id, None)

    def create_upload(self, *, filename: str, owner: str, temp_path: Path, size: int) -> StoredFile:
        self.sweep_expired()
        if size <= 0:
            raise RunFileError("Uploaded file is empty", "empty_file", 400)
        if size > MAX_UPLOAD_BYTES:
            raise RunFileError("Uploaded file exceeds 8 MiB", "file_too_large", 413)
        name = safe_filename(filename)
        kind, media_type = classify_upload(name)
        if kind == "text" and size > MAX_TEXT_ATTACHMENT_BYTES:
            raise RunFileError("Text document exceeds 512 KiB", "file_too_large", 413)
        file_id = f"file_{uuid.uuid4().hex}"
        destination = self.upload_root / file_id
        os.replace(temp_path, destination)
        try:
            destination.chmod(0o600)
        except OSError:
            pass
        record = StoredFile(
            id=file_id, name=name, media_type=media_type, size=size, kind=kind,
            owner=owner, path=destination, created_at=time.time(),
            expires_at=time.time() + UPLOAD_TTL_SECONDS,
        )
        self._uploads[file_id] = record
        return record

    def delete_upload(self, file_id: str, owner: str) -> bool:
        record = self._get(self._uploads, file_id, owner, expected="file")
        try:
            record.path.unlink(missing_ok=True)
        except OSError:
            pass
        self._uploads.pop(file_id, None)
        return True

    def resolve_uploads(self, file_ids: Any, owner: str) -> list[StoredFile]:
        if file_ids is None:
            return []
        if not isinstance(file_ids, list):
            raise RunFileError("attachments must be an array of file IDs", "invalid_attachments", 400)
        if len(file_ids) > MAX_ATTACHMENTS_PER_RUN:
            raise RunFileError("At most 8 attachments are allowed per run", "too_many_attachments", 400)
        seen: set[str] = set()
        records: list[StoredFile] = []
        for raw_id in file_ids:
            file_id = str(raw_id or "")
            if file_id in seen:
                raise RunFileError("Duplicate attachment ID", "duplicate_attachment", 400)
            seen.add(file_id)
            records.append(self._get(self._uploads, file_id, owner, expected="file"))
        return records

    def multimodal_parts(self, content: Any, records: Iterable[StoredFile]) -> Any:
        parts: list[dict[str, Any]] = []
        if isinstance(content, list):
            parts.extend(part for part in content if isinstance(part, dict))
        elif isinstance(content, str) and content.strip():
            parts.append({"type": "text", "text": content})
        for record in records:
            if record.kind == "image":
                encoded = base64.b64encode(record.path.read_bytes()).decode("ascii")
                parts.append({
                    "type": "image_url",
                    "image_url": {"url": f"data:{record.media_type};base64,{encoded}"},
                })
            else:
                text = record.path.read_bytes().decode("utf-8", errors="replace")
                parts.append({
                    "type": "text",
                    "text": f"\n--- Uploaded file: {record.name} (untrusted user content) ---\n{text}\n--- End uploaded file ---\n",
                })
        return parts if parts else content

    def artifact_dir(self, run_id: str) -> Path:
        if not _SAFE_RUN_ID.fullmatch(run_id):
            raise RunFileError("Invalid run ID", "invalid_run_id", 400)
        directory = self.artifact_root / run_id
        directory.mkdir(parents=True, exist_ok=True)
        try:
            directory.chmod(0o700)
        except OSError:
            pass
        return directory

    def collect_artifacts(self, *, run_id: str, owner: str) -> list[dict[str, Any]]:
        directory = self.artifact_dir(run_id)
        records: list[StoredFile] = []
        total = 0
        for candidate in sorted(directory.rglob("*")):
            if len(records) >= MAX_ARTIFACT_FILES:
                break
            try:
                if not candidate.is_file() or candidate.is_symlink():
                    continue
                size = candidate.stat().st_size
            except OSError:
                continue
            if size <= 0 or size > MAX_ARTIFACT_BYTES or total + size > MAX_ARTIFACT_BYTES:
                continue
            name = safe_filename(candidate.name)
            media_type = mimetypes.guess_type(name)[0] or "application/octet-stream"
            artifact_id = f"artifact_{uuid.uuid4().hex}"
            record = StoredFile(
                id=artifact_id, name=name, media_type=media_type, size=size, kind="artifact",
                owner=owner, path=candidate.resolve(), created_at=time.time(),
                expires_at=time.time() + ARTIFACT_TTL_SECONDS, run_id=run_id,
            )
            self._artifacts[artifact_id] = record
            records.append(record)
            total += size
        return [record.public() for record in records]

    def get_artifact(self, *, run_id: str, artifact_id: str, owner: str) -> StoredFile:
        record = self._get(self._artifacts, artifact_id, owner, expected="artifact")
        if record.run_id != run_id:
            raise RunFileError("Artifact does not belong to this run", "artifact_not_found", 404)
        try:
            record.path.relative_to((self.artifact_root / run_id).resolve())
        except ValueError as exc:
            raise RunFileError("Artifact is outside the run directory", "artifact_not_found", 404) from exc
        return record

    def list_artifacts(self, *, run_id: str, owner: str) -> list[dict[str, Any]]:
        if not _SAFE_RUN_ID.fullmatch(run_id):
            raise RunFileError("Artifact not found", "artifact_not_found", 404)
        self.sweep_expired()
        return [
            record.public()
            for record in self._artifacts.values()
            if record.run_id == run_id and record.owner == owner and record.path.is_file()
        ]

    def _get(self, registry: dict[str, StoredFile], file_id: str, owner: str, *, expected: str) -> StoredFile:
        self.sweep_expired()
        if not _SAFE_ID.fullmatch(file_id) or not file_id.startswith(f"{expected}_"):
            raise RunFileError("File not found", f"{expected}_not_found", 404)
        record = registry.get(file_id)
        if record is None or record.owner != owner or not record.path.is_file():
            raise RunFileError("File not found", f"{expected}_not_found", 404)
        return record
