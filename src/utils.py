from __future__ import annotations

import logging
import os
import shlex
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple, List

from botocore.exceptions import ClientError
from prefect import task, get_run_logger
from prefect.artifacts import create_markdown_artifact
from pytz import timezone

# ---------------------------------------------------------------------------
# boto3 import (so local, non-S3 runs work without AWS extras)
# ---------------------------------------------------------------------------
try:
    import boto3  # type: ignore
except Exception:  # pragma: no cover
    boto3 = None


# ---------------------------------------------------------------------------
# Generic time / logging / S3 helpers (adapted from CB working utils.py)
# ---------------------------------------------------------------------------

def get_time() -> str:
    """
    Return the current time as a compact string for filenames/logs,
    e.g. '20251204_T153015'.
    """
    tz = timezone("EST")
    now = datetime.now(tz)
    return now.strftime("%Y%m%d_T%H%M%S")


def set_s3_resource():
    """
    Return a boto3 S3 resource, supporting Localstack if
    LOCALSTACK_ENDPOINT_URL is set.

    This mirrors the pattern in your cBioPortal workflows utils module.
    """
    if boto3 is None:
        raise RuntimeError(
            "boto3 is required for S3 operations; install boto3 "
            "and configure AWS credentials or Localstack."
        )

    localstack_endpoint = os.environ.get("LOCALSTACK_ENDPOINT_URL")
    if localstack_endpoint:
        # Local testing with Localstack
        aws_region = "us-east-1"
        aws_profile = "localstack"
        boto3.setup_default_session(profile_name=aws_profile)
        s3_resource = boto3.resource(
            "s3", region_name=aws_region, endpoint_url=localstack_endpoint
        )
    else:
        # Normal AWS usage
        s3_resource = boto3.resource("s3")

    return s3_resource


def get_logger(loggername: str, log_level: str = "info") -> logging.Logger:
    """
    Basic file logger helper, similar to your working utils.

    Mostly useful if you want a non-Prefect logfile alongside Prefect logs.
    """
    log_levels = {
        "notset": logging.NOTSET,
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
    }

    logger_filename = loggername + ".log"
    logger = logging.getLogger(loggername)
    logger.setLevel(log_levels.get(log_level, logging.INFO))

    if not logger.handlers:
        file_format = "%(asctime)s - %(levelname)s - %(message)s"
        file_handler = logging.FileHandler(logger_filename, mode="w")
        file_handler.setFormatter(logging.Formatter(file_format, "%H:%M:%S"))
        file_handler.setLevel(log_levels["info"])
        logger.addHandler(file_handler)

    return logger


# ---------------------------------------------------------------------------
# S3 URL helpers
# ---------------------------------------------------------------------------

def _is_s3_path(p: str) -> bool:
    """Return True if the string looks like an s3:// URL."""
    return isinstance(p, str) and p.startswith("s3://")


def _split_s3_url(url: str) -> Tuple[str, str]:
    """
    Split 's3://bucket/key' into (bucket, key).

    Raises AssertionError if url does not start with 's3://'.
    """
    assert url.startswith("s3://"), "Not an S3 URL"
    without = url.removeprefix("s3://")
    bucket, key = without.split("/", 1)
    return bucket, key


# ---------------------------------------------------------------------------
# Prefect tasks specific to EntryRemoval-Prefect
# ---------------------------------------------------------------------------

@task(name="Stage inputs", task_run_name="stage_inputs", log_prints=True)
def stage_inputs(
    manifest_path: str,
    template_path: str,
    entries_path: str,
) -> Tuple[str, str, str]:
    """
    Resolve input paths for the flow.

    Behavior:
    ---------
    - If a path is s3://BUCKET/key, download into ./data/ and return the local path.
    - Otherwise, treat it as a local path and verify it exists.

    This is the same logic as in the flow file, now factored into utils.
    """
    logger = get_run_logger()
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)

    def resolve(p: str) -> str:
        # S3 path: download to ./data using the shared set_s3_resource()
        if _is_s3_path(p):
            if boto3 is None:
                raise RuntimeError(
                    "boto3 is required for S3 paths; install boto3 and "
                    "configure AWS credentials or Localstack."
                )
            bucket, key = _split_s3_url(p)
            dst = data_dir / Path(key).name
            logger.info("Downloading %s -> %s", p, dst)

            s3 = set_s3_resource()
            try:
                s3.Bucket(bucket).download_file(key, str(dst))
            except ClientError as ex:
                logger.error(
                    "ClientError downloading %s from bucket %s: %s",
                    key, bucket, ex,
                )
                raise

            return str(dst.resolve())

        # Local path: ensure it exists
        path = Path(p).expanduser()
        if not path.exists():
            raise FileNotFoundError(f"Required file not found: {path}")
        logger.info("Using local file: %s", path)
        return str(path.resolve())

    m = resolve(manifest_path)
    t = resolve(template_path)
    e = resolve(entries_path)
    return m, t, e


@task(name="Run entry_remove.py", task_run_name="run_entry_remove", log_prints=True)
def run_entry_remove_script(
    manifest: str,
    template: str,
    entries: str,
    python_exe: Optional[str] = None,
) -> dict:
    """
    Run entry_remove.py as a subprocess.

    Parameters
    ----------
    manifest : str
        Local path to the working manifest.
    template : str
        Local path to the manifest template.
    entries : str
        Local path to the entries-to-remove file.
    python_exe : Optional[str]
        Optional override of the Python executable (defaults to sys.executable).

    Returns
    -------
    dict with keys:
        - 'returncode'
        - 'stdout'
        - 'stderr'
    """
    logger = get_run_logger()
    python = python_exe or sys.executable

    cmd = [
        python,
        "entry_remove.py",
        "-f",
        manifest,
        "-t",
        template,
        "-e",
        entries,
    ]
    logger.info("Running command: %s", " ".join(shlex.quote(c) for c in cmd))

    # Project root is one level above src/, where entry_remove.py lives
    project_root = Path(__file__).resolve().parents[1]

    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=str(project_root),
    )

    logger.info("Return code: %s", proc.returncode)
    if proc.stdout:
        logger.info("STDOUT:\n%s", proc.stdout)
    if proc.stderr:
        logger.warning("STDERR:\n%s", proc.stderr)

    return {
        "returncode": proc.returncode,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }


@task(name="Ship outputs to S3", task_run_name="ship_outputs", log_prints=True)
def ship_outputs_to_s3(
    outputs_glob: str,
    s3_output_prefix: Optional[str] = None,
) -> Optional[List[str]]:
    """
    Upload files matching `outputs_glob` to `s3_output_prefix` if provided.

    Parameters
    ----------
    outputs_glob : str
        Glob pattern for files produced by entry_remove.py,
        e.g. '*_EntRemove*.xlsx'.
    s3_output_prefix : Optional[str]
        Full S3 prefix (URL) like:
            s3://my-bucket/path/prefix/

    Returns
    -------
    Optional[List[str]]
        List of fully-qualified S3 URLs for uploaded files, or None if no
        prefix was provided.
    """
    logger = get_run_logger()

    if not s3_output_prefix:
        logger.info("No s3_output_prefix provided; skipping upload.")
        return None

    if boto3 is None:
        raise RuntimeError(
            "boto3 is required for S3 upload; install boto3 and configure AWS."
        )

    s3 = set_s3_resource()
    bucket, prefix = _split_s3_url(s3_output_prefix.rstrip("/") + "/")

    uploaded: List[str] = []
    for p in Path(".").glob(outputs_glob):
        if p.is_file():
            key = f"{prefix}{p.name}"
            logger.info("Uploading %s -> s3://%s/%s", p, bucket, key)
            try:
                s3.Bucket(bucket).upload_file(str(p), key)
            except ClientError as ex:
                logger.error(
                    "ClientError uploading %s to s3://%s/%s: %s",
                    p, bucket, key, ex,
                )
                raise
            uploaded.append(f"s3://{bucket}/{key}")

    if not uploaded:
        logger.warning(
            "No files matching pattern '%s' found to upload.", outputs_glob
        )

    return uploaded


@task(name="Publish run summary", task_run_name="publish_summary", log_prints=True)
def publish_run_summary(result: dict, uploaded: Optional[List[str]] = None) -> None:
    """
    Create a small Markdown artifact summarizing the run so that the
    Prefect UI has a nice summary card.

    This is the same logic as in CB flow file, just centralized.
    """
    md = [
        "# entry_remove.py run summary",
        f"**Run time:** `{get_time()}`",
        f"**Return code:** `{result.get('returncode')}`",
    ]
    if result.get("stdout"):
        md += ["## Stdout", "```", result["stdout"][-8000:], "```"]
    if result.get("stderr"):
        md += ["## Stderr", "```", result["stderr"][-8000:], "```"]
    if uploaded:
        md.append("## Uploaded outputs")
        md += [f"- {u}" for u in uploaded]

    create_markdown_artifact(
        key="entry-remove-summary",
        markdown="\n".join(md),
    )
