from __future__ import annotations

import shlex
import subprocess
import sys
from pathlib import Path
from typing import Optional, Tuple

from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact

# Optional S3 support
try:
    import boto3  # type: ignore
except Exception:  # pragma: no cover
    boto3 = None


def _is_s3_path(p: str) -> bool:
    """Return True if the string looks like an S3 URL."""
    return isinstance(p, str) and p.startswith("s3://")


def _split_s3_url(url: str) -> Tuple[str, str]:
    """
    Split s3://bucket/key into (bucket, key).

    Raises AssertionError if url does not start with s3://
    """
    assert url.startswith("s3://"), "Not an S3 URL"
    without = url.removeprefix("s3://")
    bucket, key = without.split("/", 1)
    return bucket, key


@task
def stage_inputs(
    manifest_path: str,
    template_path: str,
    entries_path: str,
) -> Tuple[str, str, str]:
    """
    Resolve input paths for the flow.

    - If a path is s3://..., download it into ./data and return the local path.
    - Otherwise, treat it as a local path and verify it exists.
    """
    logger = get_run_logger()
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)

    def resolve(p: str) -> str:
        # S3 path: download to ./data
        if _is_s3_path(p):
            if boto3 is None:
                raise RuntimeError(
                    "boto3 is required for S3 paths; install boto3 and configure AWS credentials."
                )
            bucket, key = _split_s3_url(p)
            dst = data_dir / Path(key).name
            logger.info("Downloading %s -> %s", p, dst)
            import boto3 as _boto3  # local import to avoid import-time issues

            _boto3.client("s3").download_file(bucket, key, str(dst))
            return str(dst.resolve())

        # Local path: ensure it exists
        path = Path(p).expanduser()
        if not path.exists():
            raise FileNotFoundError(f"Required file not found: {path}")
        return str(path.resolve())

    m = resolve(manifest_path)
    t = resolve(template_path)
    e = resolve(entries_path)
    return m, t, e


@task
def run_entry_remove_script(
    manifest: str,
    template: str,
    entries: str,
    python_exe: Optional[str] = None,
) -> dict:
    """
    Run entry_remove.py as a subprocess.

    Returns a dict with stdout, stderr, and return code.
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

    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=str(Path(__file__).resolve().parents[1]),  # project root where entry_remove.py lives
    )

    logger.info("Return code: %s", proc.returncode)
    if proc.stdout:
        logger.info("STDOUT:\n%s", proc.stdout)
    if proc.stderr:
        logger.warning("STDERR:\n%s", proc.stderr)

    return {"returncode": proc.returncode, "stdout": proc.stdout, "stderr": proc.stderr}


@task
def ship_outputs_to_s3(
    outputs_glob: str,
    s3_output_prefix: Optional[str] = None,
) -> Optional[list]:
    """
    Upload files matching outputs_glob to s3_output_prefix if provided.

    s3_output_prefix should be a full S3 URL like:
        s3://my-bucket/path/prefix/
    """
    if not s3_output_prefix:
        return None

    if boto3 is None:
        raise RuntimeError(
            "boto3 is required for S3 upload; install boto3 and configure AWS credentials."
        )

    from pathlib import Path
    import boto3 as _boto3

    bucket, prefix = _split_s3_url(s3_output_prefix.rstrip("/") + "/")
    s3 = _boto3.client("s3")

    uploaded: list[str] = []
    for p in Path(".").glob(outputs_glob):
        if p.is_file():
            key = f"{prefix}{p.name}"
            s3.upload_file(str(p), bucket, key)
            uploaded.append(f"s3://{bucket}/{key}")
    return uploaded


@task
def publish_run_summary(result: dict, uploaded: Optional[list] = None) -> None:
    """
    Create a small Markdown artifact summarizing the run.
    """
    md = [
        "# entry_remove.py run summary",
        f"**Return code:** `{result['returncode']}`",
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


@flow(name="entry-remove-flow")
def run_entry_remove(
    # UI-entered ARGS (S3-first)
    s3_bucket: str = "ccdi-validation",
    manifest_key: str = "",
    template_key: str = "",
    entries_key: str = "",
    # Where in the bucket to put results (e.g., outputs/entry-remove/)
    output_prefix: str = "outputs/entry-remove/",

    # Pattern for local output files created by entry_remove.py
    outputs_glob: str = "*_EntRemove*.xlsx",

    # Fallback local paths (used when keys are blank or for local testing)
    manifest_path: str = "data/manifest.xlsx",
    template_path: str = "data/template.xlsx",
    entries_path: str = "data/entries.tsv",

    # Optional override: full S3 prefix URL for outputs (e.g., s3://bucket/prefix/)
    s3_output_prefix: Optional[str] = None,

    # Optional: override Python executable
    python_exe: Optional[str] = None,
) -> dict:
    """
    Prefect flow that wraps entry_remove.py for use in Prefect UI / ECS.

    Parameters (UI-facing)
    ----------------------
    s3_bucket : str
        Bucket where inputs live and outputs are stored (e.g. 'ccdi-validation').
    manifest_key : str
        S3 key (path inside the bucket) for the working manifest.
        Example: 'incoming/user123/manifest.xlsx'
    template_key : str
        S3 key for the manifest template.
        Example: 'templates/manifest_template.xlsx'
    entries_key : str
        S3 key for the file listing entries to remove.
        Example: 'incoming/user123/entries.tsv'
    output_prefix : str
        Prefix inside s3_bucket where outputs will be written.
        Example: 'outputs/entry-remove/user123/'

    outputs_glob : str
        Pattern to match local output files produced by entry_remove.py.
    manifest_path, template_path, entries_path : str
        Local paths for testing or non-S3 runs.

    s3_output_prefix : Optional[str]
        Full S3 URL for output prefix. If not provided, one is built from
        s3_bucket + output_prefix.
    python_exe : Optional[str]
        Alternate Python executable to run entry_remove.py
    """
    logger = get_run_logger()

    # --- Construct S3 URLs from UI-provided keys, if present ---
    # If user gives keys, we ignore the local paths and build S3 URLs.
    if manifest_key:
        manifest_path = f"s3://{s3_bucket}/{manifest_key}"
        logger.info("Using S3 manifest path: %s", manifest_path)
    if template_key:
        template_path = f"s3://{s3_bucket}/{template_key}"
        logger.info("Using S3 template path: %s", template_path)
    if entries_key:
        entries_path = f"s3://{s3_bucket}/{entries_key}"
        logger.info("Using S3 entries path: %s", entries_path)

    # --- Construct default s3_output_prefix if not provided ---
    if s3_output_prefix is None and output_prefix:
        # Ensure exactly one slash between bucket and prefix
        s3_output_prefix = f"s3://{s3_bucket}/{output_prefix.lstrip('/')}"
        logger.info("Using S3 output prefix: %s", s3_output_prefix)

    # --- Stage inputs (download from S3 or validate local) ---
    m, t, e = stage_inputs(manifest_path, template_path, entries_path)

    # --- Run the script ---
    result = run_entry_remove_script(m, t, e, python_exe)

    # --- Ship outputs (if configured) ---
    uploaded = ship_outputs_to_s3(outputs_glob, s3_output_prefix)

    # --- Publish a summary artifact ---
    publish_run_summary(result, uploaded)

    return result


def serve_locally() -> None:
    """
    Convenience helper for local development.

    Run:
        python flows/entry_remove_flow.py

    Then uncomment the serve_locally() call at the bottom to expose a
    local deployment you can trigger from the UI.
    """
    run_entry_remove.serve(
        name="entry-remove-local",
        tags=["local", "dev"],
        parameters={
            "manifest_path": "data/manifest.xlsx",
            "template_path": "data/template.xlsx",
            "entries_path": "data/entries.tsv",
            "outputs_glob": "*_EntRemove*.xlsx",
            "s3_output_prefix": None,
        },
    )


if __name__ == "__main__":
    # Default: run once locally (good for quick testing)
    run_entry_remove()
    # For a local dev server, uncomment:
    # serve_locally()
