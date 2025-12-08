from __future__ import annotations

from typing import Optional

from prefect import flow, get_run_logger

from src.utils import (
    stage_inputs,
    run_entry_remove_script,
    ship_outputs_to_s3,
    publish_run_summary,
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
        Bucket where inputs live and outputs are stored (e.g. "ccdi-validation").
    manifest_key : str
        S3 key (path inside the bucket) for the working manifest.
        Example: "incoming/user123/manifest.xlsx"
    template_key : str
        S3 key for the manifest template.
        Example: "templates/manifest_template.xlsx"
    entries_key : str
        S3 key for the file listing entries to remove.
        Example: "incoming/user123/entries.tsv"
    output_prefix : str
        Prefix inside s3_bucket where outputs will be written.
        Example: "outputs/entry-remove/user123/"

    outputs_glob : str
        Pattern to match local output files produced by entry_remove.py.
    manifest_path, template_path, entries_path : str
        Local paths for testing or non-S3 runs.

    s3_output_prefix : Optional[str]
        Full S3 URL for output prefix. If not provided, one is built from
        s3_bucket + output_prefix.
    python_exe : Optional[str]
        Alternate Python executable to run entry_remove.py.
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
