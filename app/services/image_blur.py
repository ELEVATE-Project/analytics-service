import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Dict, Any, Optional

logger = logging.getLogger("analytics_service.services.image_blur")

def _deface_command() -> list:
    deface_bin = shutil.which("deface")
    if deface_bin:
        return [deface_bin]
    # Fallback to python execution module
    return [sys.executable, "-m", "deface.deface"]

def anonymize_face(
    input_path: str,
    output_path: str,
    threshold: Optional[float] = None,
    scale: Optional[str] = None,
    mask_scale: Optional[float] = None,
    backend: Optional[str] = None,
    boxes: bool = False,
    draw_scores: bool = False,
    keep_metadata: bool = False
) -> Dict[str, Any]:
    """
    Executes face anonymization on an image or video path using the deface CLI.
    Runs locally as a generic Python helper.
    """
    in_path = Path(input_path).expanduser().resolve()
    out_path = Path(output_path).expanduser().resolve()

    if not in_path.exists():
        raise FileNotFoundError(f"Input file not found: {in_path}")

    out_path.parent.mkdir(parents=True, exist_ok=True)

    command = _deface_command()
    command.extend([
        str(in_path),
        "--output",
        str(out_path),
        "--replacewith",
        "blur",
    ])

    if threshold is not None:
        command.extend(["--thresh", str(threshold)])
    if scale:
        command.extend(["--scale", str(scale)])
    if mask_scale is not None:
        command.extend(["--mask-scale", str(mask_scale)])
    if backend:
        command.extend(["--backend", str(backend)])
    if boxes:
        command.append("--boxes")
    if draw_scores:
        command.append("--draw-scores")
    if keep_metadata:
        command.append("--keep-metadata")

    logger.info(f"Running deface command: {' '.join(command)}")
    
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        cwd=str(in_path.parent)
    )

    if result.returncode != 0:
        logger.error(f"deface failed with code {result.returncode}. STDERR: {result.stderr}")
        raise RuntimeError(f"deface failed with exit code {result.returncode}: {result.stderr}")

    if not out_path.exists():
        raise RuntimeError(f"deface finished but output file was not created: {out_path}")

    logger.info(f"Successfully blurred faces. Output saved to {out_path}")
    return {
        "input_path": str(in_path),
        "output_path": str(out_path),
        "file_size_bytes": os.path.getsize(out_path),
        "stdout": result.stdout.strip(),
        "stderr": result.stderr.strip()
    }
