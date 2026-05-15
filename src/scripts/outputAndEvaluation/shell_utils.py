from __future__ import annotations

import os
import shutil
from pathlib import Path


def resolve_bash_executable() -> str:
    bash_path = shutil.which("bash")
    if bash_path:
        bp = str(bash_path)
        bp_l = bp.lower()
        # Avoid returning WindowsApps or System32 WSL shim which may exist
        # but not provide a working /bin/bash usable by scripts.
        if ("windowsapps" in bp_l) or bp_l.endswith(str(Path("system32") / "bash.exe")):
            bash_path = None
        else:
            return bash_path

    if os.name == "nt":
        windows_candidates = [
            Path(os.environ.get("ProgramW6432", r"C:\Program Files")) / "Git" / "bin" / "bash.exe",
            Path(os.environ.get("ProgramW6432", r"C:\Program Files")) / "Git" / "usr" / "bin" / "bash.exe",
            Path(os.environ.get("ProgramFiles", r"C:\Program Files")) / "Git" / "bin" / "bash.exe",
            Path(os.environ.get("ProgramFiles", r"C:\Program Files")) / "Git" / "usr" / "bin" / "bash.exe",
            Path(os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)")) / "Git" / "bin" / "bash.exe",
            Path(os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)")) / "Git" / "usr" / "bin" / "bash.exe",
        ]
        for candidate in windows_candidates:
            if candidate.exists():
                return str(candidate)

    raise FileNotFoundError(
        "bash executable not found. Install Git Bash or add bash to PATH."
    )