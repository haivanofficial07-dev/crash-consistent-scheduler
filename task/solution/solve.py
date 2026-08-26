#!/usr/bin/env python3
"""Install the reference scheduler repair into the agent-visible codebase."""

from pathlib import Path
import shutil


def main() -> None:
    destination = Path("/app/scheduler/scheduler.py")
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(Path(__file__).with_name("fixed_scheduler.py"), destination)


if __name__ == "__main__":
    main()
