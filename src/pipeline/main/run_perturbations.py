"""Compatibility entrypoint for run_perturbations."""

from src.pipeline.run_perturbations import main as _legacy_main


if __name__ == "__main__":
    _legacy_main()
