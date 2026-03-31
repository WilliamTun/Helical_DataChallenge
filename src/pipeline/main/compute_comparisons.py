"""Compatibility entrypoint for compute_comparisons."""

from src.pipeline.compute_comparisons import main as _legacy_main


if __name__ == "__main__":
    _legacy_main()
