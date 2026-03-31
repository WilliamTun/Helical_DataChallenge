"""Compatibility entrypoint for preview_data."""

from src.pipeline.preview_data import main as _legacy_main


if __name__ == "__main__":
    _legacy_main()
