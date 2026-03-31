"""Compatibility entrypoint for generate_data."""

from src.pipeline.generate_data import main as _legacy_main


if __name__ == "__main__":
    _legacy_main()
