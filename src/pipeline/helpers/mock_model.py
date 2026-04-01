"""Mock foundation model runner for AnnData pipeline testing."""

from __future__ import annotations

from typing import Any

import numpy as np
from anndata import AnnData

ALLOWED_MODEL_NAMES = {
    "mock_foundation_model_1",
    "mock_foundation_model_2",
    "mock_foundation_model_3",
}
DEFAULT_MODEL_NAME = "mock_foundation_model_1"

MOCK_MODEL_VERSION = "0.1.0"
MOCK_EMBEDDING_DIM = 512
MOCK_EMBEDDING_DTYPE = "float32"


def mock_model_version_info(model_name: str | None = None) -> dict[str, Any]:
    resolved_model_name = model_name or DEFAULT_MODEL_NAME
    if resolved_model_name not in ALLOWED_MODEL_NAMES:
        raise ValueError(
            "Unsupported model_name "
            f"{resolved_model_name!r}. Allowed values: {sorted(ALLOWED_MODEL_NAMES)}"
        )
    return {
        "model_name": resolved_model_name,
        "model_version": MOCK_MODEL_VERSION,
        "embedding_dim": MOCK_EMBEDDING_DIM,
        "embedding_dtype": MOCK_EMBEDDING_DTYPE,
    }


def run_model(adata: AnnData) -> np.ndarray:
    if adata.n_obs < 0:
        raise ValueError("AnnData has invalid number of observations.")
    rng = np.random.default_rng()
    return rng.normal(loc=0.0, scale=1.0, size=(adata.n_obs, MOCK_EMBEDDING_DIM)).astype(
        np.float32
    )
