"""
Embedding backends for the recommendation engine (see PLAN.md).

One tiny interface — ``Embedder.encode(list[str]) -> float32 [n, dim]``,
L2-normalized — with three implementations selected by short name:

    potion (default)  model2vec static embeddings
                      (minishlab/potion-base-8M, 256-dim). No torch, ~30 MB,
                      embeds the whole 16k corpus in seconds; downloaded from
                      the HF hub on first use and cached locally.
    minilm            sentence-transformers/all-MiniLM-L6-v2 (384-dim).
                      Higher quality, needs the `minilm` extra (torch):
                      uv sync --extra minilm
    hash              Deterministic token feature-hashing (256-dim). No
                      model file, no network, stable across runs — for
                      tests, CI, and offline benchmarks, not for quality.

The index records the embedder's canonical name + dim in rec_meta, and the
query layer re-instantiates the same one, so index and query vectors can
never come from different models.
"""

from __future__ import annotations

import hashlib
import re

import numpy as np

_SPECS = {
    "potion": "minishlab/potion-base-8M",
    "minilm": "sentence-transformers/all-MiniLM-L6-v2",
    "hash": "hash-v1-256",
}
DEFAULT_MODEL = "potion"


def _l2_normalize(mat: np.ndarray) -> np.ndarray:
    mat = np.asarray(mat, dtype=np.float32)
    norms = np.linalg.norm(mat, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return mat / norms


class Model2VecEmbedder:
    def __init__(self, model_id: str = _SPECS["potion"]):
        from model2vec import StaticModel

        self._model = StaticModel.from_pretrained(model_id)
        self.name = model_id
        self.dim = int(self._model.dim)

    def encode(self, texts: list[str]) -> np.ndarray:
        return _l2_normalize(self._model.encode(texts))


class SentenceTransformerEmbedder:
    def __init__(self, model_id: str = _SPECS["minilm"]):
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError as e:
            raise SystemExit(
                "sentence-transformers is not installed; run "
                "`uv sync --extra minilm` to use --model minilm"
            ) from e
        self._model = SentenceTransformer(model_id)
        self.name = model_id
        self.dim = int(self._model.get_sentence_embedding_dimension())

    def encode(self, texts: list[str]) -> np.ndarray:
        return _l2_normalize(self._model.encode(texts, normalize_embeddings=True))


class HashEmbedder:
    """Feature-hashed unigrams+bigrams: similar token sets -> similar
    vectors. Deterministic (blake2b), dependency-free, quality-poor."""

    _token_re = re.compile(r"[a-z0-9][a-z0-9+#.\-]*")

    def __init__(self, dim: int = 256):
        self.name = f"hash-v1-{dim}"
        self.dim = dim

    def _tokens(self, text: str) -> list[str]:
        toks = self._token_re.findall(text.lower())
        return toks + [f"{a}_{b}" for a, b in zip(toks, toks[1:])]

    def encode(self, texts: list[str]) -> np.ndarray:
        out = np.zeros((len(texts), self.dim), dtype=np.float32)
        for i, text in enumerate(texts):
            for tok in self._tokens(text):
                h = hashlib.blake2b(tok.encode(), digest_size=8).digest()
                bucket = int.from_bytes(h[:4], "little") % self.dim
                sign = 1.0 if h[4] & 1 else -1.0
                out[i, bucket] += sign
        return _l2_normalize(out)


def get_embedder(spec: str = DEFAULT_MODEL):
    """spec is a short name (potion|minilm|hash), a canonical name a
    previous index recorded in rec_meta, or any model2vec-loadable HF id."""
    if spec in _SPECS:
        spec = _SPECS[spec]
    if spec.startswith("hash-v1-"):
        return HashEmbedder(int(spec.rsplit("-", 1)[1]))
    if spec == _SPECS["minilm"]:
        return SentenceTransformerEmbedder(spec)
    return Model2VecEmbedder(spec)
