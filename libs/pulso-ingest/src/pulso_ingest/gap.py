"""Deteccao de gap de sequencia no order book (pilar anti-toy #4).

Um order book incremental so e reconstruivel se nenhum delta se perde. Cada
delta carrega `[first_update_id, final_update_id]`; a continuidade exige

    final_update_id(anterior) + 1 == first_update_id(atual)

Um buraco (mensagem perdida, ou lacuna apos reconexao) significa que o book
local divergiu e precisa de re-sincronizacao via snapshot. Aqui o detectamos e
o reportamos fail-loud (metrica + log); a re-sync do book vem no consumo.

O detector e por **stream key** — uma string opaca decidida pelo client. Binance
usa `(exchange, symbol)` (sequencia por simbolo); Coinbase usa a sequencia
global da conexao. Sincrono e sem I/O, para teste direto.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class GapCheck:
    """Resultado de observar um delta.

    `is_gap` True quando ha descontinuidade. `missing` = quantos update_ids
    ficaram faltando (>=1). `first_seen` marca o primeiro delta de uma stream
    (ou o primeiro apos reset), onde nao ha como afirmar gap.
    """

    is_gap: bool
    missing: int = 0
    first_seen: bool = False


@dataclass
class OrderBookGapDetector:
    """Rastreia o ultimo `final_update_id` por stream e checa continuidade."""

    _last_final: dict[str, int] = field(default_factory=dict)

    def observe(self, stream_key: str, first_update_id: int, final_update_id: int) -> GapCheck:
        """Registra um delta e diz se houve gap em relacao ao anterior.

        Tolera reenvios idempotentes (delta repetido ou contido no ja visto):
        nao sao gap nem avancam o estado.
        """
        last = self._last_final.get(stream_key)
        self._last_final[stream_key] = final_update_id

        if last is None:
            return GapCheck(is_gap=False, first_seen=True)

        expected = last + 1
        if first_update_id == expected:
            return GapCheck(is_gap=False)
        if first_update_id <= last:
            # Sobreposicao/reenvio: ja vimos esse intervalo. Nao regride o estado.
            self._last_final[stream_key] = max(last, final_update_id)
            return GapCheck(is_gap=False)
        # first_update_id > expected => buraco.
        return GapCheck(is_gap=True, missing=first_update_id - expected)

    def reset(self, stream_key: str) -> None:
        """Esquece o estado de uma stream (ex.: apos reconexao deliberada)."""
        self._last_final.pop(stream_key, None)
