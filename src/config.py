"""Endereços, caminhos e limites do pipeline."""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

URL_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"
URL_ZONAS = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
URL_RELATORIO_OFICIAL = "https://www.nyc.gov/assets/tlc/downloads/csv/data_reports_monthly.csv"
USER_AGENT = "data-engineering-portfolio"

# Códigos do dicionário de dados da TLC (versão de março de 2025).
FORNECEDORES = {1: "Creative Mobile", 2: "Curb Mobility", 6: "Myle", 7: "Helix"}
PAGAMENTOS = {
    0: "Tarifa flexível", 1: "Cartão", 2: "Dinheiro", 3: "Sem cobrança",
    4: "Contestada", 5: "Desconhecido", 6: "Anulada",
}

LIMITE_DURACAO_HORAS = 24
LIMITE_DISTANCIA_MILHAS = 200
LIMITE_VALOR_USD = 1000

TAXA_QUARENTENA_CRITICA = 0.10
VARIACAO_VOLUME_ALERTA = 0.40
DIVERGENCIA_OFICIAL_ALERTA = 0.03

_FORMATO_MES = re.compile(r"^20\d{2}-(0[1-9]|1[0-2])$")


def validar_mes(mes: str) -> str:
    if not _FORMATO_MES.match(mes or ""):
        raise ValueError(f"mês inválido: {mes!r}; use AAAA-MM")
    return mes


def url_do_mes(mes: str, base: str | None = None) -> str:
    return f"{(base or URL_BASE).rstrip('/')}/yellow_tripdata_{validar_mes(mes)}.parquet"


@dataclass(frozen=True)
class Caminhos:
    dados: Path
    output: Path

    @classmethod
    def do_ambiente(cls) -> "Caminhos":
        raiz = Path(__file__).resolve().parent.parent
        return cls(
            Path(os.getenv("DADOS_DIR", raiz / "dados")),
            Path(os.getenv("OUTPUT_DIR", raiz / "output")),
        )

    @property
    def staging(self) -> Path:
        return self.dados / "staging"

    @property
    def raw(self) -> Path:
        return self.dados / "raw"

    @property
    def silver(self) -> Path:
        return self.dados / "silver"

    @property
    def quarentena(self) -> Path:
        return self.dados / "quarentena"

    @property
    def controle(self) -> Path:
        return self.dados / "controle"

    @property
    def warehouse(self) -> Path:
        return self.dados / "warehouse.duckdb"

    @property
    def alertas(self) -> Path:
        return self.dados / "alertas.jsonl"

    @property
    def zonas(self) -> Path:
        return self.dados / "referencia" / "taxi_zone_lookup.csv"
