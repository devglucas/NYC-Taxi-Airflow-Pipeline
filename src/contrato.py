"""Contrato do arquivo mensal. Quebra de contrato não adianta repetir: exige uma pessoa.

Os nomes são comparados sem diferenciar maiúsculas, porque a própria fonte alterna
`Airport_fee` e `airport_fee` entre os anos.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path

import duckdb

INTEIRO = {"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "DOUBLE", "FLOAT"}
NUMERO = INTEIRO | {"DECIMAL"}
DATA_HORA = {"TIMESTAMP", "TIMESTAMP_NS", "TIMESTAMP_MS", "TIMESTAMP_S", "TIMESTAMP WITH TIME ZONE"}
TEXTO = {"VARCHAR"}

OBRIGATORIAS = {
    "vendorid": INTEIRO, "tpep_pickup_datetime": DATA_HORA, "tpep_dropoff_datetime": DATA_HORA,
    "passenger_count": NUMERO, "trip_distance": NUMERO, "ratecodeid": NUMERO,
    "store_and_fwd_flag": TEXTO, "pulocationid": INTEIRO, "dolocationid": INTEIRO,
    "payment_type": INTEIRO, "fare_amount": NUMERO, "extra": NUMERO, "mta_tax": NUMERO,
    "tip_amount": NUMERO, "tolls_amount": NUMERO, "improvement_surcharge": NUMERO,
    "total_amount": NUMERO, "congestion_surcharge": NUMERO, "airport_fee": NUMERO,
}
# Surgiram depois de 2024; meses antigos não as têm, e isso não é erro.
OPCIONAIS = {"cbd_congestion_fee": NUMERO, "request_source": TEXTO}


class ContratoViolado(Exception):
    pass


@dataclass
class ResultadoContrato:
    linhas: int = 0
    faltando: list[str] = field(default_factory=list)
    tipos_errados: dict[str, str] = field(default_factory=dict)
    novas: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.faltando and not self.tipos_errados and self.linhas > 0

    def como_dict(self) -> dict:
        return asdict(self) | {"ok": self.ok}


def _familia(tipo: str) -> str:
    return "DECIMAL" if tipo.startswith("DECIMAL") else tipo


def verificar(parquet: Path) -> ResultadoContrato:
    with duckdb.connect() as conexao:
        origem = conexao.read_parquet(str(parquet))
        colunas = {nome.lower(): _familia(str(tipo)) for nome, tipo in zip(origem.columns, origem.types)}
        linhas = conexao.execute("select count(*) from read_parquet(?)", [str(parquet)]).fetchall()[0][0]
    resultado = ResultadoContrato(linhas=linhas)
    resultado.faltando = sorted(set(OBRIGATORIAS) - set(colunas))
    for nome, aceitos in (OBRIGATORIAS | OPCIONAIS).items():
        if nome in colunas and colunas[nome] not in aceitos:
            resultado.tipos_errados[nome] = colunas[nome]
    resultado.novas = sorted(set(colunas) - set(OBRIGATORIAS) - set(OPCIONAIS))
    return resultado


def exigir(parquet: Path) -> ResultadoContrato:
    resultado = verificar(parquet)
    if not resultado.ok:
        raise ContratoViolado(
            f"{parquet.name}: faltando={resultado.faltando} "
            f"tipos_errados={resultado.tipos_errados} linhas={resultado.linhas}"
        )
    return resultado
