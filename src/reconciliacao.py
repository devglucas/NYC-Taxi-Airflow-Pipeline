"""Compara cada mês com o relatório mensal que a TLC publica. É o gabarito dos dados reais."""
from __future__ import annotations

import calendar
import csv
from pathlib import Path

from src.config import DIVERGENCIA_OFICIAL_ALERTA


def ler_relatorio_oficial(caminho: Path) -> dict[str, int]:
    """Viagens por dia dos táxis amarelos, por mês, como a TLC publica."""
    oficial: dict[str, int] = {}
    with caminho.open(encoding="utf-8-sig", newline="") as arquivo:
        for linha in csv.DictReader(arquivo):
            if linha["License Class"].strip() == "Yellow":
                oficial[linha["Month/Year"].strip()] = int(linha["Trips Per Day"].replace(",", ""))
    return oficial


def reconciliar(metricas: list[dict], oficial: dict[str, int]) -> list[dict]:
    linhas = []
    for item in sorted(metricas, key=lambda m: m["mes"]):
        ano, mes = map(int, item["mes"].split("-"))
        dias = calendar.monthrange(ano, mes)[1]
        calculado = item["viagens"] / dias
        bruto = item["linhas_bruto"] / dias
        referencia = oficial.get(item["mes"])
        diferenca = calculado / referencia - 1 if referencia else None
        estado = "pendente" if referencia is None else (
            "ok" if abs(diferenca) <= DIVERGENCIA_OFICIAL_ALERTA else "divergente"
        )
        linhas.append(
            {
                "mes": item["mes"],
                "viagens_dia_pipeline": round(calculado, 1),
                "viagens_dia_oficial": referencia,
                "diferenca_pct": round(diferenca * 100, 2) if diferenca is not None else None,
                "diferenca_sem_tratamento_pct": round((bruto / referencia - 1) * 100, 2) if referencia else None,
                "estado": estado,
            }
        )
    return linhas
