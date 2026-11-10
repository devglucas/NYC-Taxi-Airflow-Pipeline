"""Verificações antes de publicar. Críticas barram o mês; alertas publicam e avisam."""
from __future__ import annotations

from statistics import median

from src.config import TAXA_QUARENTENA_CRITICA, VARIACAO_VOLUME_ALERTA


def _verificacao(nome: str, nivel: str, passou: bool, detalhe: str) -> dict:
    return {"verificacao": nome, "nivel": nivel, "passou": bool(passou), "detalhe": detalhe}


def avaliar(metricas: dict, historico: list[dict], colunas_novas: list[str] | None = None) -> list[dict]:
    bruto = metricas["linhas_bruto"]
    viagens = metricas["viagens"]
    quarentena = metricas["linhas_quarentena"]
    taxa = quarentena / bruto if bruto else 1.0
    resultado = [
        _verificacao(
            "conservacao_de_linhas", "critica", bruto == viagens + quarentena,
            f"bruto {bruto} = silver {viagens} + quarentena {quarentena}",
        ),
        _verificacao("silver_nao_vazia", "critica", viagens > 0, f"{viagens} viagens"),
        _verificacao(
            "silver_dentro_do_mes", "critica", metricas["fora_de_mes_na_silver"] == 0,
            f"{metricas['fora_de_mes_na_silver']} linhas fora do mês",
        ),
        _verificacao(
            "taxa_de_quarentena", "critica", taxa <= TAXA_QUARENTENA_CRITICA,
            f"{taxa:.2%} (limite {TAXA_QUARENTENA_CRITICA:.0%})",
        ),
        _verificacao(
            "fornecedor_no_dicionario", "alerta", metricas["fornecedor_desconhecido"] == 0,
            f"{metricas['fornecedor_desconhecido']} viagens de fornecedor fora do dicionário",
        ),
        _verificacao(
            "colunas_novas_na_fonte", "alerta", not colunas_novas, f"novas: {colunas_novas or []}",
        ),
    ]
    anteriores = sorted(
        (m for m in historico if m["mes"] < metricas["mes"]), key=lambda m: m["mes"]
    )[-12:]
    if len(anteriores) >= 3:
        referencia = median(m["viagens"] for m in anteriores)
        variacao = viagens / referencia - 1
        resultado.append(
            _verificacao(
                "volume_contra_historico", "alerta", abs(variacao) <= VARIACAO_VOLUME_ALERTA,
                f"{variacao:+.1%} contra a mediana de {len(anteriores)} meses anteriores",
            )
        )
    return resultado


def criticas_reprovadas(verificacoes: list[dict]) -> list[dict]:
    return [v for v in verificacoes if v["nivel"] == "critica" and not v["passou"]]


def alertas_reprovados(verificacoes: list[dict]) -> list[dict]:
    return [v for v in verificacoes if v["nivel"] == "alerta" and not v["passou"]]
