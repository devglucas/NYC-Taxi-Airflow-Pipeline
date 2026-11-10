"""O mesmo pipeline mensal em Prefect, reaproveitando `src/`, para comparar com o Airflow.

Uso, a partir da raiz do projeto:
    .venv-prefect/bin/python comparacao_prefect/fluxo.py 2026-09
"""
from __future__ import annotations

import sys
from pathlib import Path

from prefect import flow, get_run_logger, task

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src import contrato, controle, fonte, qualidade, silver, warehouse  # noqa: E402
from src.config import URL_ZONAS, Caminhos, url_do_mes  # noqa: E402

CAMINHOS = Caminhos(dados=Path("dados/prefect"), output=Path("dados/prefect/output"))


@task(retries=3, retry_delay_seconds=[30, 60, 120])
def baixar(mes: str) -> dict:
    consulta = fonte.consultar(url_do_mes(mes))
    if not consulta.disponivel:
        # Não há sensor: "ainda não publicado" vira uma falha para a política de repetição.
        raise fonte.FonteInstavel(f"{mes} ainda não publicado")
    return fonte.baixar(consulta.url, CAMINHOS.raw / f"yellow_tripdata_{mes}.parquet")


@task
def verificar_contrato(mes: str) -> list[str]:
    return contrato.exigir(CAMINHOS.raw / f"yellow_tripdata_{mes}.parquet").novas


@task(retries=1)
def transformar(mes: str) -> dict:
    return silver.processar(CAMINHOS.raw / f"yellow_tripdata_{mes}.parquet", mes, CAMINHOS.silver, CAMINHOS.quarentena)


@task
def verificar_qualidade(metricas: dict, novas: list[str]) -> None:
    verificacoes = qualidade.avaliar(metricas, [], novas)
    criticas = qualidade.criticas_reprovadas(verificacoes)
    if criticas:
        raise ValueError(f"{metricas['mes']}: {criticas}")


@task(retries=2, retry_delay_seconds=10)
def publicar(metricas: dict, sha256: str, mes: str) -> dict:
    if not CAMINHOS.zonas.exists():
        fonte.baixar_texto(URL_ZONAS, CAMINHOS.zonas)
    warehouse.carregar_zonas(CAMINHOS.warehouse, CAMINHOS.zonas)
    resultado = warehouse.publicar(CAMINHOS.warehouse, CAMINHOS.silver, metricas, sha256, f"prefect-{mes}")
    controle.gravar_json(CAMINHOS.controle / f"metricas_{mes}.json", metricas)
    return resultado


@flow(name="tlc-mensal", log_prints=True)
def tlc_mensal(mes: str) -> dict:
    baixado = baixar(mes)
    novas = verificar_contrato(mes)
    metricas = transformar(mes)
    verificar_qualidade(metricas, novas)
    resultado = publicar(metricas, baixado["sha256"], mes)
    get_run_logger().info("publicado: %s", resultado)
    return metricas


if __name__ == "__main__":
    print(tlc_mensal(sys.argv[1]))
