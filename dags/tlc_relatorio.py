"""Refaz tabelas e gráficos sempre que o warehouse recebe dados: agendamento por dado, não por hora."""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import DAG, Asset, task

from src import relatorio
from src.config import Caminhos

with DAG(
    dag_id="tlc_relatorio",
    description="Gera as saídas analíticas quando tlc_warehouse é atualizado",
    schedule=[Asset("tlc_warehouse")],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["tlc", "relatorio"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
) as dag:

    @task(pool="warehouse")
    def gerar_relatorio() -> dict:
        return relatorio.gerar(Caminhos.do_ambiente())

    gerar_relatorio()
