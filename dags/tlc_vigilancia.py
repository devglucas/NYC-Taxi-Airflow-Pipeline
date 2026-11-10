"""Vigia semanal: a TLC republica meses antigos sem aviso, e o relatório oficial sai com atraso.

Compara o ETag de cada mês já baixado, dispara `tlc_mensal` só para os que mudaram e refaz a
conciliação com o relatório oficial.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, task

from src import alertas, controle, fonte, reconciliacao
from src.config import URL_RELATORIO_OFICIAL, Caminhos, url_do_mes


def _avisar_falha(contexto: dict) -> None:
    instancia = contexto["ti"]
    alertas.registrar(
        Caminhos.do_ambiente().alertas, "erro", f"{instancia.dag_id}.{instancia.task_id}",
        str(contexto.get("exception"))[:500], id_execucao=instancia.run_id,
    )


with DAG(
    dag_id="tlc_vigilancia",
    description="Detecta meses republicados e concilia com o relatório oficial",
    schedule="0 7 * * 1",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["tlc", "monitoramento"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": _avisar_falha,
    },
) as dag:

    @task
    def detectar_republicacoes() -> list[dict]:
        caminhos = Caminhos.do_ambiente()
        mudancas = []
        for estado in controle.todos_os_arquivos(caminhos.controle):
            consulta = fonte.consultar(url_do_mes(estado["mes"]))
            if consulta.disponivel and consulta.etag != estado.get("etag"):
                mudancas.append({"mes": estado["mes"]})
                alertas.registrar(
                    caminhos.alertas, "aviso", "tlc_vigilancia", f"{estado['mes']} foi republicado",
                    etag_anterior=estado.get("etag"), etag_novo=consulta.etag,
                )
        return mudancas

    @task
    def conciliar_com_oficial() -> dict:
        caminhos = Caminhos.do_ambiente()
        arquivo = fonte.baixar_texto(URL_RELATORIO_OFICIAL, caminhos.dados / "referencia" / "data_reports_monthly.csv")
        linhas = reconciliacao.reconciliar(
            controle.todas_as_metricas(caminhos.controle), reconciliacao.ler_relatorio_oficial(arquivo)
        )
        controle.gravar_json(caminhos.controle / "reconciliacao.json", {"meses": linhas})
        divergentes = [l for l in linhas if l["estado"] == "divergente"]
        if divergentes:
            alertas.registrar(
                caminhos.alertas, "alerta", "tlc_vigilancia.conciliacao",
                f"{len(divergentes)} meses fora da tolerância",
                meses=[l["mes"] for l in divergentes],
            )
        estados = [l["estado"] for l in linhas]
        return {estado: estados.count(estado) for estado in set(estados)}

    mudancas = detectar_republicacoes()
    TriggerDagRunOperator.partial(
        task_id="reprocessar_mes", trigger_dag_id="tlc_mensal", wait_for_completion=False
    ).expand(conf=mudancas)
    conciliar_com_oficial()
