"""Integridade das DAGs. Só roda onde o Airflow existe, ou seja, dentro do contêiner."""
from datetime import datetime, timezone
from pathlib import Path

import pytest

pytest.importorskip("airflow")

from airflow.dag_processing.dagbag import DagBag  # noqa: E402
from airflow.sdk.exceptions import AirflowFailException  # noqa: E402

from dags.tlc_mensal import resolver_mes_do_contexto  # noqa: E402


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(Path(__file__).parent.parent / "dags"))


def test_sem_erros_de_importacao(dagbag):
    assert dagbag.import_errors == {}
    assert set(dagbag.dags) == {"tlc_mensal", "tlc_vigilancia", "tlc_relatorio"}


def test_ordem_das_tarefas(dagbag):
    dag = dagbag.dags["tlc_mensal"]
    ordem = [
        "resolver_mes", "aguardar_publicacao", "baixar_para_staging", "verificar_contrato",
        "precisa_processar", "transformar", "verificar_qualidade", "publicar",
    ]
    for anterior, seguinte in zip(ordem, ordem[1:]):
        assert seguinte in dag.get_task(anterior).downstream_task_ids


def test_politicas_da_dag_mensal(dagbag):
    dag = dagbag.dags["tlc_mensal"]
    assert dag.catchup is False
    assert dag.get_task("aguardar_publicacao").mode == "reschedule"
    assert dag.get_task("baixar_para_staging").pool == "downloads_tlc"
    assert dag.get_task("publicar").pool == "warehouse"
    assert dag.get_task("baixar_para_staging").retries == 3
    assert dag.get_task("verificar_qualidade").retries == 3


def test_agenda_tem_intervalo_de_dados(dagbag):
    # Sem intervalo, a execução de novembro apontaria para novembro, e não para outubro.
    assert type(dagbag.dags["tlc_mensal"].timetable).__name__ == "CronDataIntervalTimetable"


def test_mes_explicito_tem_prioridade():
    contexto = {"params": {"mes": "2025-03"}, "data_interval_start": datetime(2026, 1, 1, tzinfo=timezone.utc)}
    assert resolver_mes_do_contexto(contexto) == "2025-03"


def test_mes_vem_do_intervalo():
    contexto = {"params": {"mes": None}, "data_interval_start": datetime(2026, 1, 1, tzinfo=timezone.utc)}
    assert resolver_mes_do_contexto(contexto) == "2026-01"


def test_execucao_manual_sem_mes_falha_sem_repetir():
    with pytest.raises(AirflowFailException):
        resolver_mes_do_contexto({"params": {"mes": None}, "data_interval_start": None})
