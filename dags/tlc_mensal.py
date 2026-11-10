"""Ingestão mensal das viagens de táxi amarelo de Nova York.

A TLC publica cada mês com cerca de dois meses de atraso e sem data certa, então a execução
do mês espera o arquivo com um sensor em modo reschedule, que não ocupa vaga enquanto aguarda.
A lógica fica em `src/`: esta DAG só decide ordem, repetição e o que fazer em cada falha.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import DAG, Asset, CronDataIntervalTimetable, Param, PokeReturnValue, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

from src import alertas, contrato, controle, fonte, qualidade, silver, warehouse
from src.config import URL_ZONAS, Caminhos, url_do_mes, validar_mes

WAREHOUSE = Asset("tlc_warehouse")


def resolver_mes_do_contexto(contexto: dict) -> str:
    """Mês explícito em `params` ou o início do intervalo de dados da execução."""
    mes = contexto["params"].get("mes")
    if mes:
        return validar_mes(mes)
    inicio = contexto.get("data_interval_start")
    if inicio is None:
        raise AirflowFailException("execução manual sem data: informe o parâmetro mes")
    return inicio.strftime("%Y-%m")


def avisar_falha(contexto: dict) -> None:
    instancia = contexto["ti"]
    alertas.registrar(
        Caminhos.do_ambiente().alertas, "erro", f"{instancia.dag_id}.{instancia.task_id}",
        str(contexto.get("exception"))[:500], id_execucao=instancia.run_id, tentativa=instancia.try_number,
    )


def avisar_nova_tentativa(contexto: dict) -> None:
    instancia = contexto["ti"]
    alertas.registrar(
        Caminhos.do_ambiente().alertas, "aviso", f"{instancia.dag_id}.{instancia.task_id}",
        f"nova tentativa: {str(contexto.get('exception'))[:300]}",
        id_execucao=instancia.run_id, tentativa=instancia.try_number,
    )


with DAG(
    dag_id="tlc_mensal",
    description="Espera, baixa, valida e publica um mês de viagens de táxi",
    # Com intervalo de dados, a execução de 1º de novembro cuida de outubro. O cron simples do
    # Airflow 3 não tem intervalo e apontaria para o próprio novembro.
    schedule=CronDataIntervalTimetable("0 6 1 * *", timezone="UTC"),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=4,
    tags=["tlc", "ingestao"],
    params={
        "mes": Param(None, type=["null", "string"], description="AAAA-MM; vazio usa o intervalo da execução"),
        "forcar": Param(False, type="boolean", description="Reprocessa o arquivo já baixado mesmo sem mudança"),
        "rebaixar": Param(False, type="boolean", description="Baixa de novo mesmo com o ETag igual"),
        "base_url": Param(None, type=["null", "string"], description="Outra origem, para testes de falha"),
    },
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=10),
        "execution_timeout": timedelta(minutes=30),
        "on_failure_callback": avisar_falha,
        "on_retry_callback": avisar_nova_tentativa,
    },
) as dag:

    @task
    def resolver_mes() -> str:
        return resolver_mes_do_contexto(get_current_context())

    @task.sensor(poke_interval=timedelta(hours=6).total_seconds(), timeout=timedelta(days=75).total_seconds(), mode="reschedule")
    def aguardar_publicacao(mes: str) -> PokeReturnValue:
        url = url_do_mes(mes, get_current_context()["params"].get("base_url"))
        consulta = fonte.consultar(url)
        return PokeReturnValue(is_done=consulta.disponivel, xcom_value=consulta.como_dict())

    @task(pool="downloads_tlc", execution_timeout=timedelta(minutes=20))
    def baixar_para_staging(mes: str, consulta: dict) -> dict:
        caminhos = Caminhos.do_ambiente()
        estado = controle.ler_json(controle.arquivo_do_mes(caminhos.controle, mes)) or {}
        bruto = caminhos.raw / f"yellow_tripdata_{mes}.parquet"
        rebaixar = get_current_context()["params"]["rebaixar"]
        if not rebaixar and bruto.exists() and consulta["etag"] and estado.get("etag") == consulta["etag"]:
            return {"mes": mes, "novo": False, "consulta": consulta, "sha256": estado["sha256"]}
        # Nome único por execução: duas execuções do mesmo mês não disputam o mesmo arquivo.
        staging = caminhos.staging / f"yellow_tripdata_{mes}.{uuid.uuid4().hex[:8]}.parquet"
        recebido = fonte.baixar(consulta["url"], staging)
        return {"mes": mes, "novo": True, "consulta": consulta, "staging": str(staging), **recebido}

    @task
    def verificar_contrato(baixado: dict) -> dict:
        """Só promove para raw o que cumpre o contrato: um arquivo ruim não apaga o anterior."""
        caminhos = Caminhos.do_ambiente()
        mes = baixado["mes"]
        if not baixado["novo"]:
            return baixado | {"colunas_novas": []}
        staging = Path(baixado["staging"])
        try:
            resultado = contrato.exigir(staging)
        except contrato.ContratoViolado as erro:
            # O arquivo rejeitado fica guardado para análise e não substitui o anterior.
            rejeitados = caminhos.dados / "rejeitados"
            rejeitados.mkdir(parents=True, exist_ok=True)
            staging.replace(rejeitados / f"{get_current_context()['run_id']}.parquet")
            raise AirflowFailException(str(erro)) from erro
        caminhos.raw.mkdir(parents=True, exist_ok=True)
        staging.replace(caminhos.raw / f"yellow_tripdata_{mes}.parquet")
        controle.gravar_json(
            controle.arquivo_do_mes(caminhos.controle, mes),
            {
                "mes": mes, "url": baixado["consulta"]["url"], "etag": baixado["consulta"]["etag"],
                "ultima_modificacao": baixado["consulta"]["ultima_modificacao"],
                "sha256": baixado["sha256"], "tamanho": baixado["tamanho"], "linhas": baixado["linhas"],
                "baixado_em": datetime.now().astimezone().isoformat(timespec="seconds"),
            },
        )
        return baixado | {"colunas_novas": resultado.novas}

    @task.short_circuit
    def precisa_processar(verificado: dict) -> bool:
        """Mesmo arquivo já publicado: encerra aqui, sem trabalho repetido."""
        caminhos = Caminhos.do_ambiente()
        publicado = controle.ler_json(caminhos.controle / f"publicado_{verificado['mes']}.json") or {}
        if get_current_context()["params"]["forcar"]:
            return True
        return publicado.get("sha256") != verificado["sha256"]

    @task(max_active_tis_per_dag=3, execution_timeout=timedelta(minutes=20))
    def transformar(verificado: dict) -> dict:
        caminhos = Caminhos.do_ambiente()
        mes = verificado["mes"]
        metricas = silver.processar(
            caminhos.raw / f"yellow_tripdata_{mes}.parquet", mes, caminhos.silver, caminhos.quarentena
        )
        controle.gravar_json(controle.metricas_do_mes(caminhos.controle, mes), metricas)
        return metricas

    # Reprovação de qualidade usa AirflowFailException e não se repete. As tentativas ficam
    # para falhas de infraestrutura, como o token expirado depois que a máquina dormiu.
    @task
    def verificar_qualidade(metricas: dict, verificado: dict) -> None:
        caminhos = Caminhos.do_ambiente()
        contexto = get_current_context()
        historico = controle.todas_as_metricas(caminhos.controle)
        verificacoes = qualidade.avaliar(metricas, historico, verificado["colunas_novas"])
        controle.gravar_json(
            caminhos.controle / f"qualidade_{metricas['mes']}.json",
            {"mes": metricas["mes"], "verificacoes": verificacoes},
        )
        for falha in qualidade.alertas_reprovados(verificacoes):
            alertas.registrar(
                caminhos.alertas, "alerta", f"tlc_mensal.qualidade.{falha['verificacao']}", falha["detalhe"],
                mes=metricas["mes"], id_execucao=contexto["run_id"],
            )
        criticas = qualidade.criticas_reprovadas(verificacoes)
        if criticas:
            raise AirflowFailException(f"{metricas['mes']}: {criticas}")

    @task(pool="warehouse", outlets=[WAREHOUSE])
    def publicar(metricas: dict, verificado: dict) -> dict:
        caminhos = Caminhos.do_ambiente()
        mes = metricas["mes"]
        if not caminhos.zonas.exists():
            fonte.baixar_texto(URL_ZONAS, caminhos.zonas)
        warehouse.carregar_zonas(caminhos.warehouse, caminhos.zonas)
        id_execucao = get_current_context()["run_id"]
        resultado = warehouse.publicar(
            caminhos.warehouse, caminhos.silver, metricas, verificado["sha256"], id_execucao
        )
        controle.gravar_json(
            caminhos.controle / f"publicado_{mes}.json",
            {"mes": mes, "sha256": verificado["sha256"], "id_execucao": id_execucao},
        )
        return resultado

    mes = resolver_mes()
    consulta = aguardar_publicacao(mes)
    baixado = baixar_para_staging(mes, consulta)
    verificado = verificar_contrato(baixado)
    continuar = precisa_processar(verificado)
    metricas = transformar(verificado)
    checagem = verificar_qualidade(metricas, verificado)
    publicado = publicar(metricas, verificado)
    continuar >> metricas
    checagem >> publicado
