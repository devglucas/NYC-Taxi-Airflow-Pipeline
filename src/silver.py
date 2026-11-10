"""Transforma o Parquet bruto de um mês em silver e quarentena, sem perder linhas.

Duas decisões vieram da exploração dos dados reais:

- O fornecedor 7 (Helix) grava embarque e desembarque com o mesmo horário em todas as
  viagens. A viagem existe e foi cobrada, então fica na silver, marcada com `duracao_valida`
  falsa, e sai só das médias de duração.
- Linhas com total negativo são estornos: quase todas têm uma viagem positiva de mesmo
  horário, zona e valor. Somá-las contaria a viagem como anulada duas vezes na contagem.

- O fornecedor 1 envia parte das corridas duas vezes: um registro de app, com `request_source`,
  e outro de taxímetro, com o mesmo horário, trajeto e valores. Contar os dois infla o volume.
  Fica o registro de app, o único que informa o canal, com desempate por todas as colunas para
  que o mesmo arquivo gere sempre o mesmo resultado.
"""
from __future__ import annotations

import os
import shutil
import uuid
from pathlib import Path

import duckdb

from src.config import LIMITE_DISTANCIA_MILHAS, LIMITE_DURACAO_HORAS, LIMITE_VALOR_USD, validar_mes
from src.contrato import OBRIGATORIAS, OPCIONAIS

# Prefixos dos códigos de licença em request_source: HV é app de alto volume, EH é e-hail.
CANAL = """case
    when request_source is null then 'Rua ou ponto'
    when request_source like 'HV%' then 'App de alto volume'
    else 'Outros apps'
end"""

CHAVE_CORRIDA = ["vendorid", "pickup", "dropoff", "pulocationid", "dolocationid", "trip_distance", "fare_amount", "total_amount"]

COLUNAS_NORMALIZADAS = [
    {"tpep_pickup_datetime": "pickup", "tpep_dropoff_datetime": "dropoff"}.get(nome, nome)
    for nome in [*OBRIGATORIAS, *OPCIONAIS]
]


def _selecao_normalizada(colunas: list[str]) -> str:
    """Padroniza nomes e preenche com nulo as colunas que o mês ainda não tinha."""
    por_nome = {nome.lower(): nome for nome in colunas}
    tipos = {
        "vendorid": "integer", "passenger_count": "integer", "ratecodeid": "integer",
        "pulocationid": "integer", "dolocationid": "integer", "payment_type": "integer",
        "store_and_fwd_flag": "varchar", "request_source": "varchar",
        "tpep_pickup_datetime": "timestamp", "tpep_dropoff_datetime": "timestamp",
    }
    partes = []
    for nome in [*OBRIGATORIAS, *OPCIONAIS]:
        tipo = tipos.get(nome, "double")
        apelido = {"tpep_pickup_datetime": "pickup", "tpep_dropoff_datetime": "dropoff"}.get(nome, nome)
        origem = f'try_cast("{por_nome[nome]}" as {tipo})' if nome in por_nome else f"cast(null as {tipo})"
        partes.append(f"{origem} as {apelido}")
    return ", ".join(partes)


def _substituir_pasta(temporaria: Path, final: Path) -> None:
    """Troca a partição inteira; quem lê nunca vê metade de um mês."""
    final.parent.mkdir(parents=True, exist_ok=True)
    antiga = final.with_name(final.name + f".antiga-{uuid.uuid4().hex[:8]}")
    if final.exists():
        os.replace(final, antiga)
    os.replace(temporaria, final)
    shutil.rmtree(antiga, ignore_errors=True)


def processar(bruto: Path, mes: str, silver: Path, quarentena: Path, limite_memoria: str = "2GB") -> dict:
    validar_mes(mes)
    ano, numero_mes = map(int, mes.split("-"))
    inicio = f"{ano:04d}-{numero_mes:02d}-01"
    id_execucao = uuid.uuid4().hex[:8]
    destino_silver = silver / f"mes={mes}"
    destino_quarentena = quarentena / f"mes={mes}"
    tmp_silver = silver / f".tmp-{mes}-{id_execucao}"
    tmp_quarentena = quarentena / f".tmp-{mes}-{id_execucao}"
    tmp_duck = silver / f".duck-{mes}-{id_execucao}"
    for pasta in (tmp_silver, tmp_quarentena, tmp_duck):
        pasta.mkdir(parents=True)
    try:
        with duckdb.connect() as con:
            con.execute(f"set memory_limit='{limite_memoria}'; set threads=2; set temp_directory='{tmp_duck}'")
            colunas = con.read_parquet(str(bruto)).columns
            con.execute(
                f"""create temp table classificado as
                with normalizado as (select {_selecao_normalizada(colunas)} from read_parquet('{bruto}')),
                numerado as (
                    select *, row_number() over (
                        partition by {", ".join(CHAVE_CORRIDA)}
                        order by request_source nulls last,
                            {", ".join(c + " nulls last" for c in COLUNAS_NORMALIZADAS if c not in CHAVE_CORRIDA and c != "request_source")}
                    ) as ocorrencia from normalizado
                )
                select * exclude (ocorrencia),
                    case
                        when pickup is null or dropoff is null then 'horario_ausente'
                        when pickup < date '{inicio}' or pickup >= date '{inicio}' + interval 1 month then 'fora_do_mes'
                        when dropoff < pickup then 'desembarque_antes_do_embarque'
                        when dropoff > pickup + interval {LIMITE_DURACAO_HORAS} hour then 'duracao_acima_do_limite'
                        when total_amount is null then 'valor_ausente'
                        when total_amount < 0 then 'estorno'
                        when total_amount > {LIMITE_VALOR_USD} then 'valor_acima_do_limite'
                        when trip_distance is null or trip_distance < 0
                             or trip_distance > {LIMITE_DISTANCIA_MILHAS} then 'distancia_invalida'
                        when pulocationid is null or dolocationid is null
                             or pulocationid not between 1 and 265
                             or dolocationid not between 1 and 265 then 'zona_invalida'
                        when ocorrencia > 1 then 'corrida_em_duplicidade'
                    end as motivo_rejeicao
                from numerado"""
            )
            con.execute(
                f"""copy (
                    select * exclude (motivo_rejeicao),
                        '{mes}' as mes,
                        cast(pickup as date) as dia,
                        {CANAL} as canal,
                        date_diff('second', pickup, dropoff) / 60.0 as duracao_min,
                        dropoff > pickup as duracao_valida,
                        abs(total_amount - (fare_amount + extra + mta_tax + tip_amount + tolls_amount
                            + improvement_surcharge + coalesce(congestion_surcharge, 0)
                            + coalesce(airport_fee, 0) + coalesce(cbd_congestion_fee, 0))) <= 0.05 as soma_confere
                    from classificado where motivo_rejeicao is null
                ) to '{tmp_silver}/viagens.parquet' (format parquet, compression zstd)"""
            )
            con.execute(
                f"""copy (select *, '{mes}' as mes from classificado where motivo_rejeicao is not null)
                to '{tmp_quarentena}/rejeitados.parquet' (format parquet, compression zstd)"""
            )
            motivos = dict(
                con.execute(
                    "select motivo_rejeicao, count(*) from classificado "
                    "where motivo_rejeicao is not null group by 1 order by 2 desc"
                ).fetchall()
            )
            bruto_linhas = con.execute(f"select count(*) from read_parquet('{bruto}')").fetchall()[0][0]
            colunas_resultado = (
                "viagens", "duracao_invalida", "soma_diverge", "fornecedor_desconhecido", "receita_total",
                "primeiro_embarque", "ultimo_embarque", "fora_de_mes_na_silver",
            )
            resumo = con.execute(
                f"""select count(*), count(*) filter (not duracao_valida), count(*) filter (not soma_confere),
                    count(*) filter (vendorid not in (1, 2, 6, 7)), round(sum(total_amount), 2),
                    min(pickup), max(pickup),
                    count(*) filter (pickup < date '{inicio}' or pickup >= date '{inicio}' + interval 1 month)
                from read_parquet('{tmp_silver}/viagens.parquet')"""
            ).fetchall()[0]
        _substituir_pasta(tmp_silver, destino_silver)
        _substituir_pasta(tmp_quarentena, destino_quarentena)
    finally:
        for pasta in (tmp_silver, tmp_quarentena, tmp_duck):
            shutil.rmtree(pasta, ignore_errors=True)
    return {
        "mes": mes,
        "linhas_bruto": bruto_linhas,
        "linhas_quarentena": sum(motivos.values()),
        "motivos": motivos,
        **dict(zip(colunas_resultado, resumo)),
    }
