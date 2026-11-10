"""Publica os agregados de um mês no DuckDB, trocando a partição em uma única transação."""
from __future__ import annotations

from pathlib import Path

import duckdb

ESQUEMA = """
create table if not exists dim_zona (
    zona_id integer primary key, distrito varchar, zona varchar, area_servico varchar
);
create table if not exists viagens_dia (
    mes varchar not null, dia date not null, canal varchar not null, pagamento integer,
    viagens bigint not null, passageiros bigint, distancia_milhas double, tarifa_usd double,
    gorjeta_usd double, total_usd double, viagens_duracao_valida bigint, minutos_validos double,
    viagens_soma_diverge bigint
);
create table if not exists viagens_fornecedor_mes (
    mes varchar not null, fornecedor integer, viagens bigint not null, viagens_duracao_invalida bigint not null
);
create table if not exists viagens_origem_mes (
    mes varchar not null, zona_origem integer not null, viagens bigint not null, total_usd double
);
create table if not exists quarentena_mes (
    mes varchar not null, motivo varchar not null, linhas bigint not null
);
create table if not exists publicacoes (
    mes varchar primary key, sha256 varchar, linhas_bruto bigint, viagens bigint, id_execucao varchar,
    publicado_em timestamp
);
"""

TABELAS_DO_MES = ("viagens_dia", "viagens_fornecedor_mes", "viagens_origem_mes", "quarentena_mes")


def carregar_zonas(banco: Path, zonas_csv: Path) -> int:
    banco.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(banco)) as con:
        con.execute(ESQUEMA)
        con.execute(
            f"""insert or replace into dim_zona
            select "LocationID", "Borough", "Zone", service_zone from read_csv('{zonas_csv}', header=true)"""
        )
        return con.execute("select count(*) from dim_zona").fetchall()[0][0]


def publicar(banco: Path, silver: Path, metricas: dict, sha256: str | None, id_execucao: str) -> dict:
    mes = metricas["mes"]
    parquet = silver / f"mes={mes}" / "viagens.parquet"
    banco.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(banco)) as con:
        con.execute(ESQUEMA)
        con.execute("begin transaction")
        try:
            for tabela in TABELAS_DO_MES:
                con.execute(f"delete from {tabela} where mes = ?", [mes])
            con.execute("delete from publicacoes where mes = ?", [mes])
            con.execute(
                f"""insert into viagens_dia
                select mes, dia, canal, payment_type, count(*), sum(passenger_count), sum(trip_distance),
                    sum(fare_amount), sum(tip_amount), sum(total_amount),
                    count(*) filter (duracao_valida), sum(duracao_min) filter (duracao_valida),
                    count(*) filter (not soma_confere)
                from read_parquet('{parquet}') group by all"""
            )
            con.execute(
                f"""insert into viagens_fornecedor_mes
                select mes, vendorid, count(*), count(*) filter (not duracao_valida)
                from read_parquet('{parquet}') group by all"""
            )
            con.execute(
                f"""insert into viagens_origem_mes
                select mes, pulocationid, count(*), sum(total_amount)
                from read_parquet('{parquet}') group by all"""
            )
            for motivo, linhas in metricas["motivos"].items():
                con.execute("insert into quarentena_mes values (?, ?, ?)", [mes, motivo, linhas])
            con.execute(
                "insert into publicacoes values (?, ?, ?, ?, ?, now()::timestamp)",
                [mes, sha256, metricas["linhas_bruto"], metricas["viagens"], id_execucao],
            )
            soma = con.execute("select sum(viagens) from viagens_dia where mes = ?", [mes]).fetchall()[0][0]
            if soma != metricas["viagens"]:
                raise ValueError(f"{mes}: agregado soma {soma}, silver tem {metricas['viagens']}")
            con.execute("commit")
        except Exception:
            con.execute("rollback")
            raise
    return {"mes": mes, "viagens": soma}
