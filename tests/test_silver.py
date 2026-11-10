from datetime import datetime, timedelta

import duckdb
import pytest

from src import silver

BASE = datetime(2026, 9, 10, 8, 0)


def processar(arquivo, tmp_path, mes="2026-09"):
    return silver.processar(arquivo, mes, tmp_path / "silver", tmp_path / "quarentena")


def linhas_silver(tmp_path, mes="2026-09", colunas="*"):
    with duckdb.connect() as con:
        return con.execute(f"select {colunas} from '{tmp_path}/silver/mes={mes}/viagens.parquet'").fetchall()


@pytest.mark.parametrize(
    ("linha", "motivo"),
    [
        ({"tpep_pickup_datetime": datetime(2009, 1, 1), "tpep_dropoff_datetime": datetime(2009, 1, 1, 0, 10)}, "fora_do_mes"),
        ({"tpep_pickup_datetime": datetime(2026, 10, 1), "tpep_dropoff_datetime": datetime(2026, 10, 1, 0, 10)}, "fora_do_mes"),
        ({"tpep_dropoff_datetime": BASE - timedelta(minutes=1)}, "desembarque_antes_do_embarque"),
        ({"tpep_dropoff_datetime": BASE + timedelta(hours=25)}, "duracao_acima_do_limite"),
        ({"total_amount": -20.5}, "estorno"),
        ({"total_amount": 5000.0}, "valor_acima_do_limite"),
        ({"trip_distance": 232644.8}, "distancia_invalida"),
        ({"PULocationID": 999}, "zona_invalida"),
        ({"tpep_pickup_datetime": None}, "horario_ausente"),
    ],
)
def test_motivos_de_quarentena(escrever_parquet, tmp_path, linha, motivo):
    metricas = processar(escrever_parquet([{}, linha]), tmp_path)
    assert metricas["motivos"] == {motivo: 1}
    assert metricas["viagens"] == 1


def test_nenhuma_linha_se_perde(escrever_parquet, tmp_path):
    arquivo = escrever_parquet([{}, {"total_amount": -1.0}, {"PULocationID": 999}, {"trip_distance": 3.0}])
    metricas = processar(arquivo, tmp_path)
    assert metricas["linhas_bruto"] == 4 == metricas["viagens"] + metricas["linhas_quarentena"]
    with duckdb.connect() as con:
        quarentena = con.execute(f"select count(*) from '{tmp_path}/quarentena/mes=2026-09/rejeitados.parquet'").fetchall()
    assert quarentena[0][0] == 2


def test_linha_repetida_mantem_uma_viagem(escrever_parquet, tmp_path):
    metricas = processar(escrever_parquet([{}, {}, {}]), tmp_path)
    assert metricas["viagens"] == 1
    assert metricas["motivos"] == {"corrida_em_duplicidade": 2}


def test_mesma_corrida_em_dois_registros_mantem_o_de_app(escrever_parquet, tmp_path):
    arquivo = escrever_parquet(
        [
            {"request_source": None, "payment_type": 1, "extra": 3.25},
            {"request_source": "EH0004", "payment_type": 0, "extra": 0.0},
        ],
        extras={"request_source": None},
    )
    metricas = processar(arquivo, tmp_path)
    assert metricas["motivos"] == {"corrida_em_duplicidade": 1}
    assert linhas_silver(tmp_path, colunas="request_source, canal") == [("EH0004", "Outros apps")]


def test_mesma_chave_com_trajeto_diferente_nao_e_duplicidade(escrever_parquet, tmp_path):
    assert processar(escrever_parquet([{}, {"DOLocationID": 201}]), tmp_path)["viagens"] == 2


def test_mesmo_arquivo_gera_o_mesmo_resultado(escrever_parquet, tmp_path):
    arquivo = escrever_parquet([{}, {}, {"tip_amount": 9.0}, {"tip_amount": 9.0}, {"trip_distance": 3.0}])
    processar(arquivo, tmp_path)
    primeira = sorted(linhas_silver(tmp_path), key=str)
    processar(arquivo, tmp_path)
    assert sorted(linhas_silver(tmp_path), key=str) == primeira


def test_duracao_zero_fica_na_silver_e_marcada(escrever_parquet, tmp_path):
    processar(escrever_parquet([{"VendorID": 7, "tpep_dropoff_datetime": BASE}]), tmp_path)
    assert linhas_silver(tmp_path, colunas="duracao_valida") == [(False,)]


def test_canal_vem_de_request_source(escrever_parquet, tmp_path):
    arquivo = escrever_parquet(
        [
            {"request_source": None},
            {"request_source": "HV0003", "trip_distance": 3.0},
            {"request_source": "EH0004", "trip_distance": 4.0},
        ],
        extras={"request_source": None},
    )
    processar(arquivo, tmp_path)
    assert sorted(linhas_silver(tmp_path, colunas="canal")) == [("App de alto volume",), ("Outros apps",), ("Rua ou ponto",)]


def test_mes_antigo_sem_colunas_novas(escrever_parquet, tmp_path):
    metricas = processar(escrever_parquet([{}]), tmp_path)
    assert metricas["viagens"] == 1
    assert linhas_silver(tmp_path, colunas="cbd_congestion_fee, request_source") == [(None, None)]


def test_soma_conferida_inclui_taxa_de_congestionamento(escrever_parquet, tmp_path):
    arquivo = escrever_parquet(
        [{"total_amount": 20.5 + 0.75, "cbd_congestion_fee": 0.75}, {"total_amount": 99.0, "trip_distance": 3.0, "cbd_congestion_fee": 0.0}],
        extras={"cbd_congestion_fee": 0.0},
    )
    metricas = processar(arquivo, tmp_path)
    assert metricas["soma_diverge"] == 1


def test_reprocessar_substitui_a_particao(escrever_parquet, tmp_path):
    processar(escrever_parquet([{}, {"trip_distance": 3.0}], nome="a.parquet"), tmp_path)
    processar(escrever_parquet([{}], nome="b.parquet"), tmp_path)
    assert len(linhas_silver(tmp_path)) == 1
    assert [p.name for p in (tmp_path / "silver").iterdir()] == ["mes=2026-09"]
