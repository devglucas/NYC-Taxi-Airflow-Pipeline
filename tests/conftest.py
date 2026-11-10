from datetime import datetime

import duckdb
import pytest

COLUNAS = {
    "VendorID": 2, "tpep_pickup_datetime": datetime(2026, 9, 10, 8, 0), "tpep_dropoff_datetime": datetime(2026, 9, 10, 8, 20),
    "passenger_count": 1, "trip_distance": 2.0, "RatecodeID": 1, "store_and_fwd_flag": "N",
    "PULocationID": 100, "DOLocationID": 200, "payment_type": 1, "fare_amount": 15.0, "extra": 1.0,
    "mta_tax": 0.5, "tip_amount": 3.0, "tolls_amount": 0.0, "improvement_surcharge": 1.0,
    "total_amount": 20.5, "congestion_surcharge": 0.0, "Airport_fee": 0.0,
}


@pytest.fixture
def escrever_parquet(tmp_path):
    """Grava um Parquet com linhas escritas à mão; os testes não usam dados reais da TLC."""

    def _escrever(linhas, nome="bruto.parquet", remover=(), extras=None):
        caminho = tmp_path / nome
        modelo = {**COLUNAS, **(extras or {})}
        registros = [{k: v for k, v in {**modelo, **linha}.items() if k not in remover} for linha in linhas]
        colunas = list(registros[0])
        with duckdb.connect() as con:
            tipos = {
                "tpep_pickup_datetime": "timestamp", "tpep_dropoff_datetime": "timestamp",
                "store_and_fwd_flag": "varchar", "request_source": "varchar",
                "VendorID": "integer", "PULocationID": "integer", "DOLocationID": "integer",
                "passenger_count": "bigint", "RatecodeID": "bigint", "payment_type": "bigint",
            }
            definicao = ", ".join(f'"{c}" {tipos.get(c, "double")}' for c in colunas)
            con.execute(f"create table t ({definicao})")
            marcadores = ", ".join("?" for _ in colunas)
            con.executemany(f"insert into t values ({marcadores})", [[r[c] for c in colunas] for r in registros])
            con.execute(f"copy t to '{caminho}' (format parquet)")
        return caminho

    return _escrever
