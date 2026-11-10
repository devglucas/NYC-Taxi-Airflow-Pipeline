import pytest

from src import contrato


def test_arquivo_valido_passa(escrever_parquet):
    assert contrato.verificar(escrever_parquet([{}])).ok


def test_coluna_faltando_viola_o_contrato(escrever_parquet):
    arquivo = escrever_parquet([{}], remover={"total_amount"})
    with pytest.raises(contrato.ContratoViolado, match="total_amount"):
        contrato.exigir(arquivo)


def test_nome_com_outra_caixa_nao_e_quebra(escrever_parquet):
    arquivo = escrever_parquet([{"airport_fee": 0.0}], remover={"Airport_fee"})
    assert contrato.verificar(arquivo).ok


def test_coluna_nova_avisa_sem_reprovar(escrever_parquet):
    resultado = contrato.verificar(escrever_parquet([{}], extras={"taxa_futura": 1.0}))
    assert resultado.ok and resultado.novas == ["taxa_futura"]


def test_tipo_errado_reprova(escrever_parquet):
    arquivo = escrever_parquet([{"fare_amount": 1.0}], extras={"request_source": "HV0003"})
    assert contrato.verificar(arquivo).ok
    import duckdb

    with duckdb.connect() as con:
        con.execute(
            f"copy (select * replace (cast(fare_amount as varchar) as fare_amount) from '{arquivo}') "
            f"to '{arquivo}.2' (format parquet)"
        )
    resultado = contrato.verificar(type(arquivo)(f"{arquivo}.2"))
    assert not resultado.ok and "fare_amount" in resultado.tipos_errados


def test_arquivo_vazio_reprova(escrever_parquet):
    import duckdb

    arquivo = escrever_parquet([{}])
    with duckdb.connect() as con:
        con.execute(f"copy (select * from '{arquivo}' where false) to '{arquivo}.0' (format parquet)")
    assert not contrato.verificar(type(arquivo)(f"{arquivo}.0")).ok
