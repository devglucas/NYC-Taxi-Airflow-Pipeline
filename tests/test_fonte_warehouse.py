import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import duckdb
import pytest

from src import fonte, silver, warehouse


@pytest.fixture
def servidor():
    estado = {"status": 200, "corpo": b"", "declarado": None}

    class Manipulador(BaseHTTPRequestHandler):
        def _cabecalhos(self):
            self.send_response(estado["status"])
            self.send_header("ETag", '"abc"')
            self.send_header("Content-Length", str(estado["declarado"] or len(estado["corpo"])))
            self.end_headers()

        def do_HEAD(self):
            self._cabecalhos()

        def do_GET(self):
            self._cabecalhos()
            self.wfile.write(estado["corpo"])

        def log_message(self, *args):
            pass

    http = HTTPServer(("127.0.0.1", 0), Manipulador)
    threading.Thread(target=http.serve_forever, daemon=True).start()
    estado["url"] = f"http://127.0.0.1:{http.server_port}/a.parquet"
    yield estado
    http.shutdown()


@pytest.mark.parametrize("status", [403, 404])
def test_mes_nao_publicado_nao_e_erro(servidor, status):
    servidor["status"] = status
    assert not fonte.consultar(servidor["url"]).disponivel


def test_erro_do_servidor_e_instavel(servidor):
    servidor["status"] = 503
    with pytest.raises(fonte.FonteInstavel):
        fonte.consultar(servidor["url"])


def test_consulta_devolve_etag_sem_aspas(servidor):
    servidor["corpo"] = b"x"
    assert fonte.consultar(servidor["url"]).etag == "abc"


def test_download_valido(servidor, escrever_parquet, tmp_path):
    servidor["corpo"] = escrever_parquet([{}, {}]).read_bytes()
    destino = tmp_path / "saida" / "a.parquet"
    assert fonte.baixar(servidor["url"], destino)["linhas"] == 2
    assert destino.exists()


def test_arquivo_ilegivel_nao_chega_ao_destino(servidor, tmp_path):
    servidor["corpo"] = b"isto nao e parquet"
    destino = tmp_path / "a.parquet"
    destino.write_bytes(b"versao boa anterior")
    with pytest.raises(fonte.ArquivoCorrompido):
        fonte.baixar(servidor["url"], destino)
    assert destino.read_bytes() == b"versao boa anterior"
    assert list(tmp_path.glob("*.parcial")) == []


def test_publicar_duas_vezes_nao_duplica(escrever_parquet, tmp_path):
    banco = tmp_path / "w.duckdb"
    metricas = silver.processar(
        escrever_parquet([{}, {"trip_distance": 3.0}, {"total_amount": -1.0}]), "2026-09",
        tmp_path / "silver", tmp_path / "quarentena",
    )
    warehouse.publicar(banco, tmp_path / "silver", metricas, "h", "r1")
    warehouse.publicar(banco, tmp_path / "silver", metricas, "h", "r2")
    with duckdb.connect(str(banco)) as con:
        assert con.execute("select sum(viagens) from viagens_dia").fetchall()[0][0] == 2
        assert con.execute("select sum(linhas) from quarentena_mes").fetchall()[0][0] == 1
        assert con.execute("select id_execucao from publicacoes").fetchall() == [("r2",)]


def test_falha_na_publicacao_mantem_o_mes_anterior(escrever_parquet, tmp_path):
    banco = tmp_path / "w.duckdb"
    metricas = silver.processar(escrever_parquet([{}]), "2026-09", tmp_path / "silver", tmp_path / "quarentena")
    warehouse.publicar(banco, tmp_path / "silver", metricas, "h", "r1")
    with pytest.raises(ValueError):
        warehouse.publicar(banco, tmp_path / "silver", metricas | {"viagens": 999}, "h", "r2")
    with duckdb.connect(str(banco)) as con:
        assert con.execute("select sum(viagens) from viagens_dia").fetchall()[0][0] == 1
        assert con.execute("select id_execucao from publicacoes").fetchall() == [("r1",)]
