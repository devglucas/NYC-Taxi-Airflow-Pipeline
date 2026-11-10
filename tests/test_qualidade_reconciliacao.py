from src import qualidade, reconciliacao


def metricas(**kw):
    base = {
        "mes": "2026-09", "linhas_bruto": 100, "viagens": 98, "linhas_quarentena": 2,
        "fora_de_mes_na_silver": 0, "fornecedor_desconhecido": 0,
    }
    return base | kw


def nomes(lista):
    return {v["verificacao"] for v in lista}


def test_mes_sadio_nao_reprova():
    resultado = qualidade.avaliar(metricas(), [])
    assert not qualidade.criticas_reprovadas(resultado) and not qualidade.alertas_reprovados(resultado)


def test_linhas_perdidas_sao_criticas():
    resultado = qualidade.avaliar(metricas(viagens=90), [])
    assert "conservacao_de_linhas" in nomes(qualidade.criticas_reprovadas(resultado))


def test_quarentena_excessiva_e_critica():
    resultado = qualidade.avaliar(metricas(viagens=80, linhas_quarentena=20), [])
    assert "taxa_de_quarentena" in nomes(qualidade.criticas_reprovadas(resultado))


def test_volume_so_e_avaliado_com_historico_suficiente():
    curto = [{"mes": "2026-08", "viagens": 1000}]
    assert "volume_contra_historico" not in nomes(qualidade.avaliar(metricas(), curto))


def test_queda_de_volume_alerta_sem_barrar():
    historico = [{"mes": f"2026-0{m}", "viagens": 1000} for m in range(1, 6)]
    resultado = qualidade.avaliar(metricas(), historico)
    assert "volume_contra_historico" in nomes(qualidade.alertas_reprovados(resultado))
    assert not qualidade.criticas_reprovadas(resultado)


def test_historico_ignora_meses_futuros():
    historico = [{"mes": f"2027-0{m}", "viagens": 1000} for m in range(1, 6)]
    assert "volume_contra_historico" not in nomes(qualidade.avaliar(metricas(), historico))


def test_le_relatorio_oficial_com_milhar(tmp_path):
    arquivo = tmp_path / "r.csv"
    arquivo.write_text(
        'Month/Year,License Class,Trips Per Day\n2026-07,Green,"1,304"\n2026-07,Yellow,"113,811"\n', encoding="utf-8"
    )
    assert reconciliacao.ler_relatorio_oficial(arquivo) == {"2026-07": 113811}


def test_reconciliar_estados():
    itens = [{"mes": "2026-06", "viagens": 3000, "linhas_bruto": 3300}, {"mes": "2026-07", "viagens": 3100, "linhas_bruto": 3100}, {"mes": "2026-08", "viagens": 3100, "linhas_bruto": 3100}]
    resultado = {r["mes"]: r for r in reconciliacao.reconciliar(itens, {"2026-06": 100, "2026-07": 80})}
    assert resultado["2026-06"]["estado"] == "ok" and resultado["2026-06"]["diferenca_pct"] == 0
    assert resultado["2026-06"]["diferenca_sem_tratamento_pct"] == 10
    assert resultado["2026-07"]["estado"] == "divergente"
    assert resultado["2026-08"]["estado"] == "pendente"
