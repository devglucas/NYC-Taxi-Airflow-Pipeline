"""Exporta tabelas analíticas e gráficos a partir do warehouse, apenas com agregados."""
from __future__ import annotations

import csv
import json
from pathlib import Path

import duckdb

from src.config import Caminhos
from src.visualize import gerar_graficos

CONSULTAS = {
    "resumo_mensal": """
        select mes, sum(viagens) as viagens,
            round(sum(viagens) / count(distinct dia), 0) as viagens_dia,
            round(sum(total_usd), 2) as total_usd,
            round(sum(total_usd) / sum(viagens), 2) as ticket_medio_usd,
            round(sum(minutos_validos) / sum(viagens_duracao_valida), 2) as minutos_medios,
            round(100.0 * sum(viagens_soma_diverge) / sum(viagens), 2) as pct_soma_diverge
        from viagens_dia group by mes order by mes""",
    "viagens_por_canal": """
        select mes, canal, sum(viagens) as viagens, round(sum(total_usd), 2) as total_usd
        from viagens_dia group by all order by mes, canal""",
    "pagamentos_mes": """
        select mes, pagamento, sum(viagens) as viagens from viagens_dia group by all order by mes, pagamento""",
    "fornecedores_mes": """
        select mes, fornecedor, viagens, viagens_duracao_invalida,
            round(100.0 * viagens_duracao_invalida / viagens, 2) as pct_duracao_invalida
        from viagens_fornecedor_mes order by mes, fornecedor""",
    "quarentena_mes": """
        select q.mes, q.motivo, q.linhas, round(100.0 * q.linhas / p.linhas_bruto, 3) as pct_do_bruto
        from quarentena_mes q join publicacoes p using (mes) order by q.mes, q.linhas desc""",
    "principais_origens": """
        select z.zona, z.distrito, sum(o.viagens) as viagens, round(sum(o.total_usd), 2) as total_usd
        from viagens_origem_mes o left join dim_zona z on z.zona_id = o.zona_origem
        group by all order by viagens desc limit 20""",
}


def _escrever_csv(caminho: Path, colunas: list[str], linhas: list[tuple]) -> None:
    caminho.parent.mkdir(parents=True, exist_ok=True)
    with caminho.open("w", encoding="utf-8", newline="") as arquivo:
        escritor = csv.writer(arquivo)
        escritor.writerow(colunas)
        escritor.writerows(linhas)


def exportar_tabelas(caminhos: Caminhos) -> dict[str, list[dict]]:
    tabelas: dict[str, list[dict]] = {}
    with duckdb.connect(str(caminhos.warehouse), read_only=True) as con:
        for nome, sql in CONSULTAS.items():
            cursor = con.execute(sql)
            colunas = [c[0] for c in cursor.description]
            linhas = cursor.fetchall()
            _escrever_csv(caminhos.output / "tabelas" / f"{nome}.csv", colunas, linhas)
            tabelas[nome] = [dict(zip(colunas, l)) for l in linhas]
    reconciliacao = caminhos.controle / "reconciliacao.json"
    if reconciliacao.exists():
        meses = json.loads(reconciliacao.read_text(encoding="utf-8"))["meses"]
        if meses:
            _escrever_csv(
                caminhos.output / "tabelas" / "reconciliacao_oficial.csv", list(meses[0]),
                [tuple(m.values()) for m in meses],
            )
        tabelas["reconciliacao_oficial"] = meses
    return tabelas


def gerar(caminhos: Caminhos) -> dict:
    tabelas = exportar_tabelas(caminhos)
    graficos = gerar_graficos(tabelas, caminhos.output / "graficos")
    return {"meses": len(tabelas["resumo_mensal"]), "graficos": graficos}
