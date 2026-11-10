"""Gráficos do relatório. Cada título afirma o achado, calculado a partir dos dados."""
from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#8b5cc7", "#d1477a", "#3fa7b5", "#7a8b3c"]
CINZA = "#b5b3ab"
TINTA = "#1f1e1b"
TINTA_2 = "#52514e"
GRADE = "#e1e0d9"
EIXO = "#c3c2b7"
SUPERFICIE = "#fcfcfb"

plt.rcParams.update(
    {
        "figure.facecolor": SUPERFICIE, "axes.facecolor": SUPERFICIE, "savefig.facecolor": SUPERFICIE,
        "axes.edgecolor": EIXO, "axes.labelcolor": TINTA_2, "xtick.color": TINTA_2, "ytick.color": TINTA_2,
        "text.color": TINTA, "axes.spines.top": False, "axes.spines.right": False, "axes.titlelocation": "left",
        "axes.titlesize": 13, "axes.titleweight": "bold", "axes.titlepad": 24, "font.size": 10,
        "axes.grid": True, "axes.grid.axis": "y", "grid.color": GRADE, "axes.axisbelow": True,
    }
)


def _rotulos_mes(ax, meses: list[str]) -> None:
    passo = max(1, len(meses) // 11)
    ax.set_xticks(range(0, len(meses), passo))
    ax.set_xticklabels([meses[i] for i in range(0, len(meses), passo)], rotation=45, ha="right")
    ax.grid(False, axis="x")


def _salvar(fig, destino: Path) -> str:
    destino.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(destino, dpi=150)
    plt.close(fig)
    return destino.name


def _legenda(ax, colunas: int) -> None:
    ax.legend(loc="lower left", bbox_to_anchor=(0, 1.0), ncols=colunas, frameon=False, borderaxespad=0.2)


def _canais(tabelas: dict, destino: Path) -> str:
    por_mes: dict[str, dict[str, int]] = defaultdict(dict)
    for linha in tabelas["viagens_por_canal"]:
        por_mes[linha["mes"]][linha["canal"]] = linha["viagens"]
    meses = sorted(por_mes)
    ordem = ["Rua ou ponto", "App de alto volume", "Outros apps"]  # "Rua ou ponto" inclui o canal desconhecido
    ultimo = por_mes[meses[-1]]
    fatia = 100 * (1 - ultimo.get("Rua ou ponto", 0) / sum(ultimo.values()))
    primeiro_app = next((m for m in meses if por_mes[m].get("Rua ou ponto", 0) < sum(por_mes[m].values())), None)
    titulo = (
        f"A fonte só identifica viagens de app a partir de {primeiro_app}; em {meses[-1]}, já eram {fatia:.0f}%"
        if primeiro_app else "A fonte ainda não identifica o canal das viagens"
    )
    fig, ax = plt.subplots(figsize=(10, 5.2))
    base = [0.0] * len(meses)
    for cor, canal in zip(SERIES, ordem):
        valores = [por_mes[m].get(canal, 0) / 1e6 for m in meses]
        ax.bar(range(len(meses)), valores, bottom=base, color=cor, label=canal, width=0.85)
        base = [a + b for a, b in zip(base, valores)]
    ax.set_title(titulo, pad=30)
    _legenda(ax, 3)
    ax.set_ylabel("milhões de viagens por mês")
    _rotulos_mes(ax, meses)
    ax.text(
        0, -0.3, "Antes disso, o arquivo não tem a coluna request_source: o canal é desconhecido, não necessariamente rua.",
        transform=ax.transAxes, color=TINTA_2,
    )
    return _salvar(fig, destino / "01_viagens_por_canal.png")


def _reconciliacao(tabelas: dict, destino: Path) -> str | None:
    linhas = [l for l in tabelas.get("reconciliacao_oficial", []) if l["diferenca_pct"] is not None]
    if not linhas:
        return None
    fora = [l["mes"] for l in linhas if l["estado"] == "divergente"]
    fig, ax = plt.subplots(figsize=(10, 4.6))
    meses = [l["mes"] for l in linhas]
    valores = [l["diferenca_pct"] for l in linhas]
    ax.bar(range(len(meses)), valores, color=SERIES[0], width=0.8)
    ax.axhspan(-3, 3, color=GRADE, alpha=0.5, zorder=0, label="Tolerância de 3%")
    ax.axhline(0, color=EIXO, linewidth=1)
    titulo = f"{len(linhas) - len(fora)} de {len(linhas)} meses ficam a até 3% da contagem oficial"
    if fora:
        titulo += f"\n{fora[0]} a {fora[-1]} passam do limite, sem causa conhecida" if len(fora) > 1 else f"\n{fora[0]} passa do limite"
    ax.set_title(titulo, pad=30)
    _legenda(ax, 1)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:+.0f}%"))
    ax.set_ylabel("viagens por dia, contra o oficial")
    _rotulos_mes(ax, meses)
    return _salvar(fig, destino / "02_conciliacao_oficial.png")


ROTULOS_MOTIVO = {
    "estorno": "estorno", "corrida_em_duplicidade": "corrida em duplicidade",
    "distancia_invalida": "distância inválida", "desembarque_antes_do_embarque": "desembarque antes do embarque",
    "fora_do_mes": "fora do mês", "duracao_acima_do_limite": "duração acima de 24 h",
    "valor_acima_do_limite": "valor acima de US\\$ 1.000", "zona_invalida": "zona inválida",
    "horario_ausente": "horário ausente", "valor_ausente": "valor ausente",
}


def _quarentena(tabelas: dict, destino: Path) -> str:
    por_mes: dict[str, dict[str, float]] = defaultdict(dict)
    totais: dict[str, float] = defaultdict(float)
    for linha in tabelas["quarentena_mes"]:
        por_mes[linha["mes"]][linha["motivo"]] = linha["pct_do_bruto"]
        totais[linha["motivo"]] += linha["linhas"]
    meses = sorted(por_mes)
    ordem = sorted(totais, key=totais.get, reverse=True)
    principais, demais = ordem[:4], ordem[4:]
    fig, ax = plt.subplots(figsize=(10, 5))
    base = [0.0] * len(meses)
    for cor, motivo in zip(SERIES, principais):
        valores = [por_mes[m].get(motivo, 0) for m in meses]
        ax.bar(range(len(meses)), valores, bottom=base, color=cor, label=ROTULOS_MOTIVO.get(motivo, motivo), width=0.85)
        base = [a + b for a, b in zip(base, valores)]
    if demais:
        valores = [sum(por_mes[m].get(x, 0) for x in demais) for m in meses]
        ax.bar(range(len(meses)), valores, bottom=base, color=CINZA, label="demais motivos", width=0.85)
        base = [a + b for a, b in zip(base, valores)]
    ax.set_title(f"A quarentena chega a {max(base):.1f}% de um mês, abaixo do limite crítico de 10%", pad=52)
    _legenda(ax, 3)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:.1f}%"))
    ax.set_ylabel("linhas do arquivo bruto")
    _rotulos_mes(ax, meses)
    return _salvar(fig, destino / "03_quarentena_por_motivo.png")


def _fornecedores(tabelas: dict, destino: Path) -> str:
    por_mes: dict[str, dict[int, int]] = defaultdict(dict)
    pct_helix = []
    for linha in tabelas["fornecedores_mes"]:
        por_mes[linha["mes"]][linha["fornecedor"]] = linha["viagens_duracao_invalida"]
        if linha["fornecedor"] == 7 and linha["viagens"] > 500:
            pct_helix.append(linha["pct_duracao_invalida"])
    meses = sorted(por_mes)
    fig, ax = plt.subplots(figsize=(10, 4.6))
    helix = [por_mes[m].get(7, 0) / 1e3 for m in meses]
    outros = [sum(v for k, v in por_mes[m].items() if k != 7) / 1e3 for m in meses]
    ax.bar(range(len(meses)), outros, color=CINZA, label="Demais fornecedores", width=0.85)
    ax.bar(range(len(meses)), helix, bottom=outros, color=SERIES[1], label="Helix (código 7)", width=0.85)
    minimo = min(pct_helix) if pct_helix else 0
    quanto = "todas as suas viagens" if minimo >= 99.95 else f"pelo menos {minimo:.0f}% das suas viagens"
    ax.set_title(f"A Helix grava duração zero em {quanto}, mês a mês", pad=30)
    _legenda(ax, 2)
    ax.set_ylabel("mil viagens com duração zero")
    _rotulos_mes(ax, meses)
    return _salvar(fig, destino / "04_duracao_zero_por_fornecedor.png")


def gerar_graficos(tabelas: dict, destino: Path) -> list[str]:
    feitos = [_canais(tabelas, destino), _reconciliacao(tabelas, destino), _quarentena(tabelas, destino), _fornecedores(tabelas, destino)]
    return [f for f in feitos if f]
