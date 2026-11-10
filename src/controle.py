"""Estado de cada mês em JSON: um arquivo por mês, gravado de forma atômica.

Fica fora do DuckDB para que tarefas paralelas consultem o estado sem disputar a trava
de escrita do banco.
"""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path


def gravar_json(caminho: Path, conteudo: dict) -> None:
    caminho.parent.mkdir(parents=True, exist_ok=True)
    descritor, temporario = tempfile.mkstemp(dir=caminho.parent, suffix=".tmp")
    with os.fdopen(descritor, "w", encoding="utf-8") as arquivo:
        json.dump(conteudo, arquivo, ensure_ascii=False, indent=2, default=str)
    os.replace(temporario, caminho)


def ler_json(caminho: Path) -> dict | None:
    return json.loads(caminho.read_text(encoding="utf-8")) if caminho.exists() else None


def arquivo_do_mes(controle: Path, mes: str) -> Path:
    return controle / f"arquivo_{mes}.json"


def metricas_do_mes(controle: Path, mes: str) -> Path:
    return controle / f"metricas_{mes}.json"


def todas_as_metricas(controle: Path) -> list[dict]:
    return [json.loads(p.read_text(encoding="utf-8")) for p in sorted(controle.glob("metricas_*.json"))]


def todos_os_arquivos(controle: Path) -> list[dict]:
    return [json.loads(p.read_text(encoding="utf-8")) for p in sorted(controle.glob("arquivo_*.json"))]
