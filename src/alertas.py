"""Registra alertas em um arquivo e, se configurado, envia para um webhook."""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import requests

log = logging.getLogger(__name__)


def registrar(arquivo: Path, nivel: str, origem: str, mensagem: str, **contexto) -> dict:
    alerta = {
        "em": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "nivel": nivel, "origem": origem, "mensagem": mensagem, **contexto,
    }
    arquivo.parent.mkdir(parents=True, exist_ok=True)
    with arquivo.open("a", encoding="utf-8") as saida:
        saida.write(json.dumps(alerta, ensure_ascii=False, default=str) + "\n")
    webhook = os.getenv("ALERTA_WEBHOOK_URL")
    if webhook:
        try:
            requests.post(webhook, json={"text": f"[{nivel}] {origem}: {mensagem}"}, timeout=10)
        except requests.RequestException as erro:
            # Um webhook fora do ar não pode derrubar o callback nem esconder o alerta do arquivo.
            log.warning("webhook de alerta falhou: %s", erro)
    return alerta
