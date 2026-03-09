# -*- coding: utf-8 -*-
"""
Wrapper do executável para evitar divergência entre cópias do mesmo script.

Este arquivo delega a execução para `../AuditoriaPRESTADORV2.py`,
centralizando a lógica em um único lugar e reduzindo conflitos de merge.
"""

from pathlib import Path
import importlib.util


def _load_root_module():
    root_file = Path(__file__).resolve().parent.parent / "AuditoriaPRESTADORV2.py"
    spec = importlib.util.spec_from_file_location("auditoria_prestador_root", root_file)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Não foi possível carregar o módulo raiz: {root_file}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


if __name__ == "__main__":
    root_mod = _load_root_module()
    root_mod.run_ui()
