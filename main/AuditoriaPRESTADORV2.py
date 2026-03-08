# -*- coding: utf-8 -*-
"""
AUDITORIA PIS/COFINS - Prestadores de Serviço (lote)
Pontos integrados:
1) LIVRO PREFEITURA (CSV em pasta) -> consolida por IM/CNPJ
2) RELATÓRIO RPA PREFEITURA (CSV) -> status/alertas por IM
3) CRM p_crm_cdm (MySQL) -> consolida por CNPJ/competência (com QuestorClienteId)
4) API QUESTOR (Postgres via sua API) -> totais por codigoempresa/codigoestab/período
5) Relatório de entrada do Supervisor (Excel) -> lista de empresas (codigoempresa Questor + CNPJ)

Neste momento: apenas PRINTA resultados.
"""

import os
import re
import sys
import glob
from decimal import Decimal, InvalidOperation
from typing import Dict, Tuple, List, Any, Optional

import pandas as pd
import requests
import unicodedata


def load_env_file(env_path: str = ".env") -> None:
    """
    Carrega variáveis de ambiente a partir de um arquivo .env simples
    sem sobrescrever variáveis já definidas no ambiente do sistema.
    """
    if not os.path.exists(env_path):
        return

    with open(env_path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:].strip()
            if "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()

            if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
                value = value[1:-1]

            os.environ.setdefault(key, value)


def load_env_from_default_locations() -> None:
    """Tenta carregar .env do cwd, diretório do script e diretório pai."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(os.getcwd(), ".env"),
        os.path.join(base_dir, ".env"),
        os.path.join(os.path.dirname(base_dir), ".env"),
    ]
    loaded = set()
    for env_path in candidates:
        if env_path in loaded:
            continue
        loaded.add(env_path)
        load_env_file(env_path)


load_env_from_default_locations()

# =============================================================================
# VARIÁVEIS (depois você leva para Tkinter)
# =============================================================================

# 1) Pasta com diversos livros (CSV)
PASTA_LIVROS = r""

# 2) Relatório RPA Prefeitura (CSV)
RPA_RELATORIO_PATH = r""

# 3) Relatório Supervisor (Excel) - entrada do lote
SUPERVISOR_XLSX_PATH = r""

RELATORIO_XLSX_PATH = r""

# Competência do CRM
CRM_COMPETENCIA = ""

# Período para consultar na API do Questor
PERIODO_INICIAL = ""
PERIODO_FINAL = ""

# Se não existir CODIGOESTAB (ou Axio não preencher), usar este padrão
CODIGOESTAB_DEFAULT = 1

# --- NOVO: guia (datafim do debitofederal)
DATAFIM_GUIA = ""

# --- NOVO: alíquotas Lucro Presumido (PIS/COFINS não-cumulativo NÃO é aqui)
ALIQUOTA_PIS_LP = Decimal("0.0065")   # 0,65%
ALIQUOTA_COFINS_LP = Decimal("0.03")  # 3,00%

# API Questor (sua API)
QUESTOR_API_BASE_URL = os.getenv("QUESTOR_API_BASE_URL", "https://app.portalcdmcontabilidade.com.br")
QUESTOR_API_TOKEN = os.getenv("QUESTOR_API_TOKEN", "").strip()

# MySQL - AxioDataBase (IM <-> CNPJ/Questor codes)
MYSQL_AXIO = {
    "host": os.getenv("MYSQL_AXIO_HOST", os.getenv("MYSQL_HOST", "")),
    "port": int(os.getenv("MYSQL_AXIO_PORT", os.getenv("MYSQL_PORT", "3306"))),
    "user": os.getenv("MYSQL_AXIO_USER", os.getenv("MYSQL_USER", "")),
    "password": os.getenv("MYSQL_AXIO_PASSWORD", os.getenv("MYSQL_PASSWORD", "")),
    "database": os.getenv("MYSQL_AXIO_DATABASE", "AxioDataBase"),
    "charset": os.getenv("MYSQL_AXIO_CHARSET", "utf8mb4"),
}

# MySQL - CRM (p_crm_cdm)
MYSQL_CRM = {
    "host": os.getenv("MYSQL_CRM_HOST", os.getenv("MYSQL_HOST", "")),
    "port": int(os.getenv("MYSQL_CRM_PORT", os.getenv("MYSQL_PORT", "3306"))),
    "user": os.getenv("MYSQL_CRM_USER", os.getenv("MYSQL_USER", "")),
    "password": os.getenv("MYSQL_CRM_PASSWORD", os.getenv("MYSQL_PASSWORD", "")),
    "database": os.getenv("MYSQL_CRM_DATABASE", "p_crm_cdm"),
    "charset": os.getenv("MYSQL_CRM_CHARSET", "utf8mb4"),
}


# =============================================================================
# DB (MySQL) - conexão e execução
# =============================================================================
def get_mysql_connection(cfg: dict):
    """
    Tenta mysql-connector-python; se não existir, tenta pymysql.
    Retorna conexão e um "kind" indicando o driver.
    """
    required = ["host", "port", "user", "password", "database"]
    missing = [k for k in required if str(cfg.get(k, "")).strip() == ""]
    if missing:
        raise RuntimeError(
            f"Configuração MySQL incompleta para '{cfg.get('database', 'desconhecido')}'. "
            f"Campos ausentes: {', '.join(missing)}. "
            f"Verifique seu arquivo .env (MYSQL_*)."
        )

    import_errors = []
    conn_errors = []

    try:
        import mysql.connector  # type: ignore
    except Exception as e:
        import_errors.append(("mysql-connector-python", e))
    else:
        try:
            conn = mysql.connector.connect(
                host=cfg["host"],
                port=cfg["port"],
                user=cfg["user"],
                password=cfg["password"],
                database=cfg["database"],
                charset=cfg.get("charset", "utf8mb4"),
                autocommit=True,
            )
            return conn, "mysql-connector"
        except Exception as e:
            conn_errors.append(("mysql-connector-python", e))

    try:
        import pymysql  # type: ignore
    except Exception as e:
        import_errors.append(("pymysql", e))
    else:
        try:
            conn = pymysql.connect(
                host=cfg["host"],
                port=cfg["port"],
                user=cfg["user"],
                password=cfg["password"],
                database=cfg["database"],
                charset=cfg.get("charset", "utf8mb4"),
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=True,
            )
            return conn, "pymysql"
        except Exception as e:
            conn_errors.append(("pymysql", e))

    if conn_errors:
        driver, err = conn_errors[0]
        raise RuntimeError(
            f"Não foi possível conectar no MySQL ({cfg.get('database')}) usando {driver}. "
            f"Verifique host/porta/usuário/senha no .env. Detalhe: {err}"
        ) from err

    drivers = ", ".join(name for name, _ in import_errors) if import_errors else "mysql-connector-python ou pymysql"
    raise RuntimeError(
        f"Não foi possível conectar no MySQL ({cfg.get('database')}). "
        f"Instale um driver: {drivers}."
    )


def exec_mysql_query(cfg: dict, sql: str, params: Optional[list] = None) -> List[dict]:
    conn, kind = get_mysql_connection(cfg)
    try:
        if kind == "mysql-connector":
            cur = conn.cursor(dictionary=True)
            cur.execute(sql, params or [])
            rows = cur.fetchall()
            return rows
        else:
            with conn.cursor() as cur:
                cur.execute(sql, params or [])
                rows = cur.fetchall()
                return rows
    finally:
        try:
            conn.close()
        except Exception:
            pass


# =============================================================================
# Utils
# =============================================================================
def only_digits(s: str) -> str:
    return re.sub(r"\D+", "", s or "")


def br_money_to_decimal(v) -> Decimal:
    if v is None:
        return Decimal("0")
    if isinstance(v, Decimal):
        return v
    s = str(v).strip()
    if s == "" or s.lower() in ("nan", "none", "null"):
        return Decimal("0")
    s = s.replace("R$", "").strip()
    if "," in s:
        s = s.replace(".", "")
        s = s.replace(",", ".")
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return Decimal("0")


def normalize_col(name: str) -> str:
    s = (name or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = s.replace("°", "o").replace("º", "o")
    return s


def get_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    norm_map = {normalize_col(c): c for c in df.columns}
    for cand in candidates:
        key = normalize_col(cand)
        if key in norm_map:
            return norm_map[key]
    return None


# =============================================================================
# AXIO (IM <-> CNPJ)
# =============================================================================
SQL_AXIO_PESSOAS = """
SELECT
    inscricao_municipal,
    cnpj_cpf,
    razao_social,
    CODIGOEMPRESAQUESTOR,
    CODIGOESTAB,
    status,
    TIPO
FROM pessoas
WHERE inscricao_municipal IS NOT NULL
"""


def load_axio_mappings(cfg: dict) -> Tuple[Dict[str, dict], Dict[str, dict]]:
    """
    Retorna:
      - im_map:   {IM_digits: {...}}
      - doc_map:  {CNPJCPF_digits: {...}}
    """
    rows = exec_mysql_query(cfg, SQL_AXIO_PESSOAS)

    im_map: Dict[str, dict] = {}
    doc_map: Dict[str, dict] = {}

    for r in rows:
        im = only_digits((r.get("inscricao_municipal") or "").strip())
        doc = only_digits((r.get("cnpj_cpf") or "").strip())

        payload = {
            "inscricao_municipal": r.get("inscricao_municipal"),
            "cnpj_cpf": r.get("cnpj_cpf"),
            "razao_social": r.get("razao_social"),
            "CODIGOEMPRESAQUESTOR": r.get("CODIGOEMPRESAQUESTOR"),
            "CODIGOESTAB": r.get("CODIGOESTAB"),
            "status": r.get("status"),
            "TIPO": r.get("TIPO"),
        }

        if im:
            im_map[im] = payload
        if doc:
            doc_map[doc] = payload

    return im_map, doc_map


# =============================================================================
# LIVRO PREFEITURA (CSV)
# =============================================================================
def find_inscricao_municipal_in_header(file_path: str) -> str:
    try:
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            head = "".join([next(f, "") for _ in range(8)])
    except Exception:
        try:
            with open(file_path, "r", encoding="latin-1", errors="ignore") as f:
                head = "".join([next(f, "") for _ in range(8)])
        except Exception:
            return ""

    patterns = [
        r"Inscri[cç][aã]o\s+Municipal\s*:\s*([0-9\.\-\/]+)",
        r"Inscri[cç][aã]o\s+Mun\.\s*:\s*([0-9\.\-\/]+)",
        r"IM\s*:\s*([0-9\.\-\/]+)",
    ]

    for pat in patterns:
        m = re.search(pat, head, flags=re.IGNORECASE)
        if m:
            return only_digits(m.group(1))

    return ""


def read_prefeitura_csv(file_path: str) -> pd.DataFrame:
    for enc in ("utf-8", "latin-1"):
        try:
            df = pd.read_csv(
                file_path,
                sep=";",
                skiprows=2,
                dtype=str,
                encoding=enc,
                engine="python",
            )
            df = df.dropna(axis=1, how="all")
            return df
        except Exception:
            continue
    raise RuntimeError(f"Falha ao ler CSV: {file_path}")


def consolidate_prefeitura_df(df: pd.DataFrame) -> dict:
    # coluna natureza (pode vir com encoding zoado)
    col_nat = get_col(df, ["Natureza Operação", "Natureza Operacao", "Natureza da Operação", "Natureza da Operacao"])

    qtde_total = int(len(df))
    qtde_exigivel = qtde_total
    qtde_divergente = 0
    valores_divergentes = []

    if col_nat:
        # separa exigível vs divergente
        mask_exig = df[col_nat].apply(is_exigivel)
        df_div = df[~mask_exig].copy()
        df = df[mask_exig].copy()

        qtde_exigivel = int(len(df))
        qtde_divergente = int(len(df_div))

        # lista dos status divergentes encontrados
        if qtde_divergente > 0:
            valores_divergentes = sorted(
                {normalize_text(x) for x in df_div[col_nat].dropna().astype(str).tolist()}
            )

    # ---- daqui pra baixo mantém sua lógica de somatórios ----
    col_valor_doc = get_col(df, ["Valor Documento", "Valor do Documento", "Valor"])
    col_valor_trib = get_col(df, ["Valor Tributável", "Valor Tributavel"])
    col_imposto_retido = get_col(df, ["Imposto Retido"])
    col_pis = get_col(df, ["PIS"])
    col_cofins = get_col(df, ["COFINS"])
    col_csll = get_col(df, ["CSLL"])
    col_irrf = get_col(df, ["IRRF"])
    col_inss = get_col(df, ["INSS"])
    col_outros = get_col(df, ["OUTRAS RETENÇÕES", "OUTRAS RETENCOES", "Outras Retenções", "Outras Retencoes"])

    def sum_col(colname: Optional[str]) -> Decimal:
        if not colname:
            return Decimal("0")
        return sum((br_money_to_decimal(x) for x in df[colname].tolist()), Decimal("0"))

    return {
        "qtde_linhas_total": qtde_total,
        "qtde_linhas_exigivel": qtde_exigivel,
        "qtde_linhas_divergente": qtde_divergente,
        "naturezas_divergentes": valores_divergentes,

        "total_valor_documento": sum_col(col_valor_doc),
        "total_valor_tributavel": sum_col(col_valor_trib),
        "total_imposto_retido": sum_col(col_imposto_retido),
        "total_pis": sum_col(col_pis),
        "total_cofins": sum_col(col_cofins),
        "total_csll": sum_col(col_csll),
        "total_irrf": sum_col(col_irrf),
        "total_inss": sum_col(col_inss),
        "total_outras_retencoes": sum_col(col_outros),
    }

def list_csv_files(base_dir: str) -> List[str]:
    patterns = [
        os.path.join(base_dir, "**", "*.csv"),
        os.path.join(base_dir, "**", "*.CSV"),
    ]
    files = []
    for pat in patterns:
        files.extend(glob.glob(pat, recursive=True))
    seen = set()
    out = []
    for f in files:
        if f not in seen:
            seen.add(f)
            out.append(f)
    return out


def process_livros_prefeitura(pasta: str, im_map: Dict[str, dict]) -> Dict[str, dict]:
    files = list_csv_files(pasta)
    livros_by_im: Dict[str, dict] = {}

    for fp in files:
        im = find_inscricao_municipal_in_header(fp)
        if not im:
            continue

        try:
            df = read_prefeitura_csv(fp)
            cons = consolidate_prefeitura_df(df)
        except Exception:
            continue

        axio = im_map.get(im, {})
        cnpj = axio.get("cnpj_cpf")

        if im not in livros_by_im:
            livros_by_im[im] = {
                "IM": im,
                "CNPJ": cnpj,
                "Razao": axio.get("razao_social"),

                "TotalValorDocumento": Decimal("0"),
                "TotalValorTributavel": Decimal("0"),
                "TotalImpostoRetido": Decimal("0"),
                "TotalPIS": Decimal("0"),
                "TotalCOFINS": Decimal("0"),
                "TotalCSLL": Decimal("0"),
                "TotalIRRF": Decimal("0"),
                "TotalINSS": Decimal("0"),
                "TotalOutrasRetencoes": Decimal("0"),

                # >>> NOVO (inicializa para não dar KeyError)
                "QtdeLinhasTotal": 0,
                "QtdeLinhasExigivel": 0,
                "QtdeLinhasDivergente": 0,
                "NaturezasDivergentes": set(),

                "Arquivos": [],
            }

        # somatórios (somente Exigível já vem filtrado no consolidate_prefeitura_df)
        livros_by_im[im]["TotalValorDocumento"] += cons["total_valor_documento"]
        livros_by_im[im]["TotalValorTributavel"] += cons["total_valor_tributavel"]
        livros_by_im[im]["TotalImpostoRetido"] += cons["total_imposto_retido"]
        livros_by_im[im]["TotalPIS"] += cons["total_pis"]
        livros_by_im[im]["TotalCOFINS"] += cons["total_cofins"]
        livros_by_im[im]["TotalCSLL"] += cons["total_csll"]
        livros_by_im[im]["TotalIRRF"] += cons["total_irrf"]
        livros_by_im[im]["TotalINSS"] += cons["total_inss"]
        livros_by_im[im]["TotalOutrasRetencoes"] += cons["total_outras_retencoes"]
        livros_by_im[im]["Arquivos"].append(fp)

        # contadores de divergência
        livros_by_im[im]["QtdeLinhasTotal"] += cons["qtde_linhas_total"]
        livros_by_im[im]["QtdeLinhasExigivel"] += cons["qtde_linhas_exigivel"]
        livros_by_im[im]["QtdeLinhasDivergente"] += cons["qtde_linhas_divergente"]
        for nd in cons["naturezas_divergentes"]:
            livros_by_im[im]["NaturezasDivergentes"].add(nd)

    # >>> FINAL: converte set para lista ordenada (fora do loop de arquivos)
    for im in livros_by_im:
        livros_by_im[im]["NaturezasDivergentes"] = sorted(list(livros_by_im[im]["NaturezasDivergentes"]))

    return livros_by_im


# =============================================================================
# RPA (CSV) - status por IM + alertas
# =============================================================================
RPA_OK_PAIRS = [
    ("Serviços Prestados - Livro: Com Movimento", "Serviços Prestados - XML: BAIXADO"),
    ("Serviços Prestados - Livro:Sem Movimento", "Serviços Prestados - Sem Movimento - XML"),
]

RPA_ALERT_CONTAINS = [
    ("Contribuinte não credenciado", "ALERTA_DTE_NAO_CREDENCIADO"),
    ("Não Encontrado/Encerrado", "ALERTA_CONTRIBUINTE_NAO_ENCONTRADO_OU_ENCERRADO"),
]


def read_rpa_relatorio_csv(file_path: str) -> pd.DataFrame:
    for enc in ("utf-8", "latin-1"):
        try:
            df = pd.read_csv(
                file_path,
                sep=";",
                dtype=str,
                encoding=enc,
                engine="python",
            )
            df = df.dropna(axis=1, how="all")
            return df
        except Exception:
            continue
    raise RuntimeError(f"Falha ao ler CSV RPA: {file_path}")


def sanitize_rpa_im(v: str) -> str:
    s = (v or "").strip()
    s = s.replace('="', "").replace('"', "")
    return only_digits(s)


def rpa_status_by_im(df: pd.DataFrame) -> Dict[str, dict]:
    col_im = get_col(df, ["CNPJ", "IM", "Inscrição Municipal", "Inscricao Municipal"])
    col_livro = get_col(df, ["Livro"])
    col_ini = get_col(df, ["Data Início", "Data Inicio"])
    col_fim = get_col(df, ["Data Fim"])

    if not col_im or not col_livro:
        raise RuntimeError(f"Colunas esperadas não encontradas no RPA. Colunas: {list(df.columns)}")

    per_im: Dict[str, dict] = {}

    for _, row in df.iterrows():
        im = sanitize_rpa_im(row.get(col_im, ""))
        livro = (row.get(col_livro, "") or "").strip()
        dt_ini = (row.get(col_ini, "") or "").strip() if col_ini else ""
        dt_fim = (row.get(col_fim, "") or "").strip() if col_fim else ""

        if not im:
            continue

        if im not in per_im:
            per_im[im] = {
                "IM": im,
                "DataInicio": dt_ini,
                "DataFim": dt_fim,
                "Lancamentos": set(),
                "Alertas": set(),
                "ParOKEncontrado": "",
                "Status": "ALERTA",
            }

        per_im[im]["Lancamentos"].add(livro)

        for needle, code in RPA_ALERT_CONTAINS:
            if needle.lower() in livro.lower():
                per_im[im]["Alertas"].add(code)

    # aplica regra dos pares
    for im, info in per_im.items():
        livros = info["Lancamentos"]

        ok_pair = ""
        for a, b in RPA_OK_PAIRS:
            if a in livros and b in livros:
                ok_pair = f"{a} + {b}"
                break

        if not ok_pair:
            info["Alertas"].add("ALERTA_FALTA_LANCAMENTO_CORRESPONDENTE")

        info["ParOKEncontrado"] = ok_pair
        info["Status"] = "OK" if (ok_pair and len(info["Alertas"]) == 0) else "ALERTA"

    # normaliza sets
    out: Dict[str, dict] = {}
    for im, info in per_im.items():
        out[im] = {
            "IM": info["IM"],
            "DataInicio": info["DataInicio"],
            "DataFim": info["DataFim"],
            "Status": info["Status"],
            "ParOKEncontrado": info["ParOKEncontrado"],
            "Alertas": sorted(list(info["Alertas"])),
            "Lancamentos": sorted(list(info["Lancamentos"])),
        }
    return out

def is_rpa_sem_movimento(rpa_row: Optional[dict]) -> bool:
    if not rpa_row:
        return False
    status = normalize_text(str(rpa_row.get("Status", ""))).lower()
    par_ok = normalize_text(str(rpa_row.get("ParOKEncontrado", ""))).lower()
    return (status == "ok") and ("sem movimento" in par_ok)


# =============================================================================
# CRM p_crm_cdm (MySQL) - consulta consolidada por CNPJ/competência
# =============================================================================
SQL_CRM_CONSOLIDADO_POR_CNPJ = """
SELECT
    qc.QuestorClienteId,
    base.ClienteCod,
    base.CNPJ,
    base.Competencia,
    base.QtdeNotas,
    base.TotalValorTotal,
    base.TotalValorLiquido,

    SUM(CASE WHEN pi.NfseAuditImpostoId = 1 THEN COALESCE(pi.NfseAuditPrestadoImpostoValor, 0) ELSE 0 END) AS TotalISS,
    SUM(CASE WHEN pi.NfseAuditImpostoId = 2 THEN COALESCE(pi.NfseAuditPrestadoImpostoValor, 0) ELSE 0 END) AS TotalPIS,
    SUM(CASE WHEN pi.NfseAuditImpostoId = 3 THEN COALESCE(pi.NfseAuditPrestadoImpostoValor, 0) ELSE 0 END) AS TotalCOFINS,
    SUM(CASE WHEN pi.NfseAuditImpostoId = 4 THEN COALESCE(pi.NfseAuditPrestadoImpostoValor, 0) ELSE 0 END) AS TotalCSLL,
    SUM(CASE WHEN pi.NfseAuditImpostoId = 5 THEN COALESCE(pi.NfseAuditPrestadoImpostoValor, 0) ELSE 0 END) AS TotalIRRF,
    SUM(CASE WHEN pi.NfseAuditImpostoId = 6 THEN COALESCE(pi.NfseAuditPrestadoImpostoValor, 0) ELSE 0 END) AS TotalINSS

FROM (
    SELECT
        n.ClienteCod,
        p.PessoaCNPJ AS CNPJ,
        n.NfseAuditPrestadoCompetencia AS Competencia,
        COUNT(*) AS QtdeNotas,
        SUM(COALESCE(n.NfseAuditPrestadoValorTotal, 0))   AS TotalValorTotal,
        SUM(COALESCE(n.NfseAuditPrestadoValorLiquido, 0)) AS TotalValorLiquido
    FROM nfseauditprestado n
    JOIN pessoa p
      ON p.PessoaCod = n.ClienteCod
    WHERE p.PessoaCNPJ IS NOT NULL
      AND n.NfseAuditPrestadoCompetencia = %s
      AND p.PessoaCNPJ = %s
    GROUP BY n.ClienteCod, p.PessoaCNPJ, n.NfseAuditPrestadoCompetencia
) base

LEFT JOIN questorcliente qc
  ON qc.ClienteCod = base.ClienteCod

JOIN nfseauditprestado n
  ON n.ClienteCod = base.ClienteCod
 AND n.NfseAuditPrestadoCompetencia = base.Competencia

LEFT JOIN nfseauditprestadoimposto pi
  ON pi.NfseAuditPrestadoId = n.NfseAuditPrestadoId

GROUP BY
    qc.QuestorClienteId,
    base.ClienteCod,
    base.CNPJ,
    base.Competencia,
    base.QtdeNotas,
    base.TotalValorTotal,
    base.TotalValorLiquido

ORDER BY
    base.CNPJ;
"""


def crm_fetch_consolidado_por_cnpj(competencia: str, cnpj_masked: str) -> Optional[dict]:
    rows = exec_mysql_query(MYSQL_CRM, SQL_CRM_CONSOLIDADO_POR_CNPJ, [competencia, cnpj_masked])
    if not rows:
        return None
    return rows[0]


# =============================================================================
# QUESTOR API - consulta consolidada por codigoempresa/codigoestab/período
# =============================================================================
def questor_api_fetch(codigoempresa: int, codigoestab: int, periodo_ini: str, periodo_fim: str, datafim: str) -> Optional[dict]:
    if not QUESTOR_API_TOKEN:
        return {"_erro_http": True, "status_code": 401, "body": "QUESTOR_API_TOKEN não configurado no ambiente."}

    url = f"{QUESTOR_API_BASE_URL}/consulta/nfse-retido/consolidado-com-guia"
    headers = {"Authorization": f"Bearer {QUESTOR_API_TOKEN}"}
    params = {
        "codigoempresa": codigoempresa,
        "codigoestab": codigoestab,
        "periodo_inicial": periodo_ini,
        "periodo_final": periodo_fim,
        "datafim": datafim,
    }
    r = requests.get(url, headers=headers, params=params, timeout=60)
    if r.status_code != 200:
        return {"_erro_http": True, "status_code": r.status_code, "body": r.text}

    try:
        data = r.json()
    except Exception:
        return {"_erro_http": True, "status_code": r.status_code, "body": r.text}

    items = data.get("items") or []
    if not items:
        return None
    return items[0]


# =============================================================================
# Supervisor XLSX - lista de entrada
# =============================================================================
def read_supervisor_xlsx(path: str) -> pd.DataFrame:
    df = pd.read_excel(path, dtype=str)
    df = df.dropna(axis=1, how="all")

    col_cod = get_col(df, ["codigoempresa Questor", "codigoempresa", "CodigoEmpresa", "CODIGOEMPRESAQUESTOR"])
    col_cnpj = get_col(df, ["CNPJ", "cnpj"])
    col_razao = get_col(df, ["RAZAO", "Razao", "Razão Social", "razao_social"])
    col_regime = get_col(df, ["Regime", "Regime Tributário", "Regime Tributario", "RegimeTrib", "RegimeTributario"])
    

    if not col_cod or not col_cnpj:
        raise RuntimeError(f"Colunas esperadas não encontradas no XLSX Supervisor. Colunas: {list(df.columns)}")

    out = pd.DataFrame({
        "codigoempresa_questor": df[col_cod].astype(str).str.strip(),
        "cnpj": df[col_cnpj].astype(str).str.strip(),
        "razao": df[col_razao].astype(str).str.strip() if col_razao else "",
        "regime": df[col_regime].astype(str).str.strip() if col_regime else "",
    })

    out["codigoempresa_questor"] = out["codigoempresa_questor"].apply(lambda x: only_digits(x))
    out["cnpj_digits"] = out["cnpj"].apply(lambda x: only_digits(x))
    out["regime"] = out["regime"].apply(normalize_text)
    out = out[out["codigoempresa_questor"] != ""].copy()
    return out


def is_lucro_presumido(regime_texto: str) -> bool:
    t = normalize_text(regime_texto).lower()

    # remove tudo que não for letra/número/espaço (evita lixo)
    t = re.sub(r"[^a-z0-9 ]+", "", t)

    return "presumido" in t

# =========================
# NOVO: comparação final + ALERTA de divergência (Livro x CRM x Questor)
# (adicione este bloco no seu script)
# =========================

from decimal import Decimal

TOLERANCIA_VALOR = Decimal("0.01")  # depois você parametriza no Tkinter


def to_dec(v) -> Decimal:
    """
    Converte valores vindos de:
    - Decimal
    - float/int
    - str ('60496.25', '60.496,25', '0.0')
    - None
    """
    if v is None:
        return Decimal("0")
    if isinstance(v, Decimal):
        return v
    s = str(v).strip()
    if s == "" or s.lower() in ("nan", "none", "null"):
        return Decimal("0")
    # pt-BR?
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    try:
        return Decimal(s)
    except Exception:
        return Decimal("0")


def diff_ok(a: Decimal, b: Decimal, tol: Decimal) -> bool:
    return (a - b).copy_abs() <= tol


def build_alerta_final(livro_row: Optional[dict], crm_row: Optional[dict], questor_row: Optional[dict]) -> dict:
    """
    Retorna:
      StatusFinal: OK / DIVERGENTE / SEM_DADOS
      Divergencias: lista de strings
      Comparacoes: dict com valores lado a lado
    """
    divergencias = []
    comps = {}

    # Se não tem nenhum dado, não compara
    if not livro_row and not crm_row and not questor_row:
        return {"StatusFinal": "SEM_DADOS", "Divergencias": ["SEM_FONTES"], "Comparacoes": {}}

    # --- Totais principais ---
    livro_total = to_dec(livro_row.get("TotalValorDocumento")) if livro_row else None
    crm_total = to_dec(crm_row.get("TotalValorTotal")) if crm_row else None
    questor_total = to_dec(questor_row.get("total_valorcontabil")) if (questor_row and isinstance(questor_row, dict)) else None

    comps["TOTAL_VALOR"] = {"Livro": str(livro_total) if livro_total is not None else None,
                            "CRM": str(crm_total) if crm_total is not None else None,
                            "Questor": str(questor_total) if questor_total is not None else None}

    # compara Livro x CRM
    if livro_total is not None and crm_total is not None and not diff_ok(livro_total, crm_total, TOLERANCIA_VALOR):
        divergencias.append("DIVERG_TOTAL_LIVRO_X_CRM")

    # compara Livro x Questor
    if livro_total is not None and questor_total is not None and not diff_ok(livro_total, questor_total, TOLERANCIA_VALOR):
        divergencias.append("DIVERG_TOTAL_LIVRO_X_QUESTOR")

    # compara CRM x Questor
    if crm_total is not None and questor_total is not None and not diff_ok(crm_total, questor_total, TOLERANCIA_VALOR):
        divergencias.append("DIVERG_TOTAL_CRM_X_QUESTOR")

    # --- Impostos: PIS/COFINS/CSLL/IRRF/INSS ---
    # Livro pode não ter esses campos preenchidos conforme layout; aqui já usamos os consolidados do livro.
    pairs = [
        ("PIS", "TotalPIS", "total_valor_pis"),
        ("COFINS", "TotalCOFINS", "total_valor_cofins"),
        ("CSLL", "TotalCSLL", "total_valor_csll"),
        ("IRRF", "TotalIRRF", "total_valor_irrf"),
        ("INSS", "TotalINSS", "total_valor_inss"),
    ]

    for label, livro_key, questor_key in pairs:
        lv = to_dec(livro_row.get(livro_key)) if livro_row else None
        cv = to_dec(crm_row.get(f"Total{label}")) if crm_row else None
        qv = to_dec(questor_row.get(questor_key)) if (questor_row and isinstance(questor_row, dict)) else None

        comps[label] = {"Livro": str(lv) if lv is not None else None,
                        "CRM": str(cv) if cv is not None else None,
                        "Questor": str(qv) if qv is not None else None}

        if lv is not None and cv is not None and not diff_ok(lv, cv, TOLERANCIA_VALOR):
            divergencias.append(f"DIVERG_{label}_LIVRO_X_CRM")

        if lv is not None and qv is not None and not diff_ok(lv, qv, TOLERANCIA_VALOR):
            divergencias.append(f"DIVERG_{label}_LIVRO_X_QUESTOR")

        if cv is not None and qv is not None and not diff_ok(cv, qv, TOLERANCIA_VALOR):
            divergencias.append(f"DIVERG_{label}_CRM_X_QUESTOR")

    status = "OK" if len(divergencias) == 0 else "DIVERGENTE"
    return {"StatusFinal": status, "Divergencias": divergencias, "Comparacoes": comps}

def calc_pis_cofins_lp(base_receita: Decimal, pis_retido: Decimal, cofins_retido: Decimal) -> dict:
    pis_bruto = (base_receita * ALIQUOTA_PIS_LP).quantize(Decimal("0.01"))
    cofins_bruto = (base_receita * ALIQUOTA_COFINS_LP).quantize(Decimal("0.01"))

    pis_retido = pis_retido.quantize(Decimal("0.01"))
    cofins_retido = cofins_retido.quantize(Decimal("0.01"))

    pis_liq = pis_bruto - pis_retido
    cofins_liq = cofins_bruto - cofins_retido

    if pis_liq < 0:
        pis_liq = Decimal("0.00")
    if cofins_liq < 0:
        cofins_liq = Decimal("0.00")

    return {
        "pis_bruto": pis_bruto,
        "cofins_bruto": cofins_bruto,
        "pis_retido": pis_retido,
        "cofins_retido": cofins_retido,
        "pis_liquido": pis_liq.quantize(Decimal("0.01")),
        "cofins_liquido": cofins_liq.quantize(Decimal("0.01")),
    }

def is_gt_zero(v: Decimal, tol: Decimal) -> bool:
    return v > tol

def normalize_text(s: str) -> str:
    s = "" if s is None else str(s)

    # normaliza unicode (transforma variações estranhas)
    s = unicodedata.normalize("NFKC", s)

    # remove caracteres invisíveis comuns
    s = s.replace("\u00A0", " ")  # NBSP
    s = s.replace("\u200b", "")   # zero-width space
    s = s.replace("\ufeff", "")   # BOM

    # normaliza espaços
    s = re.sub(r"[ \t\r\n]+", " ", s).strip()
    return s

def is_exigivel(natureza: str) -> bool:
    t = normalize_text(natureza).lower()
    return t == "exigível" or t == "exigivel"

def apuracao_status_text(apuracao_fechada: int) -> str:
    return "FECHADA" if int(apuracao_fechada) == 1 else "ABERTA"

def moeda(v: Decimal) -> str:
    # só para print didático (sem formatação BR sofisticada)
    return f"{v:.2f}"

def exportar_relatorio_excel(report_rows: list[dict], output_path: str):
    if not report_rows:
        print("[Excel] Nenhuma linha para exportar.")
        return

    df = pd.DataFrame(report_rows)

    # ordem didática das colunas
    colunas = [
        "codigoempresa_questor", "codigoestab", "cnpj", "razao", "im", "regime",
        "rpa_status", "rpa_par_ok", "rpa_alertas",
        "livro_total_valor_documento", "livro_pis", "livro_cofins", "livro_csll", "livro_irrf", "livro_inss",
        "crm_questorclienteid", "crm_clientecod", "crm_competencia", "crm_qtde_notas",
        "crm_total_valor", "crm_total_liquido", "crm_pis", "crm_cofins", "crm_csll", "crm_irrf", "crm_inss",
        "questor_inscrfederal", "questor_total_valorcontabil",
        "questor_pis_retido", "questor_cofins_retido", "questor_csll_retido", "questor_irrf_retido", "questor_inss_retido",
        "apuracao_piscofins_status", "questor_guia_pis", "questor_guia_cofins",
        "lp_base", "lp_pis_bruto", "lp_cofins_bruto", "lp_pis_retido", "lp_cofins_retido",
        "lp_pis_liquido", "lp_cofins_liquido",
        "alerta_guia", "status_final", "divergencias_finais"
    ]

    # garante que todas existam
    for c in colunas:
        if c not in df.columns:
            df[c] = ""

    df = df[colunas]

    # aba só com divergências/alertas
    mask_sem_mov = df["status_final"].astype(str) == "SEM MOVIMENTO"
    df_div = df[
        (~mask_sem_mov) & (
            (df["status_final"].astype(str) != "OK") |
            (df["alerta_guia"].astype(str) != "OK — apuração fechada e valores da guia conferem.") |
            (df["rpa_status"].astype(str) != "OK")
        )
    ].copy()

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Resumo")
        df_div.to_excel(writer, index=False, sheet_name="Divergencias")

        wb = writer.book
        for ws_name in ["Resumo", "Divergencias"]:
            ws = writer.sheets[ws_name]

            # filtro e congelar cabeçalho
            ws.auto_filter.ref = ws.dimensions
            ws.freeze_panes = "A2"

            # largura automática simples
            for col in ws.columns:
                max_len = 0
                col_letter = col[0].column_letter
                for cell in col:
                    val = "" if cell.value is None else str(cell.value)
                    if len(val) > max_len:
                        max_len = len(val)
                ws.column_dimensions[col_letter].width = min(max(max_len + 2, 12), 45)

    print(f"[Excel] Relatório gerado em: {output_path}")


# -*- coding: utf-8 -*-
"""
UI (Tkinter) - Auditoria Prestador
- Mostra somente:
  1) Pasta Livros Prefeitura
  2) CSV RPA Prefeitura
  3) XLSX Supervisor (entrada)
  4) Período de apuração (Inicial e Final)
  5) Saída Excel

Regras:
- O período informado alimenta:
  - PERIODO_INICIAL, PERIODO_FINAL
  - DATAFIM_GUIA = PERIODO_FINAL
  - CRM_COMPETENCIA = MM/YYYY (derivado de PERIODO_FINAL)
- Não exibe: codigoestabdefault, api, etc. (ficam como defaults internos)
- Ao executar: abre uma janela "Executando..." até terminar
"""

import os
import re
import threading
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from datetime import datetime
import calendar


def _competencia_mm_yyyy_from_date(date_str_yyyy_mm_dd: str) -> str:
    """
    Recebe 'YYYY-MM-DD' e retorna 'MM/YYYY'
    """
    dt = datetime.strptime(date_str_yyyy_mm_dd.strip(), "%Y-%m-%d")
    return f"{dt.month:02d}/{dt.year}"


def _validate_date_yyyy_mm_dd(date_str: str) -> bool:
    try:
        datetime.strptime(date_str.strip(), "%Y-%m-%d")
        return True
    except Exception:
        return False


def _open_date_picker(parent, target_var: tk.StringVar):
    """Abre um calendário simples para selecionar data no formato YYYY-MM-DD."""
    today = datetime.today()
    raw = (target_var.get() or "").strip()
    try:
        current = datetime.strptime(raw, "%Y-%m-%d") if raw else today
    except Exception:
        current = today

    state = {"year": current.year, "month": current.month}

    win = tk.Toplevel(parent)
    win.title("Selecionar data")
    win.transient(parent)
    win.grab_set()
    win.resizable(False, False)

    frm = ttk.Frame(win, padding=10)
    frm.pack(fill="both", expand=True)

    header = ttk.Frame(frm)
    header.pack(fill="x", pady=(0, 8))

    lbl_month = ttk.Label(header, text="", font=("Segoe UI", 10, "bold"))
    lbl_month.pack(side="left", padx=8)

    grid = ttk.Frame(frm)
    grid.pack()

    weekdays = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"]
    for i, wd in enumerate(weekdays):
        ttk.Label(grid, text=wd, width=4, anchor="center").grid(row=0, column=i, padx=1, pady=1)

    def select_day(day: int):
        date_text = f"{state['year']:04d}-{state['month']:02d}-{day:02d}"
        target_var.set(date_text)
        win.destroy()

    def render_calendar():
        for w in grid.grid_slaves():
            info = w.grid_info()
            if int(info.get("row", 0)) >= 1:
                w.destroy()

        lbl_month.config(text=f"{calendar.month_name[state['month']]} {state['year']}")
        cal = calendar.Calendar(firstweekday=0)
        weeks = cal.monthdayscalendar(state["year"], state["month"])

        for r, week in enumerate(weeks, start=1):
            for c, day in enumerate(week):
                if day == 0:
                    ttk.Label(grid, text="", width=4).grid(row=r, column=c, padx=1, pady=1)
                else:
                    b = ttk.Button(grid, text=f"{day}", width=3, command=lambda d=day: select_day(d))
                    b.grid(row=r, column=c, padx=1, pady=1)

    def prev_month():
        if state["month"] == 1:
            state["month"] = 12
            state["year"] -= 1
        else:
            state["month"] -= 1
        render_calendar()

    def next_month():
        if state["month"] == 12:
            state["month"] = 1
            state["year"] += 1
        else:
            state["month"] += 1
        render_calendar()

    ttk.Button(header, text="◀", width=3, command=prev_month).pack(side="left")
    ttk.Button(header, text="▶", width=3, command=next_month).pack(side="right")

    footer = ttk.Frame(frm)
    footer.pack(fill="x", pady=(8, 0))

    def set_today():
        target_var.set(datetime.today().strftime("%Y-%m-%d"))
        win.destroy()

    ttk.Button(footer, text="Hoje", command=set_today).pack(side="left")
    ttk.Button(footer, text="Fechar", command=win.destroy).pack(side="right")

    render_calendar()


def run_ui():
    root = tk.Tk()
    root.title("Auditoria Prestador — Configurações")
    root.geometry("920x600")
    root.minsize(880, 520)

    # -----------------------------
    # Style (mais bonito / moderno)
    # -----------------------------
    style = ttk.Style()
    try:
        style.theme_use("clam")
    except Exception:
        pass

    style.configure("TFrame", background="#f6f7fb")
    style.configure("Card.TFrame", background="#ffffff", relief="flat")
    style.configure("Title.TLabel", background="#f6f7fb", font=("Segoe UI", 16, "bold"))
    style.configure("Sub.TLabel", background="#f6f7fb", font=("Segoe UI", 10))
    style.configure("Label.TLabel", background="#ffffff", font=("Segoe UI", 10))
    style.configure("TEntry", font=("Segoe UI", 10))
    style.configure("TButton", font=("Segoe UI", 10))
    style.configure("Primary.TButton", font=("Segoe UI", 10, "bold"))

    style.map(
        "Primary.TButton",
        foreground=[("disabled", "#888888")],
    )

    # -----------------------------
    # Vars (defaults)
    # -----------------------------
    v_pasta_livros = tk.StringVar(value=PASTA_LIVROS or "")
    v_rpa_csv = tk.StringVar(value=RPA_RELATORIO_PATH or "")
    v_sup_xlsx = tk.StringVar(value=SUPERVISOR_XLSX_PATH or "")

    # período (2 campos)
    v_per_ini = tk.StringVar(value=PERIODO_INICIAL or "")
    v_per_fim = tk.StringVar(value=PERIODO_FINAL or "")

    # saída excel
    v_out_xlsx = tk.StringVar(value=RELATORIO_XLSX_PATH or "")

    # -----------------------------
    # Helpers UI
    # -----------------------------
    def pick_dir(var):
        d = filedialog.askdirectory()
        if d:
            var.set(d)

    def pick_file(var, filetypes):
        fp = filedialog.askopenfilename(filetypes=filetypes)
        if fp:
            var.set(fp)

    def pick_save(var):
        fp = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel", "*.xlsx")],
            initialfile=os.path.basename(var.get()) if var.get() else "Relatorio_Auditoria_Prestador.xlsx",
        )
        if fp:
            var.set(fp)

    def _run_main_with_progress():
        # Janela modal de execução
        win = tk.Toplevel(root)
        win.title("Executando")
        win.geometry("480x160")
        win.resizable(False, False)
        win.transient(root)
        win.grab_set()

        fr = ttk.Frame(win, padding=16, style="Card.TFrame")
        fr.pack(fill="both", expand=True)

        ttk.Label(fr, text="Executando auditoria...", font=("Segoe UI", 12, "bold"), background="#ffffff").pack(anchor="w")
        ttk.Label(
            fr,
            text="Aguarde. A janela será fechada automaticamente ao finalizar.",
            font=("Segoe UI", 10),
            background="#ffffff",
            foreground="#555555",
        ).pack(anchor="w", pady=(6, 10))

        pb = ttk.Progressbar(fr, mode="indeterminate")
        pb.pack(fill="x", pady=(8, 10))
        pb.start(10)

        btn_close = ttk.Button(fr, text="Fechar", state="disabled", command=win.destroy)
        btn_close.pack(anchor="e")

        def worker():
            err = None
            try:
                main()
            except Exception as e:
                err = e

            def finish():
                pb.stop()
                btn_close.config(state="normal")
                if err:
                    messagebox.showerror("Erro", f"Ocorreu um erro durante a execução:\n\n{type(err).__name__}: {err}")
                else:
                    messagebox.showinfo("Concluído", f"Execução finalizada.\n\nRelatório gerado em:\n{RELATORIO_XLSX_PATH}")
                try:
                    win.destroy()
                except Exception:
                    pass

            win.after(0, finish)

        threading.Thread(target=worker, daemon=True).start()

    def apply_and_run():
        # validações mínimas
        pasta = v_pasta_livros.get().strip()
        rpa = v_rpa_csv.get().strip()
        sup = v_sup_xlsx.get().strip()
        per_ini = v_per_ini.get().strip()
        per_fim = v_per_fim.get().strip()
        out_xlsx = v_out_xlsx.get().strip()

        if not pasta:
            messagebox.showerror("Erro", "Informe a pasta dos Livros Prefeitura.")
            return
        if not os.path.isdir(pasta):
            messagebox.showerror("Erro", "A pasta dos Livros Prefeitura não foi encontrada.")
            return

        if not rpa:
            messagebox.showerror("Erro", "Informe o CSV do RPA Prefeitura.")
            return
        if not os.path.isfile(rpa):
            messagebox.showerror("Erro", "O arquivo CSV do RPA não foi encontrado.")
            return

        if not sup:
            messagebox.showerror("Erro", "Informe o XLSX do Supervisor (entrada).")
            return
        if not os.path.isfile(sup):
            messagebox.showerror("Erro", "O arquivo XLSX do Supervisor não foi encontrado.")
            return

        if not per_ini or not _validate_date_yyyy_mm_dd(per_ini):
            messagebox.showerror("Erro", "Período Inicial inválido. Use YYYY-MM-DD.")
            return
        if not per_fim or not _validate_date_yyyy_mm_dd(per_fim):
            messagebox.showerror("Erro", "Período Final inválido. Use YYYY-MM-DD.")
            return

        # compara datas
        dt_ini = datetime.strptime(per_ini, "%Y-%m-%d")
        dt_fim = datetime.strptime(per_fim, "%Y-%m-%d")
        if dt_ini > dt_fim:
            messagebox.showerror("Erro", "Período Inicial não pode ser maior que o Período Final.")
            return

        if not out_xlsx:
            messagebox.showerror("Erro", "Informe o caminho de saída do Excel.")
            return

        # aplica nos globais (somente o que a UI controla)
        global PASTA_LIVROS, RPA_RELATORIO_PATH, SUPERVISOR_XLSX_PATH
        global PERIODO_INICIAL, PERIODO_FINAL, DATAFIM_GUIA, CRM_COMPETENCIA
        global RELATORIO_XLSX_PATH

        PASTA_LIVROS = pasta
        RPA_RELATORIO_PATH = rpa
        SUPERVISOR_XLSX_PATH = sup

        PERIODO_INICIAL = per_ini
        PERIODO_FINAL = per_fim

        # Mesma data final alimenta a guia
        DATAFIM_GUIA = per_fim

        # Competência CRM derivada do período final
        CRM_COMPETENCIA = _competencia_mm_yyyy_from_date(per_fim)

        RELATORIO_XLSX_PATH = out_xlsx

        # Executa com janela de progresso
        _run_main_with_progress()

    # -----------------------------
    # Layout
    # -----------------------------
    container = ttk.Frame(root, padding=16)
    container.pack(fill="both", expand=True)

    header = ttk.Frame(container)
    header.pack(fill="x", pady=(0, 12))

    ttk.Label(header, text="Auditoria Prestador", style="Title.TLabel").pack(anchor="w")
    ttk.Label(
        header,
        text="Preencha os caminhos e o período de apuração. O relatório será gerado em Excel.",
        style="Sub.TLabel",
        foreground="#555555",
    ).pack(anchor="w", pady=(4, 0))

    card = ttk.Frame(container, padding=16, style="Card.TFrame")
    card.pack(fill="both", expand=True)

    def field_row(parent, label, var, btn_text=None, btn_cmd=None):
        r = ttk.Frame(parent, style="Card.TFrame")
        r.pack(fill="x", pady=8)

        ttk.Label(r, text=label, style="Label.TLabel", width=30).pack(side="left")
        e = ttk.Entry(r, textvariable=var)
        e.pack(side="left", fill="x", expand=True, padx=(8, 8))

        if btn_text and btn_cmd:
            ttk.Button(r, text=btn_text, command=btn_cmd).pack(side="left")

    # Fontes
    ttk.Label(card, text="Fontes", style="Label.TLabel", font=("Segoe UI", 11, "bold")).pack(anchor="w", pady=(0, 6))

    field_row(card, "Relação Clientes (entrada):", v_sup_xlsx, "Selecionar", lambda: pick_file(v_sup_xlsx, [("Excel", "*.xlsx")]))
    field_row(card, "Pasta Livros Prefeitura CSV:", v_pasta_livros, "Selecionar", lambda: pick_dir(v_pasta_livros))
    field_row(card, "Relatorio Processamento WeBot:", v_rpa_csv, "Selecionar", lambda: pick_file(v_rpa_csv, [("CSV", "*.csv")]))
    

    ttk.Separator(card).pack(fill="x", pady=12)

    # Período
    ttk.Label(card, text="Período de Apuração", style="Label.TLabel", font=("Segoe UI", 11, "bold")).pack(anchor="w", pady=(0, 6))

    period_frame = ttk.Frame(card, style="Card.TFrame")
    period_frame.pack(fill="x", pady=4)

    ttk.Label(period_frame, text="Inicial (YYYY-MM-DD):", style="Label.TLabel", width=22).pack(side="left")
    ttk.Entry(period_frame, textvariable=v_per_ini, width=14).pack(side="left", padx=(8, 4))
    ttk.Button(period_frame, text="📅", width=3, command=lambda: _open_date_picker(root, v_per_ini)).pack(side="left", padx=(0, 20))

    ttk.Label(period_frame, text="Final (YYYY-MM-DD):", style="Label.TLabel", width=20).pack(side="left")
    ttk.Entry(period_frame, textvariable=v_per_fim, width=14).pack(side="left", padx=(8, 4))
    ttk.Button(period_frame, text="📅", width=3, command=lambda: _open_date_picker(root, v_per_fim)).pack(side="left", padx=(0, 0))

    ttk.Label(
        card,
        text="Observação: o Período Final também será usado como DataFim da Guia e define a Competência do CRM (MM/YYYY).",
        style="Label.TLabel",
        foreground="#666666",
    ).pack(anchor="w", pady=(8, 0))

    ttk.Separator(card).pack(fill="x", pady=12)

    # Saída
    ttk.Label(card, text="Saída", style="Label.TLabel", font=("Segoe UI", 11, "bold")).pack(anchor="w", pady=(0, 6))
    field_row(card, "Relatório Excel (.xlsx):", v_out_xlsx, "Salvar como", lambda: pick_save(v_out_xlsx))

    # Botões
    btns = ttk.Frame(container)
    btns.pack(fill="x", pady=(12, 0))

    ttk.Button(btns, text="Cancelar", command=root.destroy).pack(side="right")
    ttk.Button(btns, text="Executar Auditoria", style="Primary.TButton", command=apply_and_run).pack(side="right", padx=(0, 10))

    root.mainloop()




# =============================================================================
# MAIN - liga tudo + gera Excel didático
# =============================================================================
def main():
    print("[1/6] Carregando mapeamento Axio (IM <-> CNPJ/Questor codes)...")
    im_map, doc_map = load_axio_mappings(MYSQL_AXIO)
    print(f"  IMs: {len(im_map)} | Docs(CNPJ/CPF): {len(doc_map)}")

    print("[2/6] Processando LIVROS PREFEITURA (pasta) e consolidando por IM...")
    livros_by_im = process_livros_prefeitura(PASTA_LIVROS, im_map)
    print(f"  IMs com livro consolidado: {len(livros_by_im)}")

    print("[3/6] Processando RPA PREFEITURA (CSV) e calculando status/alertas por IM...")
    df_rpa = read_rpa_relatorio_csv(RPA_RELATORIO_PATH)
    rpa_by_im = rpa_status_by_im(df_rpa)
    print(f"  IMs no RPA: {len(rpa_by_im)}")

    print("[4/6] Lendo relatório do Supervisor (entrada do lote)...")
    df_sup = read_supervisor_xlsx(SUPERVISOR_XLSX_PATH)
    print(f"  Empresas no XLSX: {len(df_sup)}")

    print("[5/6] Consultando CRM (p_crm_cdm) e API Questor por empresa (uma a uma)...")
    print("")

    report_rows = []

    for idx, row in df_sup.iterrows():
        cod_emp_str = row["codigoempresa_questor"]
        cnpj_masked = row["cnpj"]
        cnpj_digits = row["cnpj_digits"]
        razao = row.get("razao", "")
        regime = row.get("regime", "")

        if not cod_emp_str:
            continue

        codigoempresa = int(cod_emp_str)

        axio = doc_map.get(cnpj_digits)  # busca no Axio por CNPJ/CPF
        im_digits = only_digits(axio.get("inscricao_municipal")) if axio else ""

        # codigoestab: tenta Axio; senão default
        codigoestab = CODIGOESTAB_DEFAULT
        if axio and axio.get("CODIGOESTAB") is not None:
            try:
                codigoestab = int(str(axio.get("CODIGOESTAB")).strip())
            except Exception:
                codigoestab = CODIGOESTAB_DEFAULT

        # CRM
        crm_row = crm_fetch_consolidado_por_cnpj(CRM_COMPETENCIA, cnpj_masked)

        # Questor API (novo endpoint consolidado com guia + apuração)
        questor_row = questor_api_fetch(codigoempresa, codigoestab, PERIODO_INICIAL, PERIODO_FINAL, DATAFIM_GUIA)

        # Livro Prefeitura (por IM)
        livro_row = livros_by_im.get(im_digits) if im_digits else None

        # RPA (por IM)
        rpa_row = rpa_by_im.get(im_digits) if im_digits else None

        # variáveis para Excel (default)
        alerta_guia_text = "NÃO APLICÁVEL"
        apuracao_piscofins_status = ""
        lp_base = Decimal("0.00")
        lp_pis_bruto = Decimal("0.00")
        lp_cofins_bruto = Decimal("0.00")
        lp_pis_retido = Decimal("0.00")
        lp_cofins_retido = Decimal("0.00")
        lp_pis_liquido = Decimal("0.00")
        lp_cofins_liquido = Decimal("0.00")
        questor_guia_pis = Decimal("0.00")
        questor_guia_cofins = Decimal("0.00")
        guia_divergencias = []
        guia_valor_baixo_acumulo = False
        guia_valor_baixo_total = Decimal("0.00")

        print("=" * 120)
        print(f"[{idx+1}/{len(df_sup)}] codigoempresa_questor={codigoempresa} codigoestab={codigoestab} | CNPJ={cnpj_masked} | {razao}")

        if axio:
            print(f"  Axio: IM={axio.get('inscricao_municipal')} | Razão={axio.get('razao_social')}")
            print(f"  Axio: CODIGOEMPRESAQUESTOR={axio.get('CODIGOEMPRESAQUESTOR')} CODIGOESTAB={axio.get('CODIGOESTAB')}")
        else:
            print("  Axio: NÃO LOCALIZADO (por CNPJ)")

        # RPA
        if rpa_row:
            print(f"  RPA: Status={rpa_row['Status']} | ParOK={rpa_row['ParOKEncontrado']}")
            if rpa_row["Alertas"]:
                print(f"  RPA: Alertas={', '.join(rpa_row['Alertas'])}")
        else:
            print("  RPA: SEM REGISTRO (por IM)")

        # Livro
        if livro_row:
            print("  Livro Prefeitura (consolidado):")
            print(f"    TotalValorDocumento={livro_row['TotalValorDocumento']}")
            print(f"    TotalPIS={livro_row['TotalPIS']}  TotalCOFINS={livro_row['TotalCOFINS']}  TotalCSLL={livro_row['TotalCSLL']}")
            print(f"    TotalIRRF={livro_row['TotalIRRF']}  TotalINSS={livro_row['TotalINSS']}  Outras={livro_row['TotalOutrasRetencoes']}")
        else:
            print("  Livro Prefeitura: SEM DADOS (por IM)")
        
        # antes (pode quebrar quando livro_row é None)
        # if livro_row.get("QtdeLinhasDivergente", 0) > 0:

        # depois (seguro)
        if livro_row and livro_row.get("QtdeLinhasDivergente", 0) > 0:
            print(f"    ALERTA_LIVRO: Natureza diferente de Exigível ({livro_row['QtdeLinhasDivergente']} linhas) -> {livro_row.get('NaturezasDivergentes')}")

        # CRM
        if crm_row:
            print("  CRM (p_crm_cdm) consolidado:")
            print(f"    QuestorClienteId={crm_row.get('QuestorClienteId')} ClienteCod={crm_row.get('ClienteCod')} Competencia={crm_row.get('Competencia')}")
            print(f"    TotalValorTotal={crm_row.get('TotalValorTotal')} TotalValorLiquido={crm_row.get('TotalValorLiquido')} QtdeNotas={crm_row.get('QtdeNotas')}")
            print(f"    PIS={crm_row.get('TotalPIS')} COFINS={crm_row.get('TotalCOFINS')} CSLL={crm_row.get('TotalCSLL')} IRRF={crm_row.get('TotalIRRF')} INSS={crm_row.get('TotalINSS')}")
        else:
            print("  CRM (p_crm_cdm): SEM DADOS para este CNPJ/competência")

        # Questor
        if questor_row is None:
            print("  Questor API: SEM DADOS")
        elif isinstance(questor_row, dict) and questor_row.get("_erro_http"):
            print(f"  Questor API: ERRO HTTP {questor_row.get('status_code')}")
            print(f"  Body: {questor_row.get('body')}")
        else:
            print("  Questor API (consolidado):")
            print(f"    estab_inscrfederal={questor_row.get('estab_inscrfederal')}")
            print(f"    total_valorcontabil={questor_row.get('total_valorcontabil')}")
            print(f"    PIS={questor_row.get('total_valor_pis')} COFINS={questor_row.get('total_valor_cofins')} CSLL={questor_row.get('total_valor_csll')}")
            print(f"    IRRF={questor_row.get('total_valor_irrf')} INSS={questor_row.get('total_valor_inss')}")

        # --- Auditoria Guia PIS/COFINS (somente Lucro Presumido)
        if is_lucro_presumido(regime):

            # base de cálculo: Livro (preferencial) senão CRM
            if livro_row:
                base_lp = to_dec(livro_row.get("TotalValorDocumento"))
            elif crm_row:
                base_lp = to_dec(crm_row.get("TotalValorTotal"))
            else:
                base_lp = Decimal("0.00")

            # retenções (informativas) - prioridade CRM, senão Livro
            pis_retido = to_dec(crm_row.get("TotalPIS")) if crm_row else Decimal("0.00")
            cofins_retido = to_dec(crm_row.get("TotalCOFINS")) if crm_row else Decimal("0.00")
            if (not crm_row) and livro_row:
                pis_retido = to_dec(livro_row.get("TotalPIS"))
                cofins_retido = to_dec(livro_row.get("TotalCOFINS"))

            # calcula bruto/retido/líquido
            calc = calc_pis_cofins_lp(base_lp, pis_retido, cofins_retido)

            # salvar para Excel
            lp_base = base_lp
            lp_pis_bruto = calc["pis_bruto"]
            lp_cofins_bruto = calc["cofins_bruto"]
            lp_pis_retido = calc["pis_retido"]
            lp_cofins_retido = calc["cofins_retido"]
            lp_pis_liquido = calc["pis_liquido"]
            lp_cofins_liquido = calc["cofins_liquido"]

            print("  GUIA PIS/COFINS (Lucro Presumido):")
            print(f"    BaseLP={base_lp}")
            print(f"    Bruto:   PIS={calc['pis_bruto']} | COFINS={calc['cofins_bruto']}")
            print(f"    Retidos: PIS={calc['pis_retido']} | COFINS={calc['cofins_retido']}")
            print(f"    Líquido (Bruto-Retido): PIS={calc['pis_liquido']} | COFINS={calc['cofins_liquido']}")

            questor_ok = (questor_row and isinstance(questor_row, dict) and not questor_row.get("_erro_http"))
            apuracao_fechada = int(to_dec(questor_row.get("apuracao_fechada"))) if questor_ok else 0
            status_ap = apuracao_status_text(apuracao_fechada)
            apuracao_piscofins_status = status_ap

            print(f"    Apuração PIS/COFINS no Questor: {status_ap}")

            # esperado (líquido) é o que deveria ir para guia
            esperado_pis = calc["pis_liquido"]
            esperado_cofins = calc["cofins_liquido"]

            guia_valor_baixo_total = (esperado_pis + esperado_cofins).quantize(Decimal("0.01"))
            if Decimal("0.00") < guia_valor_baixo_total < Decimal("10.00"):
                guia_valor_baixo_acumulo = True
                if "GUIA_VALOR_BAIXO_POSSIVEL_ACUMULO" not in guia_divergencias:
                    guia_divergencias.append("GUIA_VALOR_BAIXO_POSSIVEL_ACUMULO")

            if questor_ok:
                guia_pis = to_dec(questor_row.get("guia_pis_debito"))
                guia_cofins = to_dec(questor_row.get("guia_cofins_debito"))
                questor_guia_pis = guia_pis
                questor_guia_cofins = guia_cofins

                print(f"    Guia no Questor: PIS={moeda(guia_pis)} | COFINS={moeda(guia_cofins)}")
                print(f"    Esperado a pagar: PIS={moeda(esperado_pis)} | COFINS={moeda(esperado_cofins)}")
                if guia_valor_baixo_acumulo:
                    print(f"    ALERTA_GUIA: valor total esperado da guia ({moeda(guia_valor_baixo_total)}) é maior que 0 e menor que 10. Verificar mês anterior para possível acúmulo.")

                diverg = []
                if not diff_ok(guia_pis, esperado_pis, TOLERANCIA_VALOR):
                    diverg.append("GUIA_PIS_DIVERGENTE")
                if not diff_ok(guia_cofins, esperado_cofins, TOLERANCIA_VALOR):
                    diverg.append("GUIA_COFINS_DIVERGENTE")
                guia_divergencias = list(diverg)

                if int(apuracao_fechada) == 0:
                    if "APURACAO_PISCOFINS_ABERTA" not in guia_divergencias:
                        guia_divergencias.append("APURACAO_PISCOFINS_ABERTA")
                    alerta_guia_text = "APURAÇÃO ABERTA — é necessário fechar a apuração de PIS/COFINS no Questor."
                    print(f"    ALERTA_GUIA: {alerta_guia_text}")
                else:
                    if diverg:
                        alerta_guia_text = f"APURAÇÃO FECHADA — valores da guia divergentes do esperado. ({', '.join(diverg)})"
                        print(f"    ALERTA_GUIA: {alerta_guia_text}")
                    else:
                        alerta_guia_text = "OK — apuração fechada e valores da guia conferem."
                        print(f"    ALERTA_GUIA: {alerta_guia_text}")

            else:
                if "GUIA_QUESTOR_SEM_DADOS" not in guia_divergencias:
                    guia_divergencias.append("GUIA_QUESTOR_SEM_DADOS")
                if "APURACAO_PISCOFINS_ABERTA" not in guia_divergencias:
                    guia_divergencias.append("APURACAO_PISCOFINS_ABERTA")

                print(f"    Esperado a pagar: PIS={moeda(esperado_pis)} | COFINS={moeda(esperado_cofins)}")
                if guia_valor_baixo_acumulo:
                    print(f"    ALERTA_GUIA: valor total esperado da guia ({moeda(guia_valor_baixo_total)}) é maior que 0 e menor que 10. Verificar mês anterior para possível acúmulo.")
                alerta_guia_text = "Não foi possível consultar a guia no Questor (sem dados/erro)."
                print(f"    ALERTA_GUIA: {alerta_guia_text}")
                if int(apuracao_fechada) == 0:
                    alerta_guia_text = "Não foi possível consultar a guia no Questor (sem dados/erro). Ação: fechar a apuração de PIS/COFINS no Questor e tentar novamente."
                    print(f"      Ação: {alerta_guia_text}")

        # >>> ALERTA FINAL
        alerta = build_alerta_final(livro_row, crm_row, questor_row)
        sem_movimento_rpa = is_rpa_sem_movimento(rpa_row)

        divergencias_completas = list(alerta["Divergencias"])
        for d in guia_divergencias:
            if d not in divergencias_completas:
                divergencias_completas.append(d)

        rpa_status_norm = normalize_text(str(rpa_row.get("Status", ""))).lower() if rpa_row else "sem registro"

        print("  ALERTA FINAL:")
        if sem_movimento_rpa:
            status_final = "SEM MOVIMENTO"
            divergencias_finais = "-"
            print(f"    StatusFinal={status_final}")
            print("    Divergencias=-")
            print("    Observação=Empresa sem movimento conforme RPA.")
        elif rpa_status_norm == "alerta":
            status_final = "ALERTA"
            divergencias_finais = ", ".join(rpa_row.get("Alertas", [])) if rpa_row and rpa_row.get("Alertas") else "RPA_STATUS_ALERTA"
            print(f"    StatusFinal={status_final}")
            print(f"    Divergencias={divergencias_finais}")
            print("    Observação=Status final priorizado pelo RPA com ALERTA.")
        elif not rpa_row:
            status_final = "SEM REGISTRO"
            divergencias_finais = "RPA_SEM_REGISTRO"
            print(f"    StatusFinal={status_final}")
            print(f"    Divergencias={divergencias_finais}")
            print("    Observação=Status final priorizado pelo RPA sem registro.")
        else:
            if guia_valor_baixo_acumulo:
                status_final = "ALERTA"
            elif alerta["StatusFinal"] == "SEM_DADOS":
                status_final = "DIVERGENTE"
            else:
                status_final = "DIVERGENTE" if divergencias_completas else alerta["StatusFinal"]

            print(f"    StatusFinal={status_final}")
            if divergencias_completas:
                print(f"    Divergencias={', '.join(divergencias_completas)}")
            else:
                print("    Divergencias=-")

            divergencias_finais = ", ".join(divergencias_completas) if divergencias_completas else "-"

        # linha para Excel (didático)
        questor_ok_for_export = (questor_row and isinstance(questor_row, dict) and not questor_row.get("_erro_http"))

        report_rows.append({
            "codigoempresa_questor": codigoempresa,
            "codigoestab": codigoestab,
            "cnpj": cnpj_masked,
            "razao": razao,
            "im": im_digits,
            "regime": regime,

            "rpa_status": rpa_row.get("Status") if rpa_row else "SEM REGISTRO",
            "rpa_par_ok": rpa_row.get("ParOKEncontrado") if rpa_row else "",
            "rpa_alertas": ", ".join(rpa_row.get("Alertas", [])) if rpa_row else "",

            "livro_total_valor_documento": str(to_dec(livro_row.get("TotalValorDocumento"))) if livro_row else "",
            "livro_pis": str(to_dec(livro_row.get("TotalPIS"))) if livro_row else "",
            "livro_cofins": str(to_dec(livro_row.get("TotalCOFINS"))) if livro_row else "",
            "livro_csll": str(to_dec(livro_row.get("TotalCSLL"))) if livro_row else "",
            "livro_irrf": str(to_dec(livro_row.get("TotalIRRF"))) if livro_row else "",
            "livro_inss": str(to_dec(livro_row.get("TotalINSS"))) if livro_row else "",

            "crm_questorclienteid": crm_row.get("QuestorClienteId") if crm_row else "",
            "crm_clientecod": crm_row.get("ClienteCod") if crm_row else "",
            "crm_competencia": crm_row.get("Competencia") if crm_row else "",
            "crm_qtde_notas": crm_row.get("QtdeNotas") if crm_row else "",
            "crm_total_valor": str(to_dec(crm_row.get("TotalValorTotal"))) if crm_row else "",
            "crm_total_liquido": str(to_dec(crm_row.get("TotalValorLiquido"))) if crm_row else "",
            "crm_pis": str(to_dec(crm_row.get("TotalPIS"))) if crm_row else "",
            "crm_cofins": str(to_dec(crm_row.get("TotalCOFINS"))) if crm_row else "",
            "crm_csll": str(to_dec(crm_row.get("TotalCSLL"))) if crm_row else "",
            "crm_irrf": str(to_dec(crm_row.get("TotalIRRF"))) if crm_row else "",
            "crm_inss": str(to_dec(crm_row.get("TotalINSS"))) if crm_row else "",

            "questor_inscrfederal": questor_row.get("estab_inscrfederal") if questor_ok_for_export else "",
            "questor_total_valorcontabil": str(to_dec(questor_row.get("total_valorcontabil"))) if questor_ok_for_export else "",
            "questor_pis_retido": str(to_dec(questor_row.get("total_valor_pis"))) if questor_ok_for_export else "",
            "questor_cofins_retido": str(to_dec(questor_row.get("total_valor_cofins"))) if questor_ok_for_export else "",
            "questor_csll_retido": str(to_dec(questor_row.get("total_valor_csll"))) if questor_ok_for_export else "",
            "questor_irrf_retido": str(to_dec(questor_row.get("total_valor_irrf"))) if questor_ok_for_export else "",
            "questor_inss_retido": str(to_dec(questor_row.get("total_valor_inss"))) if questor_ok_for_export else "",

            "apuracao_piscofins_status": apuracao_piscofins_status,
            "questor_guia_pis": str(questor_guia_pis),
            "questor_guia_cofins": str(questor_guia_cofins),

            "lp_base": str(lp_base),
            "lp_pis_bruto": str(lp_pis_bruto),
            "lp_cofins_bruto": str(lp_cofins_bruto),
            "lp_pis_retido": str(lp_pis_retido),
            "lp_cofins_retido": str(lp_cofins_retido),
            "lp_pis_liquido": str(lp_pis_liquido),
            "lp_cofins_liquido": str(lp_cofins_liquido),

            "alerta_guia": alerta_guia_text,
            "status_final": status_final,
            "divergencias_finais": divergencias_finais,
        })

        print("")

    # Exporta Excel (Resumo + Divergencias)
    exportar_relatorio_excel(report_rows, RELATORIO_XLSX_PATH)

    print("[6/6] FIM.")


if __name__ == "__main__":
    run_ui()      # abre a interface e depois executa
    # main()      # opcional: deixe comentado só se quiser rodar sem tela