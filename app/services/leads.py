import pandas as pd

def format_leads(leads: pd.DataFrame) -> pd.DataFrame:
    leads = leads.copy()
    leads["CNPJ"] = (
        leads["cnpj_basico"].astype(str).str.zfill(8).str.replace(r"(\d{2})(\d{3})(\d{3})", r"\1.\2.\3", regex=True)
        + "/"
        + leads["cnpj_ordem"].astype(str).str.zfill(4)
        + "-"
        + leads["cnpj_dv"].astype(str).str.zfill(2)
    )
    
    # Mover coluna CNPJ para o início
    cols = leads.columns.tolist()
    cols.insert(0, cols.pop(cols.index("CNPJ")))
    leads = leads[cols]
    
    leads["Porte"] = leads["porte"].map({
        0: "NÃO INFORMADO",
        1: "MICRO EMPRESA",
        3: "EMPRESA DE PEQUENO PORTE",
        5: "DEMAIS"
    }).fillna(leads["porte"])
    
    leads["Situacao"] = leads["situacao_cadastral"].map({
        1: "NULA",
        2: "ATIVA",
        3: "SUSPENSA",
        4: "INAPTA",
        8: "BAIXADA"
    }).fillna(leads["situacao_cadastral"])
    
    leads["Data de abertura"] = pd.to_datetime(leads["Data de abertura"], errors='coerce').dt.strftime('%d/%m/%Y')
    
    leads.drop(columns=["cnpj_basico", "cnpj_ordem", "cnpj_dv", "porte", "situacao_cadastral"], inplace=True)
    
    return leads
