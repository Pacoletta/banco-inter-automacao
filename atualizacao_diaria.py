import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
import json
from tabulate import tabulate

# Configurações de API e PostgreSQL
API_CONFIG = {
    "client_id": "e2213200-4c64-4a8d-bb01-732c220677c9",
    "client_secret": "dda9f6bf-1df0-41ce-8208-c4d00a772d47",
    "cert_path": "./Inter_API_Certificado.crt",
    "key_path": "./Inter_API_Chave.key",
    "token_url": "https://cdpj.partners.bancointer.com.br/oauth/v2/token",
    "transactions_url": "https://cdpj.partners.bancointer.com.br/banking/v2/extrato/completo"
}

DB_CONFIG = {
    "host": "34.44.151.223",
    "database": "banco-inter",
    "user": "pacoleta",
    "password": "123456"
}

# Variáveis globais para o token
token_acesso = None
token_expiration = None

# Função para gerar o token de autenticação
def gerar_token():
    payload = {
        "client_id": API_CONFIG["client_id"],
        "client_secret": API_CONFIG["client_secret"],
        "scope": "extrato.read",
        "grant_type": "client_credentials"
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    try:
        response = requests.post(
            API_CONFIG["token_url"],
            data=payload,
            headers=headers,
            cert=(API_CONFIG["cert_path"], API_CONFIG["key_path"])
        )
        response.raise_for_status()
        token_data = response.json()
        global token_expiration
        token_expiration = datetime.now() + timedelta(seconds=token_data.get("expires_in", 3600))
        print("Token gerado com sucesso.")
        return token_data["access_token"]
    except requests.RequestException as e:
        print(f"Erro ao gerar token: {e}")
        return None

# Função para obter um token válido
def obter_token():
    global token_acesso, token_expiration
    if not token_acesso or datetime.now() >= token_expiration:
        token_acesso = gerar_token()
    return token_acesso

# Função para baixar transações
def baixar_dados_intervalo(data_inicio, data_fim):
    token = obter_token()
    if not token:
        print("Erro ao obter o token.")
        return []

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    params = {"dataInicio": data_inicio, "dataFim": data_fim, "pagina": 0, "tamanhoPagina": 150}

    todas_transacoes = []
    while True:
        try:
            response = requests.get(
                API_CONFIG["transactions_url"],
                headers=headers,
                params=params,
                cert=(API_CONFIG["cert_path"], API_CONFIG["key_path"])
            )
            response.raise_for_status()
            dados = response.json()
            transacoes = dados.get("transacoes", [])
            if transacoes:
                todas_transacoes.extend(transacoes)
                params["pagina"] += 1
            else:
                break
        except requests.RequestException as e:
            if response.status_code == 429:
                print("Erro 429: Limite de requisições excedido. Aguardando 35 segundos...")
                continue
            print(f"Erro ao baixar transações: {e}")
            break
    print(f"Total de transações baixadas: {len(todas_transacoes)}")
    return todas_transacoes

# Função para criar a tabela no banco de dados
def criar_tabela_transacoes():
    query = '''
    CREATE TABLE IF NOT EXISTS transacoes (
        idTransacao TEXT PRIMARY KEY,
        dataInclusao DATE,
        dataTransacao DATE,
        tipoOperacao TEXT,
        tipoTransacao TEXT,
        valor NUMERIC,
        titulo TEXT,
        descricao TEXT,
        detalhes JSON
    );
    '''
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            conn.commit()

# Função para salvar as transações no banco de dados
def salvar_transacoes(df):
    criar_tabela_transacoes()
    df = df.where(pd.notnull(df), None)
    if "detalhes" in df.columns:
        df["detalhes"] = df["detalhes"].apply(lambda x: json.dumps(x) if isinstance(x, dict) else None)

    records = df.to_dict(orient="records")
    query = '''
    INSERT INTO transacoes (idTransacao, dataInclusao, dataTransacao, tipoOperacao, tipoTransacao, valor, titulo, descricao, detalhes)
    VALUES %s ON CONFLICT (idTransacao) DO NOTHING;
    '''
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cursor:
            execute_values(cursor, query, [
                (
                    rec["idTransacao"],
                    rec["dataInclusao"],
                    rec["dataTransacao"],
                    rec["tipoOperacao"],
                    rec["tipoTransacao"],
                    rec["valor"],
                    rec["titulo"],
                    rec["descricao"],
                    rec["detalhes"]
                ) for rec in records
            ])
            conn.commit()

# Script principal para atualização diária
data_inicio = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
data_fim = datetime.today().strftime("%Y-%m-%d")

print(f"Atualizando transações de {data_inicio} até {data_fim}...")

# Baixar transações
transacoes = baixar_dados_intervalo(data_inicio, data_fim)

# Exibir no terminal antes de salvar
if transacoes:
    df_transacoes = pd.DataFrame(transacoes)

    print("\nPré-visualização das transações extraídas:")
    print(tabulate(df_transacoes.head(10), headers="keys", tablefmt="grid"))

    print("\nResumo das transações:")
    print(df_transacoes.describe())

    salvar = input("\nDeseja salvar essas transações no banco de dados? (s/n): ").strip().lower()
    if salvar == "s":
        salvar_transacoes(df_transacoes)
        print("Transações salvas no banco de dados com sucesso.")
    else:
        print("Operação cancelada. As transações não foram salvas.")
else:
    print("Nenhuma transação encontrada para o período especificado.")