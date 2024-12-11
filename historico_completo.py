import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
import json
import time
from tabulate import tabulate  # Para exibir tabelas formatadas no terminal

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
                time.sleep(35)
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

# Função para processar mês a mês
def processar_mes_a_mes(data_inicial, data_final):
    data_inicio = datetime.strptime(data_inicial, "%Y-%m-%d")
    data_fim = datetime.strptime(data_final, "%Y-%m-%d")

    while data_inicio <= data_fim:
        data_inicio_str = data_inicio.strftime("%Y-%m-%d")
        ultimo_dia_mes = (data_inicio.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        data_fim_mes = min(ultimo_dia_mes, data_fim)

        print(f"Processando transações de {data_inicio_str} a {data_fim_mes.strftime('%Y-%m-%d')}...")
        transacoes = baixar_dados_intervalo(data_inicio_str, data_fim_mes.strftime("%Y-%m-%d"))
        
        if transacoes:
            df_transacoes = pd.DataFrame(transacoes)

            # Exibição no terminal
            print("\nPré-visualização das transações baixadas:")
            print(tabulate(df_transacoes.head(10), headers="keys", tablefmt="grid"))

            salvar = input("\nDeseja salvar essas transações no banco de dados? (s/n): ").strip().lower()
            if salvar == "s":
                salvar_transacoes(df_transacoes)
                print("Transações salvas no banco com sucesso.")
            else:
                print("Salvamento cancelado pelo usuário.")

        data_inicio = (data_inicio + timedelta(days=31)).replace(day=1)
        time.sleep(35)

# Execução
data_inicio = "2023-01-01"
data_fim = datetime.today().strftime("%Y-%m-%d")
processar_mes_a_mes(data_inicio, data_fim)