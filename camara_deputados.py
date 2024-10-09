import requests
import pandas as pd
from google.colab import auth
from google.cloud import storage
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from datetime import datetime

# Função para configurar a sessão de requisição com retry e timeout
def configurar_sessao():
    session = requests.Session()
    retry = Retry(
        total=5,  # Número de tentativas
        backoff_factor=1,  # Tempo de espera entre as tentativas (exponencial)
        status_forcelist=[429, 500, 502, 503, 504],  # Códigos HTTP para retry
        allowed_methods=["GET"],  # Métodos HTTP a aplicar retry
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session

# Configurar a sessão de requisição
sessao = configurar_sessao()

# URL para obter a lista de todos os deputados
url_deputados = "https://dadosabertos.camara.leg.br/api/v2/deputados"

# Parâmetros para a lista de deputados
params_deputados = {
    'itens': 100,
    'pagina': 1
}

# Função para capturar despesas de um deputado específico
def obter_despesas_deputado(deputado_id):
    url_despesas = f"https://dadosabertos.camara.leg.br/api/v2/deputados/{deputado_id}/despesas"
    params_despesas = {
        'ano': [2024],
        'itens': 100,
        'pagina': 1,
        'ordem': 'desc',
        'ordenarPor': 'dataDocumento'
    }
    despesas = []
    while True:
        try:
            response = sessao.get(url_despesas, params=params_despesas, timeout=10)
            if response.status_code == 200:
                data = response.json()['dados']
                if not data:
                    break
                for item in data:
                    item['idDeputado'] = deputado_id  # Adicionar ID do deputado em cada item
                despesas.extend(data)
                params_despesas['pagina'] += 1
            else:
                print(f"Erro ao obter despesas do deputado {deputado_id}: {response.status_code}")
                break
        except requests.exceptions.Timeout:
            print(f"Timeout na requisição para o deputado {deputado_id}. Tentando novamente...")
    return despesas

# Função para capturar informações adicionais dos deputados
def obter_info_deputado(deputado_id):
    url_info = f"https://dadosabertos.camara.leg.br/api/v2/deputados/{deputado_id}"
    try:
        response = sessao.get(url_info, timeout=10)
        if response.status_code == 200:
            data = response.json()['dados']
            info = {
                'id': data['id'],
                'nome': data['ultimoStatus']['nome'],
                'partido': data['ultimoStatus']['siglaPartido'],
                'estado': data['ultimoStatus']['siglaUf']
            }
            return info
        else:
            print(f"Erro ao obter info do deputado {deputado_id}: {response.status_code}")
            return None
    except requests.exceptions.Timeout:
        print(f"Timeout ao obter info do deputado {deputado_id}.")
        return None

# Função para capturar a lista de deputados
def obter_lista_deputados():
    deputados = []
    while True:
        try:
            response = sessao.get(url_deputados, params=params_deputados, timeout=10)
            if response.status_code == 200:
                data = response.json()['dados']
                if not data:
                    break
                deputados.extend(data)
                params_deputados['pagina'] += 1
            else:
                print(f"Erro ao obter lista de deputados: {response.status_code}")
                break
        except requests.exceptions.Timeout:
            print("Timeout ao obter lista de deputados. Tentando novamente...")
    return deputados

# Obter a lista completa de deputados
lista_deputados = obter_lista_deputados()

# Inicializar lista para armazenar todas as despesas
todas_despesas = []
deputados_info = []

# Iterar sobre cada deputado e capturar suas despesas e informações
for deputado in lista_deputados:
    deputado_id = deputado['id']
    nome_deputado = deputado['nome']

    print(f"Capturando despesas do deputado: {nome_deputado} (ID: {deputado_id})")

    # Obter despesas do deputado
    despesas_deputado = obter_despesas_deputado(deputado_id)
    todas_despesas.extend(despesas_deputado)

    # Obter informações adicionais do deputado
    info_deputado = obter_info_deputado(deputado_id)
    if info_deputado:
        deputados_info.append(info_deputado)

# Converter a lista de despesas e informações em DataFrames
df_despesas = pd.DataFrame(todas_despesas)
df_info_deputados = pd.DataFrame(deputados_info)

# Verifique as colunas de df_despesas
print(df_despesas.columns)

# Juntar as informações de deputados com as despesas
df_completo = pd.merge(df_despesas, df_info_deputados, left_on='idDeputado', right_on='id', how='left')

# Exibir as primeiras linhas do DataFrame para visualização
print(df_completo.head())

# Obter a data atual para incluir no nome do arquivo
data_atual = datetime.now().strftime("%Y-%m-%d")

# Definir o nome do arquivo com a data atual
file_name = f'despesas_deputados_{data_atual}.csv'

# Salvar o DataFrame como CSV no Google Cloud Storage
auth.authenticate_user()

# Criar o cliente do Google Cloud Storage
client = storage.Client()

# Defina o nome do bucket e o arquivo que será salvo
bucket_name = 'camara_deputados'

# Referenciar o bucket e o arquivo
bucket = client.bucket(bucket_name)
blob = bucket.blob(file_name)

# Salvar o CSV no bucket
df_completo.to_csv(file_name, index=False)
blob.upload_from_filename(file_name)

print(f"Arquivo salvo com sucesso no bucket {bucket_name} como {file_name}.")
