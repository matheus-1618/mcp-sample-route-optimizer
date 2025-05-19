
import boto3
import uuid
import random
import time
from datetime import datetime, timedelta
from decimal import Decimal

# Inicializar o cliente do DynamoDB
dynamodb = boto3.resource('dynamodb',region_name="us-east-1")
client = boto3.client('dynamodb',region_name="us-east-1")

# Função para verificar se uma tabela existe
def table_exists(table_name):
    try:
        client.describe_table(TableName=table_name)
        return True
    except client.exceptions.ResourceNotFoundException:
        return False

# Função para esperar até que a tabela esteja ativa
def wait_for_active_table(table_name):
    print(f"Aguardando a tabela {table_name} ficar ativa...")
    while True:
        response = client.describe_table(TableName=table_name)
        status = response['Table']['TableStatus']
        if status == 'ACTIVE':
            break
        print(f"Status da tabela: {status}, aguardando...")
        time.sleep(5)
    print(f"Tabela {table_name} está ativa!")

# Lista de localizações no Brasil para uso nos dados
localizacoes_brasil = [
    {
        'cidade': 'São Paulo',
        'estado': 'SP',
        'bairro': 'Vila Olímpia',
        'cep': '04551-000',
        'coordenadas': {'latitude': Decimal(str(-23.5959)), 'longitude': Decimal(str(-46.6847))}
    },
    {
        'cidade': 'São Paulo',
        'estado': 'SP',
        'bairro': 'Moema',
        'cep': '04077-000',
        'coordenadas': {'latitude': Decimal(str(-23.6008)), 'longitude': Decimal(str(-46.6680))}
    },
    {
        'cidade': 'São Bernardo do Campo',
        'estado': 'SP',
        'bairro': 'Centro',
        'cep': '09750-000',
        'coordenadas': {'latitude': Decimal(str(-23.6944)), 'longitude': Decimal(str(-46.5654))}
    },
    {
        'cidade': 'São Bernardo do Campo',
        'estado': 'SP',
        'bairro': 'Rudge Ramos',
        'cep': '09636-000',
        'coordenadas': {'latitude': Decimal(str(-23.6667)), 'longitude': Decimal(str(-46.5667))}
    },
    {
        'cidade': 'Santo André',
        'estado': 'SP',
        'bairro': 'Centro',
        'cep': '09010-000',
        'coordenadas': {'latitude': Decimal(str(-23.6639)), 'longitude': Decimal(str(-46.5383))}
    },
    {
        'cidade': 'Rio de Janeiro',
        'estado': 'RJ',
        'bairro': 'Copacabana',
        'cep': '22070-000',
        'coordenadas': {'latitude': Decimal(str(-22.9711)), 'longitude': Decimal(str(-43.1863))}
    },
    {
        'cidade': 'Rio de Janeiro',
        'estado': 'RJ',
        'bairro': 'Barra da Tijuca',
        'cep': '22640-100',
        'coordenadas': {'latitude': Decimal(str(-23.0000)), 'longitude': Decimal(str(-43.3650))}
    },
    {
        'cidade': 'Belo Horizonte',
        'estado': 'MG',
        'bairro': 'Savassi',
        'cep': '30130-170',
        'coordenadas': {'latitude': Decimal(str(-19.9369)), 'longitude': Decimal(str(-43.9380))}
    },
    {
        'cidade': 'Curitiba',
        'estado': 'PR',
        'bairro': 'Batel',
        'cep': '80420-090',
        'coordenadas': {'latitude': Decimal(str(-25.4428)), 'longitude': Decimal(str(-49.2889))}
    },
    {
        'cidade': 'Porto Alegre',
        'estado': 'RS',
        'bairro': 'Moinhos de Vento',
        'cep': '90570-000',
        'coordenadas': {'latitude': Decimal(str(-30.0277)), 'longitude': Decimal(str(-51.2038))}
    },
    {
        'cidade': 'Salvador',
        'estado': 'BA',
        'bairro': 'Barra',
        'cep': '40140-130',
        'coordenadas': {'latitude': Decimal(str(-13.0089)), 'longitude': Decimal(str(-38.5186))}
    },
    {
        'cidade': 'Recife',
        'estado': 'PE',
        'bairro': 'Boa Viagem',
        'cep': '51030-300',
        'coordenadas': {'latitude': Decimal(str(-8.1200)), 'longitude': Decimal(str(-34.9000))}
    },
    {
        'cidade': 'Fortaleza',
        'estado': 'CE',
        'bairro': 'Meireles',
        'cep': '60165-150',
        'coordenadas': {'latitude': Decimal(str(-3.7319)), 'longitude': Decimal(str(-38.5267))}
    },
    {
        'cidade': 'Brasília',
        'estado': 'DF',
        'bairro': 'Asa Sul',
        'cep': '70200-000',
        'coordenadas': {'latitude': Decimal(str(-15.7975)), 'longitude': Decimal(str(-47.8919))}
    },
    {
        'cidade': 'Manaus',
        'estado': 'AM',
        'bairro': 'Adrianópolis',
        'cep': '69057-000',
        'coordenadas': {'latitude': Decimal(str(-3.0833)), 'longitude': Decimal(str(-60.0000))}
    }
]

# Centros de distribuição da transportadora
centros_distribuicao = [
    {
        'nome': 'CD São Bernardo do Campo',
        'cidade': 'São Bernardo do Campo',
        'estado': 'SP',
        'bairro': 'Demarchi',
        'cep': '09820-000',
        'coordenadas': {'latitude': Decimal(str(-23.7214)), 'longitude': Decimal(str(-46.5658))}
    },
    {
        'nome': 'CD Guarulhos',
        'cidade': 'Guarulhos',
        'estado': 'SP',
        'bairro': 'Cumbica',
        'cep': '07180-000',
        'coordenadas': {'latitude': Decimal(str(-23.4356)), 'longitude': Decimal(str(-46.5358))}
    },
    {
        'nome': 'CD Rio de Janeiro',
        'cidade': 'Rio de Janeiro',
        'estado': 'RJ',
        'bairro': 'Jacarepaguá',
        'cep': '22775-000',
        'coordenadas': {'latitude': Decimal(str(-22.9489)), 'longitude': Decimal(str(-43.3920))}
    },
    {
        'nome': 'CD Belo Horizonte',
        'cidade': 'Contagem',
        'estado': 'MG',
        'bairro': 'Cinco',
        'cep': '32010-000',
        'coordenadas': {'latitude': Decimal(str(-19.9322)), 'longitude': Decimal(str(-44.0711))}
    },
    {
        'nome': 'CD Curitiba',
        'cidade': 'Curitiba',
        'estado': 'PR',
        'bairro': 'Cidade Industrial',
        'cep': '81450-000',
        'coordenadas': {'latitude': Decimal(str(-25.4950)), 'longitude': Decimal(str(-49.3549))}
    }
]

# 1. Tabela de Produtos
table_name = "mcp-produtos"
if not table_exists(table_name):
    print(f"Criando tabela {table_name}...")
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {'AttributeName': 'produto_id', 'KeyType': 'HASH'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'produto_id', 'AttributeType': 'S'}
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    wait_for_active_table(table_name)
else:
    table = dynamodb.Table(table_name)
    print(f"Tabela {table_name} já existe.")

# Inserir dados de produtos com localização
produtos = [
    {
        'produto_id': str(uuid.uuid4()),
        'nome': 'Smartphone Galaxy S23',
        'categoria': 'Eletrônicos',
        'peso': Decimal('0.25'),
        'dimensoes': {'altura': Decimal('15.5'), 'largura': Decimal('7.5'), 'profundidade': Decimal('0.8')},
        'valor': Decimal('4999.99'),
        'fragilidade': 'Alta',
        'localizacao': {
            'centro_distribuicao': 'CD São Bernardo do Campo',
            'cidade': 'São Bernardo do Campo',
            'estado': 'SP',
            'bairro': 'Demarchi',
            'cep': '09820-000',
            'coordenadas': {'latitude': Decimal(str(-23.7214)), 'longitude': Decimal(str(-46.5658))}
        }
    },
    {
        'produto_id': str(uuid.uuid4()),
        'nome': 'Notebook Dell XPS',
        'categoria': 'Eletrônicos',
        'peso': Decimal('1.8'),
        'dimensoes': {'altura': Decimal('30.0'), 'largura': Decimal('20.0'), 'profundidade': Decimal('1.5')},
        'valor': Decimal('8999.99'),
        'fragilidade': 'Alta',
        'localizacao': {
            'centro_distribuicao': 'CD São Bernardo do Campo',
            'cidade': 'São Bernardo do Campo',
            'estado': 'SP',
            'bairro': 'Demarchi',
            'cep': '09820-000',
            'coordenadas': {'latitude': Decimal(str(-23.7214)), 'longitude': Decimal(str(-46.5658))}
        }
    },
    {
        'produto_id': str(uuid.uuid4()),
        'nome': 'Tênis Nike Air Max',
        'categoria': 'Vestuário',
        'peso': Decimal('0.5'),
        'dimensoes': {'altura': Decimal('12.0'), 'largura': Decimal('20.0'), 'profundidade': Decimal('30.0')},
        'valor': Decimal('899.99'),
        'fragilidade': 'Baixa',
        'localizacao': {
            'centro_distribuicao': 'CD Guarulhos',
            'cidade': 'Guarulhos',
            'estado': 'SP',
            'bairro': 'Cumbica',
            'cep': '07180-000',
            'coordenadas': {'latitude': Decimal(str(-23.4356)), 'longitude': Decimal(str(-46.5358))}
        }
    },
    {
        'produto_id': str(uuid.uuid4()),
        'nome': 'Livro Clean Code',
        'categoria': 'Livros',
        'peso': Decimal('0.4'),
        'dimensoes': {'altura': Decimal('24.0'), 'largura': Decimal('17.0'), 'profundidade': Decimal('2.5')},
        'valor': Decimal('129.90'),
        'fragilidade': 'Média',
        'localizacao': {
            'centro_distribuicao': 'CD São Bernardo do Campo',
            'cidade': 'São Bernardo do Campo',
            'estado': 'SP',
            'bairro': 'Demarchi',
            'cep': '09820-000',
            'coordenadas': {'latitude': Decimal(str(-23.7214)), 'longitude': Decimal(str(-46.5658))}
        }
    },
    {
        'produto_id': str(uuid.uuid4()),
        'nome': 'Cafeteira Elétrica',
        'categoria': 'Eletrodomésticos',
        'peso': Decimal('2.3'),
        'dimensoes': {'altura': Decimal('35.0'), 'largura': Decimal('20.0'), 'profundidade': Decimal('15.0')},
        'valor': Decimal('349.90'),
        'fragilidade': 'Média',
        'localizacao': {
            'centro_distribuicao': 'CD Rio de Janeiro',
            'cidade': 'Rio de Janeiro',
            'estado': 'RJ',
            'bairro': 'Jacarepaguá',
            'cep': '22775-000',
            'coordenadas': {'latitude': Decimal(str(-22.9489)), 'longitude': Decimal(str(-43.3920))}
        }
    },
    {
        'produto_id': str(uuid.uuid4()),
        'nome': 'Monitor Ultrawide',
        'categoria': 'Eletrônicos',
        'peso': Decimal('5.2'),
        'dimensoes': {'altura': Decimal('60.0'), 'largura': Decimal('25.0'), 'profundidade': Decimal('10.0')},
        'valor': Decimal('2499.99'),
        'fragilidade': 'Alta',
        'localizacao': {
            'centro_distribuicao': 'CD São Bernardo do Campo',
            'cidade': 'São Bernardo do Campo',
            'estado': 'SP',
            'bairro': 'Demarchi',
            'cep': '09820-000',
            'coordenadas': {'latitude': Decimal(str(-23.7214)), 'longitude': Decimal(str(-46.5658))}
        }
    },
    {
        'produto_id': str(uuid.uuid4()),
        'nome': 'Cadeira Gamer',
        'categoria': 'Móveis',
        'peso': Decimal('15.0'),
        'dimensoes': {'altura': Decimal('120.0'), 'largura': Decimal('65.0'), 'profundidade': Decimal('65.0')},
        'valor': Decimal('1299.90'),
        'fragilidade': 'Média',
        'localizacao': {
            'centro_distribuicao': 'CD Belo Horizonte',
            'cidade': 'Contagem',
            'estado': 'MG',
            'bairro': 'Cinco',
            'cep': '32010-000',
            'coordenadas': {'latitude': Decimal(str(-19.9322)), 'longitude': Decimal(str(-44.0711))}
        }
    },
    {
        'produto_id': str(uuid.uuid4()),
        'nome': 'Fone de Ouvido Bluetooth',
        'categoria': 'Eletrônicos',
        'peso': Decimal('0.2'),
        'dimensoes': {'altura': Decimal('18.0'), 'largura': Decimal('8.0'), 'profundidade': Decimal('8.0')},
        'valor': Decimal('399.90'),
        'fragilidade': 'Média',
        'localizacao': {
            'centro_distribuicao': 'CD São Bernardo do Campo',
            'cidade': 'São Bernardo do Campo',
            'estado': 'SP',
            'bairro': 'Demarchi',
            'cep': '09820-000',
            'coordenadas': {'latitude': Decimal(str(-23.7214)), 'longitude': Decimal(str(-46.5658))}
        }
    },
    {
        'produto_id': str(uuid.uuid4()),
        'nome': 'Mochila para Notebook',
        'categoria': 'Acessórios',
        'peso': Decimal('0.8'),
        'dimensoes': {'altura': Decimal('45.0'), 'largura': Decimal('30.0'), 'profundidade': Decimal('15.0')},
        'valor': Decimal('199.90'),
        'fragilidade': 'Baixa',
        'localizacao': {
            'centro_distribuicao': 'CD Curitiba',
            'cidade': 'Curitiba',
            'estado': 'PR',
            'bairro': 'Cidade Industrial',
            'cep': '81450-000',
            'coordenadas': {'latitude': Decimal(str(-25.4950)), 'longitude': Decimal(str(-49.3549))}
        }
    },
    {
        'produto_id': str(uuid.uuid4()),
        'nome': 'Relógio Inteligente',
        'categoria': 'Eletrônicos',
        'peso': Decimal('0.05'),
        'dimensoes': {'altura': Decimal('4.5'), 'largura': Decimal('3.8'), 'profundidade': Decimal('1.0')},
        'valor': Decimal('1499.90'),
        'fragilidade': 'Alta',
        'localizacao': {
            'centro_distribuicao': 'CD São Bernardo do Campo',
            'cidade': 'São Bernardo do Campo',
            'estado': 'SP',
            'bairro': 'Demarchi',
            'cep': '09820-000',
            'coordenadas': {'latitude': Decimal(str(-23.7214)), 'longitude': Decimal(str(-46.5658))}
        }
    }
]

print(f"Inserindo dados na tabela {table_name}...")
for produto in produtos:
    table.put_item(Item=produto)
    print(f"Produto inserido: {produto['nome']}")

# Armazenar IDs dos produtos para uso posterior
produto_ids = [produto['produto_id'] for produto in produtos]

# 2. Tabela de Clientes
table_name = "mcp-clientes"
if not table_exists(table_name):
    print(f"Criando tabela {table_name}...")
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {'AttributeName': 'cliente_id', 'KeyType': 'HASH'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'cliente_id', 'AttributeType': 'S'}
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    wait_for_active_table(table_name)
else:
    table = dynamodb.Table(table_name)
    print(f"Tabela {table_name} já existe.")

# Inserir dados de clientes com localização
clientes = [
    {
        'cliente_id': str(uuid.uuid4()),
        'nome': 'João Silva',
        'email': 'joao.silva@email.com',
        'telefone': '(11) 98765-4321',
        'endereco': {
            'rua': 'Av. Paulista',
            'numero': '1000',
            'complemento': 'Apto 123',
            'bairro': 'Bela Vista',
            'cidade': 'São Paulo',
            'estado': 'SP',
            'cep': '01310-100'
        },
        'localizacao': {
            'cidade': 'São Paulo',
            'estado': 'SP',
            'bairro': 'Bela Vista',
            'cep': '01310-100',
            'coordenadas': {'latitude': Decimal(str(-23.5632)), 'longitude': Decimal(str(-46.6541))}
        },
        'data_cadastro': datetime.now().isoformat()
    },
    {
        'cliente_id': str(uuid.uuid4()),
        'nome': 'Maria Oliveira',
        'email': 'maria.oliveira@email.com',
        'telefone': '(21) 98765-1234',
        'endereco': {
            'rua': 'Rua Copacabana',
            'numero': '500',
            'complemento': 'Bloco B',
            'bairro': 'Copacabana',
            'cidade': 'Rio de Janeiro',
            'estado': 'RJ',
            'cep': '22050-002'
        },
        'localizacao': {
            'cidade': 'Rio de Janeiro',
            'estado': 'RJ',
            'bairro': 'Copacabana',
            'cep': '22050-002',
            'coordenadas': {'latitude': Decimal(str(-22.9711)), 'longitude': Decimal(str(-43.1863))}
        },
        'data_cadastro': datetime.now().isoformat()
    },
    {
        'cliente_id': str(uuid.uuid4()),
        'nome': 'Pedro Santos',
        'email': 'pedro.santos@email.com',
        'telefone': '(31) 99876-5432',
        'endereco': {
            'rua': 'Av. Afonso Pena',
            'numero': '1500',
            'complemento': 'Sala 303',
            'bairro': 'Centro',
            'cidade': 'Belo Horizonte',
            'estado': 'MG',
            'cep': '30130-003'
        },
        'localizacao': {
            'cidade': 'Belo Horizonte',
            'estado': 'MG',
            'bairro': 'Centro',
            'cep': '30130-003',
            'coordenadas': {'latitude': Decimal(str(-19.9227)), 'longitude': Decimal(str(-43.9451))}
        },
        'data_cadastro': datetime.now().isoformat()
    },
    {
        'cliente_id': str(uuid.uuid4()),
        'nome': 'Ana Costa',
        'email': 'ana.costa@email.com',
        'telefone': '(41) 98888-7777',
        'endereco': {
            'rua': 'Rua XV de Novembro',
            'numero': '700',
            'complemento': '',
            'bairro': 'Centro',
            'cidade': 'Curitiba',
            'estado': 'PR',
            'cep': '80020-310'
        },
        'localizacao': {
            'cidade': 'Curitiba',
            'estado': 'PR',
            'bairro': 'Centro',
            'cep': '80020-310',
            'coordenadas': {'latitude': Decimal(str(-25.4290)), 'longitude': Decimal(str(-49.2671))}
        },
        'data_cadastro': datetime.now().isoformat()
    },
    {
        'cliente_id': str(uuid.uuid4()),
        'nome': 'Carlos Ferreira',
        'email': 'carlos.ferreira@email.com',
        'telefone': '(51) 97777-8888',
        'endereco': {
            'rua': 'Av. Ipiranga',
            'numero': '1200',
            'complemento': 'Casa',
            'bairro': 'Partenon',
            'cidade': 'Porto Alegre',
            'estado': 'RS',
            'cep': '90160-093'
        },
        'localizacao': {
            'cidade': 'Porto Alegre',
            'estado': 'RS',
            'bairro': 'Partenon',
            'cep': '90160-093',
            'coordenadas': {'latitude': Decimal(str(-30.0368)), 'longitude': Decimal(str(-51.2090))}
        },
        'data_cadastro': datetime.now().isoformat()
    },
    {
        'cliente_id': str(uuid.uuid4()),
        'nome': 'Fernanda Lima',
        'email': 'fernanda.lima@email.com',
        'telefone': '(81) 96666-5555',
        'endereco': {
            'rua': 'Av. Boa Viagem',
            'numero': '3000',
            'complemento': 'Apto 1502',
            'bairro': 'Boa Viagem',
            'cidade': 'Recife',
            'estado': 'PE',
            'cep': '51030-000'
        },
        'localizacao': {
            'cidade': 'Recife',
            'estado': 'PE',
            'bairro': 'Boa Viagem',
            'cep': '51030-000',
            'coordenadas': {'latitude': Decimal(str(-8.1200)), 'longitude': Decimal(str(-34.9000))}
        },
        'data_cadastro': datetime.now().isoformat()
    },
    {
        'cliente_id': str(uuid.uuid4()),
        'nome': 'Roberto Almeida',
        'email': 'roberto.almeida@email.com',
        'telefone': '(62) 95555-4444',
        'endereco': {
            'rua': 'Av. 85',
            'numero': '2200',
            'complemento': 'Qd. 10 Lt. 20',
            'bairro': 'Setor Marista',
            'cidade': 'Goiânia',
            'estado': 'GO',
            'cep': '74160-010'
        },
        'localizacao': {
            'cidade': 'Goiânia',
            'estado': 'GO',
            'bairro': 'Setor Marista',
            'cep': '74160-010',
            'coordenadas': {'latitude': Decimal(str(-16.6799)), 'longitude': Decimal(str(-49.2550))}
        },
        'data_cadastro': datetime.now().isoformat()
    },
    {
        'cliente_id': str(uuid.uuid4()),
        'nome': 'Luciana Martins',
        'email': 'luciana.martins@email.com',
        'telefone': '(71) 94444-3333',
        'endereco': {
            'rua': 'Av. Tancredo Neves',
            'numero': '1500',
            'complemento': 'Sala 1010',
            'bairro': 'Caminho das Árvores',
            'cidade': 'Salvador',
            'estado': 'BA',
            'cep': '41820-021'
        },
        'localizacao': {
            'cidade': 'Salvador',
            'estado': 'BA',
            'bairro': 'Caminho das Árvores',
            'cep': '41820-021',
            'coordenadas': {'latitude': Decimal(str(-12.9833)), 'longitude': Decimal(str(-38.4833))}
        },
        'data_cadastro': datetime.now().isoformat()
    },
    {
        'cliente_id': str(uuid.uuid4()),
        'nome': 'Marcelo Souza',
        'email': 'marcelo.souza@email.com',
        'telefone': '(92) 93333-2222',
        'endereco': {
            'rua': 'Av. Djalma Batista',
            'numero': '1000',
            'complemento': '',
            'bairro': 'Chapada',
            'cidade': 'Manaus',
            'estado': 'AM',
            'cep': '69050-010'
        },
        'localizacao': {
            'cidade': 'Manaus',
            'estado': 'AM',
            'bairro': 'Chapada',
            'cep': '69050-010',
            'coordenadas': {'latitude': Decimal(str(-3.0833)), 'longitude': Decimal(str(-60.0000))}
        },
        'data_cadastro': datetime.now().isoformat()
    },
    {
        'cliente_id': str(uuid.uuid4()),
        'nome': 'Juliana Pereira',
        'email': 'juliana.pereira@email.com',
        'telefone': '(85) 92222-1111',
        'endereco': {
            'rua': 'Av. Beira Mar',
            'numero': '500',
            'complemento': 'Apto 2001',
            'bairro': 'Meireles',
            'cidade': 'Fortaleza',
            'estado': 'CE',
            'cep': '60165-121'
        },
        'localizacao': {
            'cidade': 'Fortaleza',
            'estado': 'CE',
            'bairro': 'Meireles',
            'cep': '60165-121',
            'coordenadas': {'latitude': Decimal(str(-3.7319)), 'longitude': Decimal(str(-38.5267))}
        },
        'data_cadastro': datetime.now().isoformat()
    }
]

print(f"Inserindo dados na tabela {table_name}...")
for cliente in clientes:
    table.put_item(Item=cliente)
    print(f"Cliente inserido: {cliente['nome']}")

# Armazenar IDs dos clientes para uso posterior
cliente_ids = [cliente['cliente_id'] for cliente in clientes]

# 3. Tabela de Pedidos
table_name = "mcp-pedidos"
if not table_exists(table_name):
    print(f"Criando tabela {table_name}...")
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {'AttributeName': 'pedido_id', 'KeyType': 'HASH'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'pedido_id', 'AttributeType': 'S'},
            {'AttributeName': 'cliente_id', 'AttributeType': 'S'}
        ],
        GlobalSecondaryIndexes=[
            {
                'IndexName': 'cliente_id-index',
                'KeySchema': [
                    {'AttributeName': 'cliente_id', 'KeyType': 'HASH'}
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                }
            }
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    wait_for_active_table(table_name)
else:
    table = dynamodb.Table(table_name)
    print(f"Tabela {table_name} já existe.")

# Gerar datas aleatórias recentes
def random_date(start_date, days_back):
    return (start_date - timedelta(days=random.randint(0, days_back))).isoformat()

# Status possíveis para um pedido
status_pedido = ['Aguardando Pagamento', 'Pagamento Confirmado', 'Em Separação', 'Em Transporte', 'Entregue', 'Cancelado']

# Inserir dados de pedidos
pedidos = []
for i in range(10):
    # Selecionar cliente aleatório
    cliente_idx = random.randint(0, len(clientes) - 1)
    cliente_id = cliente_ids[cliente_idx]
    cliente = clientes[cliente_idx]
    
    # Gerar itens do pedido (1 a 5 produtos aleatórios)
    num_itens = random.randint(1, 5)
    itens = []
    valor_total = Decimal('0.0')
    
    for _ in range(num_itens):
        produto_idx = random.randint(0, len(produtos) - 1)
        produto_id = produto_ids[produto_idx]
        produto = produtos[produto_idx]
        quantidade = random.randint(1, 3)
        
        valor_unitario = produto['valor']
        valor_item = valor_unitario * Decimal(str(quantidade))
        valor_total += valor_item
        
        itens.append({
            'produto_id': produto_id,
            'nome_produto': produto['nome'],
            'quantidade': quantidade,
            'valor_unitario': valor_unitario,
            'valor_item': valor_item,
            'localizacao_produto': produto['localizacao']
        })
    
    # Gerar data do pedido (últimos 30 dias)
    data_pedido = random_date(datetime.now(), 30)
    
    # Gerar status aleatório
    status = random.choice(status_pedido)
    
    # Usar a localização do cliente para o endereço de entrega
    endereco_entrega = cliente['endereco']
    localizacao_entrega = cliente['localizacao']
    
    pedido = {
        'pedido_id': str(uuid.uuid4()),
        'cliente_id': cliente_id,
        'nome_cliente': cliente['nome'],
        'data_pedido': data_pedido,
        'status': status,
        'itens': itens,
        'valor_total': valor_total,
        'forma_pagamento': random.choice(['Cartão de Crédito', 'Boleto', 'Pix', 'Transferência']),
        'frete': Decimal(str(random.uniform(10, 50))),
        'prazo_entrega': random.randint(1, 15),
        'endereco_entrega': endereco_entrega,
        'localizacao_entrega': localizacao_entrega
    }
    
    pedidos.append(pedido)

print(f"Inserindo dados na tabela {table_name}...")
for pedido in pedidos:
    table.put_item(Item=pedido)
    print(f"Pedido inserido: {pedido['pedido_id']}")

# Armazenar IDs dos pedidos para uso posterior
pedido_ids = [pedido['pedido_id'] for pedido in pedidos]

# 4. Tabela de Veículos
table_name = "mcp-veiculos"
if not table_exists(table_name):
    print(f"Criando tabela {table_name}...")
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {'AttributeName': 'veiculo_id', 'KeyType': 'HASH'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'veiculo_id', 'AttributeType': 'S'}
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    wait_for_active_table(table_name)
else:
    table = dynamodb.Table(table_name)
    print(f"Tabela {table_name} já existe.")

# Inserir dados de veículos com localização
veiculos = [
    {
        'veiculo_id': str(uuid.uuid4()),
        'placa': 'ABC1D23',
        'modelo': 'Fiorino',
        'marca': 'Fiat',
        'ano': 2020,
        'tipo': 'Furgão',
        'capacidade_kg': Decimal('650'),
        'capacidade_m3': Decimal('3.1'),
        'status': 'Disponível',
        'km_atual': Decimal('45000'),
        'ultima_manutencao': (datetime.now() - timedelta(days=30)).isoformat(),
        'base_operacional': {
            'nome': 'CD São Bernardo do Campo',
            'cidade': 'São Bernardo do Campo',
            'estado': 'SP',
            'bairro': 'Demarchi',
            'cep': '09820-000',
            'coordenadas': {'latitude': Decimal(str(-23.7214)), 'longitude': Decimal(str(-46.5658))}
        },
        'localizacao_atual': {
            'cidade': 'São Bernardo do Campo',
            'estado': 'SP',
            'bairro': 'Centro',
            'cep': '09750-000',
            'coordenadas': {'latitude': Decimal(str(-23.6944)), 'longitude': Decimal(str(-46.5654))}
        },
        'destino_atual': None
    },
    {
        'veiculo_id': str(uuid.uuid4()),
        'placa': 'DEF4G56',
        'modelo': 'Sprinter',
        'marca': 'Mercedes-Benz',
        'ano': 2021,
        'tipo': 'Van',
        'capacidade_kg': Decimal('1500'),
        'capacidade_m3': Decimal('10.5'),
        'status': 'Em Rota',
        'km_atual': Decimal('32000'),
        'ultima_manutencao': (datetime.now() - timedelta(days=15)).isoformat(),
        'base_operacional': {
            'nome': 'CD São Bernardo do Campo',
            'cidade': 'São Bernardo do Campo',
            'estado': 'SP',
            'bairro': 'Demarchi',
            'cep': '09820-000',
            'coordenadas': {'latitude': Decimal(str(-23.7214)), 'longitude': Decimal(str(-46.5658))}
        },
        'localizacao_atual': {
            'cidade': 'São Paulo',
            'estado': 'SP',
            'bairro': 'Vila Olímpia',
            'cep': '04551-000',
            'coordenadas': {'latitude': Decimal(str(-23.5959)), 'longitude': Decimal(str(-46.6847))}
        },
        'destino_atual': {
            'cidade': 'São Paulo',
            'estado': 'SP',
            'bairro': 'Moema',
            'cep': '04077-000',
            'coordenadas': {'latitude': Decimal(str(-23.6008)), 'longitude': Decimal(str(-46.6680))}
        }
    },
    {
        'veiculo_id': str(uuid.uuid4()),
        'placa': 'GHI7J89',
        'modelo': 'HR',
        'marca': 'Hyundai',
        'ano': 2019,
        'tipo': 'Caminhão Leve',
        'capacidade_kg': Decimal('1200'),
        'capacidade_m3': Decimal('8.5'),
        'status': 'Disponível',
        'km_atual': Decimal('78000'),
        'ultima_manutencao': (datetime.now() - timedelta(days=7)).isoformat(),
        'base_operacional': {
            'nome': 'CD Guarulhos',
            'cidade': 'Guarulhos',
            'estado': 'SP',
            'bairro': 'Cumbica',
            'cep': '07180-000',
            'coordenadas': {'latitude': Decimal(str(-23.4356)), 'longitude': Decimal(str(-46.5358))}
        },
        'localizacao_atual': {
            'cidade': 'Guarulhos',
            'estado': 'SP',
            'bairro': 'Cumbica',
            'cep': '07180-000',
            'coordenadas': {'latitude': Decimal(str(-23.4356)), 'longitude': Decimal(str(-46.5358))}
        },
        'destino_atual': None
    },
    {
        'veiculo_id': str(uuid.uuid4()),
        'placa': 'JKL0M12',
        'modelo': 'Accelo 815',
        'marca': 'Mercedes-Benz',
        'ano': 2022,
        'tipo': 'Caminhão Médio',
        'capacidade_kg': Decimal('8000'),
        'capacidade_m3': Decimal('35'),
        'status': 'Em Manutenção',
        'km_atual': Decimal('25000'),
        'ultima_manutencao': datetime.now().isoformat(),
        'base_operacional': {
            'nome': 'CD Rio de Janeiro',
            'cidade': 'Rio de Janeiro',
            'estado': 'RJ',
            'bairro': 'Jacarepaguá',
            'cep': '22775-000',
            'coordenadas': {'latitude': Decimal(str(-22.9489)), 'longitude': Decimal(str(-43.3920))}
        },
        'localizacao_atual': {
            'cidade': 'Rio de Janeiro',
            'estado': 'RJ',
            'bairro': 'Jacarepaguá',
            'cep': '22775-000',
            'coordenadas': {'latitude': Decimal(str(-22.9489)), 'longitude': Decimal(str(-43.3920))}
        },
        'destino_atual': None
    },
    {
        'veiculo_id': str(uuid.uuid4()),
        'placa': 'NOP3Q45',
        'modelo': 'Atego 2430',
        'marca': 'Mercedes-Benz',
        'ano': 2020,
        'tipo': 'Caminhão Pesado',
        'capacidade_kg': Decimal('23000'),
        'capacidade_m3': Decimal('60'),
        'status': 'Disponível',
        'km_atual': Decimal('65000'),
        'ultima_manutencao': (datetime.now() - timedelta(days=45)).isoformat(),
        'base_operacional': {
            'nome': 'CD Belo Horizonte',
            'cidade': 'Contagem',
            'estado': 'MG',
            'bairro': 'Cinco',
            'cep': '32010-000',
            'coordenadas': {'latitude': Decimal(str(-19.9322)), 'longitude': Decimal(str(-44.0711))}
        },
        'localizacao_atual': {
            'cidade': 'Contagem',
            'estado': 'MG',
            'bairro': 'Cinco',
            'cep': '32010-000',
            'coordenadas': {'latitude': Decimal(str(-19.9322)), 'longitude': Decimal(str(-44.0711))}
        },
        'destino_atual': None
    },
    {
        'veiculo_id': str(uuid.uuid4()),
        'placa': 'RST6U78',
        'modelo': 'Delivery Express',
        'marca': 'Volkswagen',
        'ano': 2021,
        'tipo': 'Furgão',
        'capacidade_kg': Decimal('1000'),
        'capacidade_m3': Decimal('6.5'),
        'status': 'Em Rota',
        'km_atual': Decimal('28000'),
        'ultima_manutencao': (datetime.now() - timedelta(days=60)).isoformat(),
        'base_operacional': {
            'nome': 'CD Curitiba',
            'cidade': 'Curitiba',
            'estado': 'PR',
            'bairro': 'Cidade Industrial',
            'cep': '81450-000',
            'coordenadas': {'latitude': Decimal(str(-25.4950)), 'longitude': Decimal(str(-49.3549))}
        },
        'localizacao_atual': {
            'cidade': 'Curitiba',
            'estado': 'PR',
            'bairro': 'Batel',
            'cep': '80420-090',
            'coordenadas': {'latitude': Decimal(str(-25.4428)), 'longitude': Decimal(str(-49.2889))}
        },
        'destino_atual': {
            'cidade': 'Curitiba',
            'estado': 'PR',
            'bairro': 'Centro',
            'cep': '80020-310',
            'coordenadas': {'latitude': Decimal(str(-25.4290)), 'longitude': Decimal(str(-49.2671))}
        }
    },
    {
        'veiculo_id': str(uuid.uuid4()),
        'placa': 'VWX9Y01',
        'modelo': 'Master',
        'marca': 'Renault',
        'ano': 2022,
        'tipo': 'Van',
        'capacidade_kg': Decimal('1500'),
        'capacidade_m3': Decimal('13'),
        'status': 'Disponível',
        'km_atual': Decimal('18000'),
        'ultima_manutencao': (datetime.now() - timedelta(days=10)).isoformat(),
        'base_operacional': {
            'nome': 'CD São Bernardo do Campo',
            'cidade': 'São Bernardo do Campo',
            'estado': 'SP',
            'bairro': 'Demarchi',
            'cep': '09820-000',
            'coordenadas': {'latitude': Decimal(str(-23.7214)), 'longitude': Decimal(str(-46.5658))}
        },
        'localizacao_atual': {
            'cidade': 'São Bernardo do Campo',
            'estado': 'SP',
            'bairro': 'Rudge Ramos',
            'cep': '09636-000',
            'coordenadas': {'latitude': Decimal(str(-23.6667)), 'longitude': Decimal(str(-46.5667))}
        },
        'destino_atual': None
    },
    {
        'veiculo_id': str(uuid.uuid4()),
        'placa': 'ZAB2C34',
        'modelo': 'Daily 35S14',
        'marca': 'Iveco',
        'ano': 2020,
        'tipo': 'Furgão',
        'capacidade_kg': Decimal('1400'),
        'capacidade_m3': Decimal('12'),
        'status': 'Em Rota',
        'km_atual': Decimal('55000'),
        'ultima_manutencao': (datetime.now() - timedelta(days=20)).isoformat(),
        'base_operacional': {
            'nome': 'CD Rio de Janeiro',
            'cidade': 'Rio de Janeiro',
            'estado': 'RJ',
            'bairro': 'Jacarepaguá',
            'cep': '22775-000',
            'coordenadas': {'latitude': Decimal(str(-22.9489)), 'longitude': Decimal(str(-43.3920))}
        },
        'localizacao_atual': {
            'cidade': 'Rio de Janeiro',
            'estado': 'RJ',
            'bairro': 'Barra da Tijuca',
            'cep': '22640-100',
            'coordenadas': {'latitude': Decimal(str(-23.0000)), 'longitude': Decimal(str(-43.3650))}
        },
        'destino_atual': {
            'cidade': 'Rio de Janeiro',
            'estado': 'RJ',
            'bairro': 'Copacabana',
            'cep': '22070-000',
            'coordenadas': {'latitude': Decimal(str(-22.9711)), 'longitude': Decimal(str(-43.1863))}
        }
    },
    {
        'veiculo_id': str(uuid.uuid4()),
        'placa': 'DEF5G67',
        'modelo': 'Boxer',
        'marca': 'Peugeot',
        'ano': 2021,
        'tipo': 'Van',
        'capacidade_kg': Decimal('1300'),
        'capacidade_m3': Decimal('11.5'),
        'status': 'Disponível',
        'km_atual': Decimal('35000'),
        'ultima_manutencao': (datetime.now() - timedelta(days=25)).isoformat(),
        'base_operacional': {
            'nome': 'CD Guarulhos',
            'cidade': 'Guarulhos',
            'estado': 'SP',
            'bairro': 'Cumbica',
            'cep': '07180-000',
            'coordenadas': {'latitude': Decimal(str(-23.4356)), 'longitude': Decimal(str(-46.5358))}
        },
        'localizacao_atual': {
            'cidade': 'Guarulhos',
            'estado': 'SP',
            'bairro': 'Cumbica',
            'cep': '07180-000',
            'coordenadas': {'latitude': Decimal(str(-23.4356)), 'longitude': Decimal(str(-46.5358))}
        },
        'destino_atual': None
    },
    {
        'veiculo_id': str(uuid.uuid4()),
        'placa': 'HIJ8K90',
        'modelo': 'Cargo 816',
        'marca': 'Ford',
        'ano': 2019,
        'tipo': 'Caminhão Leve',
        'capacidade_kg': Decimal('5000'),
        'capacidade_m3': Decimal('30'),
        'status': 'Em Manutenção',
        'km_atual': Decimal('85000'),
        'ultima_manutencao': datetime.now().isoformat(),
        'base_operacional': {
            'nome': 'CD São Bernardo do Campo',
            'cidade': 'São Bernardo do Campo',
            'estado': 'SP',
            'bairro': 'Demarchi',
            'cep': '09820-000',
            'coordenadas': {'latitude': Decimal(str(-23.7214)), 'longitude': Decimal(str(-46.5658))}
        },
        'localizacao_atual': {
            'cidade': 'Santo André',
            'estado': 'SP',
            'bairro': 'Centro',
            'cep': '09010-000',
            'coordenadas': {'latitude': Decimal(str(-23.6639)), 'longitude': Decimal(str(-46.5383))}
        },
        'destino_atual': None
    }
]

print(f"Inserindo dados na tabela {table_name}...")
for veiculo in veiculos:
    table.put_item(Item=veiculo)
    print(f"Veículo inserido: {veiculo['modelo']} - {veiculo['placa']}")

# Armazenar IDs dos veículos para uso posterior
veiculo_ids = [veiculo['veiculo_id'] for veiculo in veiculos]

# 5. Tabela de Entregas
table_name = "mcp-entregas"
if not table_exists(table_name):
    print(f"Criando tabela {table_name}...")
    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=[
            {'AttributeName': 'entrega_id', 'KeyType': 'HASH'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'entrega_id', 'AttributeType': 'S'},
            {'AttributeName': 'pedido_id', 'AttributeType': 'S'}
        ],
        GlobalSecondaryIndexes=[
            {
                'IndexName': 'pedido_id-index',
                'KeySchema': [
                    {'AttributeName': 'pedido_id', 'KeyType': 'HASH'}
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                }
            }
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    wait_for_active_table(table_name)
else:
    table = dynamodb.Table(table_name)
    print(f"Tabela {table_name} já existe.")

# Status possíveis para uma entrega
status_entrega = ['Aguardando Coleta', 'Em Trânsito', 'Em Distribuição', 'Entregue', 'Tentativa Falha', 'Devolvido']

# Inserir dados de entregas com localização
entregas = []
for pedido_id in pedido_ids:
    # Encontrar o pedido correspondente
    pedido = next((p for p in pedidos if p['pedido_id'] == pedido_id), None)
    
    # Só criar entrega para pedidos com status adequado
    if pedido and pedido['status'] in ['Pagamento Confirmado', 'Em Separação', 'Em Transporte', 'Entregue']:
        # Selecionar veículo aleatório
        veiculo_idx = random.randint(0, len(veiculos) - 1)
        veiculo_id = veiculo_ids[veiculo_idx]
        veiculo = veiculos[veiculo_idx]
        
        # Obter localização de origem (centro de distribuição)
        origem = veiculo['base_operacional']
        
        # Obter localização de destino (endereço do cliente)
        destino = pedido['localizacao_entrega']
        
        # Gerar data de previsão (1 a 15 dias após o pedido)
        data_pedido = datetime.fromisoformat(pedido['data_pedido'])
        data_prevista = (data_pedido + timedelta(days=random.randint(1, 15))).isoformat()
        
        # Gerar status aleatório
        status = random.choice(status_entrega)
        
        # Se status for "Entregue", gerar data de entrega
        data_entrega = None
        if status == 'Entregue':
            data_entrega = (datetime.fromisoformat(data_prevista) - timedelta(days=random.randint(0, 2))).isoformat()
        
        # Gerar histórico de rastreamento com localizações
        historico = []
        
        # Adicionar evento de coleta
        data_coleta = (data_pedido + timedelta(days=1)).isoformat()
        historico.append({
            'data': data_coleta,
            'status': 'Coletado',
            'local': origem['nome'],
            'localizacao': {
                'cidade': origem['cidade'],
                'estado': origem['estado'],
                'bairro': origem['bairro'],
                'cep': origem['cep'],
                'coordenadas': origem['coordenadas']
            },
            'observacao': 'Pacote coletado para envio'
        })
        
        # Adicionar eventos intermediários
        if status in ['Em Trânsito', 'Em Distribuição', 'Entregue', 'Tentativa Falha', 'Devolvido']:
            data_transito = (datetime.fromisoformat(data_coleta) + timedelta(days=1)).isoformat()
            
            # Escolher um ponto intermediário aleatório
            ponto_intermediario = random.choice(localizacoes_brasil)
            
            historico.append({
                'data': data_transito,
                'status': 'Em Trânsito',
                'local': f"Centro Logístico {ponto_intermediario['cidade']}",
                'localizacao': {
                    'cidade': ponto_intermediario['cidade'],
                    'estado': ponto_intermediario['estado'],
                    'bairro': ponto_intermediario['bairro'],
                    'cep': ponto_intermediario['cep'],
                    'coordenadas': ponto_intermediario['coordenadas']
                },
                'observacao': 'Pacote em trânsito para o destino'
            })
        
        if status in ['Em Distribuição', 'Entregue', 'Tentativa Falha', 'Devolvido']:
            data_distribuicao = (datetime.fromisoformat(data_transito) + timedelta(days=1)).isoformat()
            
            # Usar um ponto próximo ao destino
            ponto_distribuicao = {
                'cidade': destino['cidade'],
                'estado': destino['estado'],
                'bairro': 'Centro de Distribuição Local',
                'cep': destino['cep'],
                'coordenadas': {
                    'latitude': Decimal(str(float(destino['coordenadas']['latitude']) + 0.01)),
                    'longitude': Decimal(str(float(destino['coordenadas']['longitude']) + 0.01))
                }
            }
            
            historico.append({
                'data': data_distribuicao,
                'status': 'Em Distribuição',
                'local': f"Unidade Local {destino['cidade']}",
                'localizacao': ponto_distribuicao,
                'observacao': 'Pacote saiu para entrega'
            })
        
        if status == 'Entregue':
            historico.append({
                'data': data_entrega,
                'status': 'Entregue',
                'local': f"Endereço do Destinatário - {destino['cidade']}, {destino['bairro']}",
                'localizacao': destino,
                'observacao': 'Pacote entregue ao destinatário'
            })
        elif status == 'Tentativa Falha':
            data_tentativa = (datetime.fromisoformat(data_distribuicao) + timedelta(hours=random.randint(1, 8))).isoformat()
            historico.append({
                'data': data_tentativa,
                'status': 'Tentativa Falha',
                'local': f"Endereço do Destinatário - {destino['cidade']}, {destino['bairro']}",
                'localizacao': destino,
                'observacao': 'Destinatário ausente'
            })
        elif status == 'Devolvido':
            data_devolucao = (datetime.fromisoformat(data_distribuicao) + timedelta(days=3)).isoformat()
            historico.append({
                'data': data_devolucao,
                'status': 'Devolvido',
                'local': origem['nome'],
                'localizacao': {
                    'cidade': origem['cidade'],
                    'estado': origem['estado'],
                    'bairro': origem['bairro'],
                    'cep': origem['cep'],
                    'coordenadas': origem['coordenadas']
                },
                'observacao': 'Pacote devolvido após 3 tentativas sem sucesso'
            })
        
        entrega = {
            'entrega_id': str(uuid.uuid4()),
            'pedido_id': pedido_id,
            'veiculo_id': veiculo_id,
            'data_prevista': data_prevista,
            'data_entrega': data_entrega,
            'status': status,
            'codigo_rastreio': f"MCT{random.randint(10000000, 99999999)}BR",
            'entregador': random.choice(['Carlos Motorista', 'João Entregador', 'Pedro Motorista', 'Marcos Entregador', 'Paulo Motorista']),
            'origem': origem,
            'destino': destino,
            'historico': historico,
            'localizacao_atual': historico[-1]['localizacao'],
            'observacoes': random.choice(['', 'Cliente solicitou entrega no período da tarde', 'Entregar na portaria', ''])
        }
        
        entregas.append(entrega)

print(f"Inserindo dados na tabela {table_name}...")
for entrega in entregas:
    table.put_item(Item=entrega)
    print(f"Entrega inserida: {entrega['codigo_rastreio']}")

print("\nCriação de tabelas e inserção de dados concluída com sucesso!")
print(f"Foram criadas 5 tabelas com prefixo 'mcp-' e inseridos dados fictícios em cada uma.")
print("Resumo:")
print(f"- mcp-produtos: {len(produtos)} produtos")
print(f"- mcp-clientes: {len(clientes)} clientes")
print(f"- mcp-pedidos: {len(pedidos)} pedidos")
print(f"- mcp-veiculos: {len(veiculos)} veículos")
print(f"- mcp-entregas: {len(entregas)} entregas")
