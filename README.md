# Azure SQL → Snowflake | Engenharia de Dados

Pipeline de engenharia de dados ponta a ponta desenvolvido para demonstrar competências em ingestão de dados com Python, arquitetura Medallion, transformação com dbt, orquestração com Apache Airflow e testes de qualidade.

O projeto utiliza um banco Azure SQL como fonte de dados, Snowflake como plataforma analítica e dbt para transformar os dados em camadas reutilizáveis para análise.

## Objetivos do projeto

- Implementar um pipeline de dados automatizado de ponta a ponta.
- Demonstrar ingestão inicial e incremental de dados.
- Aplicar a arquitetura Medallion: Bronze, Silver e Gold.
- Utilizar Python para extração e carga de dados.
- Utilizar Snowflake como plataforma de armazenamento e processamento analítico.
- Desenvolver transformações SQL versionadas com dbt.
- Implementar testes de qualidade e validações de dados.
- Orquestrar as etapas do pipeline com Apache Airflow.
- Executar e validar a solução em ambiente local com Docker.
- Aplicar boas práticas de organização, versionamento e segurança de credenciais.

## Arquitetura

```text
                   ┌──────────────────────┐
                   │      Azure SQL       │
                   │  Banco de origem     │
                   └──────────┬───────────┘
                              │
                              ▼
                   ┌──────────────────────┐
                   │ Python               │
                   │ Extração e ingestão  │
                   └──────────┬───────────┘
                              │
                              ▼
                   ┌──────────────────────┐
                   │ Snowflake — Bronze   │
                   │ Dados ingeridos      │
                   └──────────┬───────────┘
                              │
                              ▼
                   ┌──────────────────────┐
                   │ dbt — Silver         │
                   │ Limpeza e regras     │
                   │ de transformação     │
                   └──────────┬───────────┘
                              │
                              ▼
                   ┌──────────────────────┐
                   │ dbt — Gold           │
                   │ Dados analíticos     │
                   │ e modelos de negócio │
                   └──────────┬───────────┘
                              │
                              ▼
                   ┌──────────────────────┐
                   │ Testes de qualidade  │
                   │ e validações dbt     │
                   └──────────────────────┘

       Apache Airflow: orquestra as etapas do pipeline
       Docker: ambiente local de execução
       Git/GitHub: versionamento do projeto
```

## Tecnologias utilizadas

| Tecnologia | Finalidade |
|---|---|
| Azure SQL Database | Banco de dados de origem |
| Python | Extração, ingestão e controle do processamento |
| pyodbc | Conexão com SQL Server/Azure SQL |
| Snowflake | Armazenamento e processamento analítico |
| SQL | Consultas e transformações de dados |
| dbt | Modelagem, transformação e testes de dados |
| Apache Airflow | Agendamento e orquestração do pipeline |
| Docker | Containerização do ambiente de execução |
| PostgreSQL | Banco de metadados do Airflow |
| Git | Controle de versão |
| GitHub | Hospedagem do código-fonte e portfólio |

## Arquitetura Medallion

### Bronze — Dados de origem

Camada responsável por armazenar os dados ingeridos do Azure SQL no Snowflake.

Principais características:

- Extração das tabelas de origem.
- Registro da tabela de origem e da chave do registro.
- Identificação de versões dos registros por meio de hash.
- Preservação do histórico de versões dos dados.
- Controle de ingestão por tabela.
- Suporte à carga inicial e à ingestão incremental, conforme a configuração de cada tabela.

O mecanismo de carga utiliza a combinação de tabela de origem, chave do registro e hash da linha para evitar a duplicação de versões idênticas, mantendo versões diferentes quando os dados mudam.

### Silver — Dados tratados

Camada de transformação implementada com dbt.

Responsabilidades:

- Padronização dos dados.
- Aplicação de regras de transformação.
- Tratamento de dados para consumo analítico.
- Organização de modelos reutilizáveis.
- Aplicação de testes e validações.

### Gold — Dados analíticos

Camada destinada a disponibilizar dados estruturados para análises e necessidades de negócio.

Responsabilidades:

- Consolidar dados transformados.
- Organizar modelos analíticos.
- Facilitar consultas e análises.
- Disponibilizar estruturas reutilizáveis para consumidores de dados.

## Estratégia de ingestão

O pipeline contempla dois tipos de carga:

### Carga completa

Utilizada para tabelas configuradas para extração integral, especialmente quando não há uma coluna adequada para identificar alterações.

### Carga incremental

Utiliza colunas de data, como `created_at` e `updated_at`, conforme a configuração da tabela, para identificar registros a serem extraídos.

O objetivo é reduzir a necessidade de reprocessar integralmente tabelas que permitem identificar alterações.

A implementação considera as características de cada tabela de origem. A estratégia incremental não depende de CDC (Change Data Capture).

**Observação:** a ingestão incremental depende da disponibilidade e da confiabilidade das colunas de controle na origem. Alterações retroativas, exclusões físicas e registros sem atualização adequada precisam ser considerados ao evoluir a solução para produção.

## Orquestração com Apache Airflow

O Airflow coordena a execução do pipeline por meio da DAG:

`azure_sql_snowflake_medallion`

Fluxo de execução:

1. Ingestão dos dados do Azure SQL para a camada Bronze.
2. Validação da conexão e da configuração do dbt.
3. Construção dos modelos Silver.
4. Construção dos modelos Gold.
5. Execução dos testes de qualidade.

O Airflow permite acompanhar o estado das execuções, consultar logs, identificar falhas e executar novamente as etapas necessárias.

## Qualidade de dados

Os testes são implementados por meio do dbt e executados como parte do fluxo orquestrado.

Eles ajudam a verificar regras de integridade e consistência dos modelos, conforme as definições existentes no projeto.

Na execução validada durante o desenvolvimento, os resultados foram:

| Etapa | Resultado observado |
|---|---|
| Construção Silver | 22 modelos aprovados |
| Construção Gold | 10 modelos aprovados |
| Testes dbt | 113 aprovados, 1 aviso e 0 erros |
| DAG do Airflow | Execução concluída com sucesso |

Esses números representam uma execução específica do projeto e podem mudar conforme os modelos e testes forem modificados.

O aviso apresentado pelos testes deve ser analisado para determinar se representa uma divergência esperada ou um problema de qualidade. Uma execução bem-sucedida, isoladamente, não garante que todos os dados estejam corretos.

## Ambiente de execução

O projeto foi desenvolvido e validado em ambiente local, utilizando Windows, Docker e containers para os componentes de orquestração e processamento.

O ambiente inclui:

- Apache Airflow.
- PostgreSQL para os metadados do Airflow.
- Python para a ingestão.
- dbt para as transformações.
- Conexões com Azure SQL e Snowflake.

A execução depende da configuração correta das conexões e das credenciais dos serviços externos.

## Pré-requisitos

Antes de executar o projeto, tenha instalado e configurado:

- Git.
- Docker Desktop com suporte a containers Linux.
- Python compatível com o ambiente configurado no projeto.
- Acesso a uma instância Azure SQL Database.
- Acesso a uma conta Snowflake.
- Driver ODBC compatível com SQL Server, conforme a configuração de ingestão.

Os serviços Azure SQL e Snowflake precisam estar acessíveis a partir do ambiente de execução.

## Configuração

### 1. Clonar o repositório

```powershell
git clone <URL_DO_REPOSITORIO>
cd PROJETO_AZSQL_PYTHON_DBT_AIRFLOW_SNOWFLAKE
```

Substitua `<URL_DO_REPOSITORIO>` pela URL real do repositório no GitHub.

### 2. Configurar as credenciais

Configure as variáveis de ambiente e os arquivos de configuração exigidos pelo projeto para conectar ao Azure SQL e ao Snowflake.

Consulte os arquivos de configuração e o código de ingestão para identificar os nomes exatos das variáveis utilizadas.

Não inclua senhas, tokens, chaves privadas ou arquivos locais de credenciais no GitHub.

### 3. Validar o ambiente Docker

Com o Docker Desktop em execução, acesse a pasta que contém o arquivo Docker Compose e inicialize os serviços conforme a configuração do projeto.

Exemplo, caso o arquivo esteja na raiz do repositório:

```powershell
docker compose up -d --build
```

Se o arquivo estiver em outra pasta, informe o caminho correto usando a opção `-f`.

### 4. Validar o Airflow

Acesse a interface web do Airflow utilizando o endereço e as credenciais configurados no ambiente local.

Verifique se a DAG `azure_sql_snowflake_medallion` está disponível e se não há erros de importação.

### 5. Executar o pipeline

Acione a DAG pela interface do Airflow ou utilize o mecanismo de agendamento configurado.

Acompanhe os logs das tarefas de ingestão, transformação e testes até a conclusão da execução.

## Segurança

O projeto deve manter as credenciais fora do controle de versão.

Boas práticas adotadas ou previstas para a evolução do projeto:

- Não versionar arquivos `.env` com credenciais.
- Não inserir senhas diretamente no código-fonte.
- Utilizar variáveis de ambiente ou mecanismos apropriados de gerenciamento de segredos.
- Restringir o acesso às credenciais e aos serviços externos.
- Utilizar identidades e permissões adequadas ao ambiente de execução.

Para uma implantação em nuvem, o gerenciamento de segredos pode ser integrado a um serviço como o Azure Key Vault, acompanhado de uma estratégia de autenticação e autorização apropriada.

## Organização do projeto

A estrutura deve ser consultada no repositório para identificar os caminhos exatos dos componentes.

Os principais componentes lógicos são:

- **Ingestão:** código Python responsável pela extração e carga.
- **dbt:** projeto de transformação, modelos e testes.
- **Airflow:** DAG e configuração de orquestração.
- **Docker:** arquivos de containerização e configuração do ambiente.
- **Configuração:** parâmetros de conexão e execução, sem credenciais versionadas.

## Possíveis evoluções

A arquitetura permite evoluções futuras, como:

- Implantação dos componentes em infraestrutura de nuvem.
- Integração efetiva com um serviço de gerenciamento de segredos.
- Implementação de CI/CD para validações automatizadas.
- Monitoramento de duração, volume e falhas de ingestão.
- Melhoria das métricas de registros inseridos, atualizados e rejeitados.
- Expansão das regras de qualidade e reconciliação entre camadas.
- Tratamento explícito de exclusões e alterações retroativas na origem.
- Documentação adicional dos modelos e das regras de negócio.

Esses itens são possibilidades de evolução, não funcionalidades que este README declara como já implementadas.

## Objetivo profissional

Este projeto faz parte do meu portfólio de Engenharia de Dados e demonstra a aplicação prática de conceitos de ingestão, modelagem analítica, processamento incremental, qualidade de dados, automação e orquestração.

A proposta é simular um fluxo de dados próximo de um cenário corporativo, utilizando tecnologias presentes em arquiteturas modernas de dados.

## Autor

Projeto desenvolvido para estudo, prática e demonstração de competências em Engenharia de Dados.

GitHub: https://github.com/paulormf
