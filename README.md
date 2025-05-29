# Projeto de Banco Relacional - MC536

## Visão Geral
Alunos participantes:
- Felipe Rocha Verol (RA: 248552)
- Lucas Bellusci D'Andréa (RA: 206009)
- Theo Maceres Silva (RA: 220825)

Esse é um repositório para o primeiro projeto da disciplina MC536, que envolve projetar um banco de dados relacional, populá-lo e fazer 5 consultas não triviais, usando PostgreSQL.
Para essa tarefa, foi criado um banco de dados que busca relacionar índices escolares com o mercado de trabalho de uma região e fazer comparações de outras escolas com o Instituo Federal de Brasília.

## Datasets
Foram usados os seguintes datasets para o projeto:
- [MICRODADOS_ENEM_ESCOLA.csv](dados/brutos/microdados_enem_por_escola.zip): relaciona escolas, de diferentes locais, com seus índices escolares e desempenho no ENEM em cada participação.
- [mundo_trabalho.csv](dados/brutos/mundo_trabalho_csv.zip): contém dados de alunos e ex-alunos do Instituto Federal de Brasília sobre suas matrículas em diferentes cursos e sobre seus vínculos empregatícios. <br/> <br/>

OBS: os datasets em formato CSV estão compactados dentro de arquivos ZIP

## Modelos Conceitual, Relacional e Físico
Aqui estão os modelos criados para o melhor entendimento de como deve ser o funcionamento do banco de dados:

### Modelo Conceitual
![Modelo Conceitual do Banco de Dados](modelos/modelo_conceitual.png) 

### Modelo Relacional
![Modelo Relacional do Banco de Dados](modelos/modelo_relacional.png)

### Modelo Físico
O SQL do modelo físico, gerado pela ferramenta pgAdmin 4, está no reposítorio, podendo ser encontrado no diretório [modelos/modelo_fisico.sql](modelos/modelo_fisico.sql).

## Explicação das principais tabelas
- aluno_ifb: contém informações sobre um estudante do IFB, com primary key sendo id_aluno
- matricula_ifb: contém informção sobre as matrículas de um aluno, como o curso que está sendo cursado pelo aluno
- vinculo_empregaticio: contém informações sobre o emprego que um aluno do IFB tem ou teve, como a remuneração e o local do trabalho

- escola: contém informações sobre a escola, como sua localização
- indicadores_escolares: contém informações mais específicas sobre como é a escola, como taxa de abandono e taxa de reprovação
- desempenho_enem: contém informações sobre o desempenho no ENEM de uma escola em determinado ano
- participacao_enem: contém informações sobre a participação de uma escola em determinado ano, como número de participantes

## Como testar

### 1. Criar o banco
Para criar o banco, é necessário ter o PostgreSQL e o pgAdmin 4 instalados na máquina. Depois, é preciso criar um novo banco de dados no pgAdmin 4 e copiar e colar o SQL do arquivo [modelos/modelo_fisico.sql](modelos/modelo_fisico.sql), utilizando a ferramenta Query Tool, do pgAdmin 4, que pode ser acessada clicando com o botão direito no nome do banco de dados no menu lateral da esquerda.

### 2. Popular o banco
Agora, para popular o banco, é necessário executar os scripts da pasta [/scripts_insercao](scripts_insercao). Perceba que os scripts Python estão ordenados, de maneira que é necessário executá-los em ordem crescente (de 01 até 08). Lembre-se de colocar as credenciais corretas para o seu banco nos scripts que precisam de uma conexão, que é feita através da biblioteca psycopg2.

### 3. Executar as consultas
Para executar as queries, basta copiar e colar o SQL dos arquivos da pasta [/queries](queries), utilizando a ferramenta Query Tool, do pgAdmin 4, da mesma forma que foi usada no item 1.

## Consultas
### - [Query 1](queries/query1.sql): Desempenho Escolar e Remuneração de Egressos do IFB
Objetivo: Avaliar a correlação entre o desempenho no ENEM dos alunos do Instituto Federal de Brasília (IFB) e os valores de remuneração média e salário contratual obtidos por eles ao ingressarem no mercado de trabalho, considerando o ano de admissão como referência temporal comum.

### - [Query 2](queries/query2.sql): Remuneração e Desempenho no ENEM por Localidade e Ano
Objetivo: Analisar a relação entre a remuneração média de egressos no mercado de trabalho formal e o desempenho médio dos estudantes no ENEM, segmentado por município e ano.

### - [Query 3](queries/query3.sql): Qualidade Educacional e Condições de Trabalho por Localidade
Objetivo: Investigar a relação entre o desempenho médio no ENEM dos municípios e as condições contratuais dos vínculos empregatícios ativos nesses locais, considerando remuneração média, carga horária e número de escolas participantes.

### - [Query 4](queries/query4.sql): Análise de Relação entre Abandono Escolar e Tempo de Emprego
Objetivo: Investigar se existe relação entre a taxa de abandono escolar no IFB e o tempo de permanência dos alunos no primeiro emprego formal, considerando os ingressantes entre 2009 e 2015. A análise classifica os níveis de abandono tanto no ambiente escolar quanto no empregatício, permitindo identificar possíveis padrões de evasão e sua influência na trajetória profissional dos egressos.
 
### - [Query 5](queries/query5.sql): Mobilidade Geográfica e Remuneração de Egressos
Objetivo: Analisar os alunos do IFB que estudaram em cursos presenciais e que, após formados, conseguiram empregos com remuneração anual igual ou superior a R$1000 fora do município de sua escola. O foco é identificar padrões de migração geográfica para o trabalho, relacionando a última remuneração conhecida com o curso frequentado e o deslocamento entre município de estudo e município de emprego.








