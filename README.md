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
- [MICRODADOS_ENEM_ESCOLA.csv](dados/brutos/microdados_enem_por_escola.zip): relacions escolas, de diferentes locais, com seus índices escolares e desempenho no ENEM em cada participação.
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
 
## Consultas
- [query 1](queries/query1.sql):
- [query 2](queries/query2.sql):
- [query 3](queries/query3.sql):
- [query 4](queries/query4.sql):

Análise de Relação entre Abandono Escolar e Tempo de Emprego

Objetivo: Investigar se existe correlação entre a taxa de abandono escolar no IFB e a duração dos vínculos empregatícios dos egressos.

Mecanismo de Funcionamento:

1. Filtragem Inicial: Seleciona alunos que ingressaram entre 2009-2015 (alunos_ano_de_entrada).

2. Consolidação de Matrículas: Mantém apenas a última matrícula de cada aluno (formatando_uma_matricula).

3. Cruzamento com Dados Empregatícios: Relaciona com vínculos de trabalho, excluindo casos sem desligamento (comparando_com_ano_empregado).

4. Indicadores Educacionais: Adiciona a taxa de abandono da escola específica (código 53006178) no ano de ingresso (puxando_taxa_abandono_escola).

5. Classificação: Categoriza tanto o tempo de emprego quanto a taxa de abandono em níveis BAIXA, MÉDIA e ALTA (indicadores_abandono).

Métricas-Chave:

- tempo_emprego: Duração em meses do vínculo empregatício
- taxa_abandono_ifb: Percentual de abandono no ano de ingresso
- Classificações comparativas entre abandono escolar e empregatício

Aplicações Práticas:

1. Identificar se alunos de períodos com maior evasão escolar tendem a ter menor estabilidade no mercado de trabalho
2. Subsidiar políticas de retenção estudantil com base no impacto futuro na empregabilidade
3. Comparar a eficácia de diferentes estratégias pedagógicas ao longo dos anos
 
- [query 5](queries/query5.sql):
  
Mobilidade Geográfica e Remuneração de Egressos

Objetivo: Analisar o perfil de remuneração e mobilidade geográfica de alunos formados em cursos presenciais.

Mecanismo de Funcionamento:

1. Seleção Inicial: Identifica alunos de cursos presenciais com sua última matrícula (matriculados_a_distancia).

2. Vínculos Empregatícios: Relaciona com a última remuneração registrada (relacionando_com_idremuneracao).

3. Filtragem Qualitativa: Considera apenas remunerações acima de R$1.000 (matriculados_a_distancia_remuneracao).

4. Análise Geográfica: Compara município da escola, por meio da tabela Escola, com município do emprego (quantidade_alunos_que_saem_da_cidade).

5. Consolidação: Mantém apenas o registro mais recente para cada aluno (tabela_formatada).

Métricas-Chave:

- vl_ultima_remuneracao_ano: Remuneração anual no último emprego registrado
- municipio_escola vs municipio_empregado: Mobilidade geográfica
- no_curso: Área de formação do egresso

Aplicações Práticas:

1. Identificar cursos com maior índice de migração para outras cidades
2. Analisar a relação entre mobilidade geográfica e ganhos salariais
3. Subsidiar políticas de atração de empregos locais para egressos
4. Orientar alunos sobre oportunidades em diferentes regiões
