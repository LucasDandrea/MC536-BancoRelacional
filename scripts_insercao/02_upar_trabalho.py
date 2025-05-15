from getpass import getpass
from datetime import datetime
import csv
import io
import zipfile
import psycopg2


# Configurações de conexão
dbname = ""
user = ""
host = ""
password = ""


def clean_text(value):
    """Corrige problemas de codificação em textos"""
    if not value:
        return None
    try:
        return value.encode('latin1').decode('utf-8', errors='replace').strip()
    except:
        return value.strip() if value else None


def transfer_mundo_trabalho_data(cursor):
    """Transfere dados da tabela temporária para as tabelas permanentes"""
    print("\nIniciando transferência para tabelas permanentes...")

    try:
        CODIGO_IFB = '53006178'

        # 1. Transferir cursos
        cursor.execute("""
            INSERT INTO public.curso_ifb (
                co_curso, no_curso, co_tipo_curso, tipo_curso,
                co_tipo_nivel, ds_tipo_nivel, ds_eixo_tecnologico,
                modalidade_ensino, carga_horaria
            )
            SELECT DISTINCT ON (co_curso)
                co_curso, 
                no_curso,
                co_tipo_curso, 
                tipo_curso,
                co_tipo_nivel, 
                ds_tipo_nivel,
                ds_eixo_tecnologico,
                modalidade_ensino,
            CASE WHEN carga_horaria ~ '^\d+$' THEN carga_horaria::integer ELSE NULL END
            FROM temp_mundo_trabalho
            WHERE co_curso IS NOT NULL AND co_curso != ''
            ON CONFLICT (co_curso) DO NOTHING
        """)
        print(f"Cursos transferidos: {cursor.rowcount} registros")

        # 2. Transferir alunos
        cursor.execute("""
            INSERT INTO public.aluno_ifb (
                id_aluno, data_nascimento, idade, sexo,
                nacionalidade, raca, portador_deficiencia
            )
            SELECT DISTINCT ON (id_aluno)
                id_aluno, 
                CASE 
                    WHEN data_nascimento ~ '^\d{8}$' THEN to_date(data_nascimento, 'DDMMYYYY') 
                    ELSE NULL 
                END,
                CASE 
                    WHEN idade ~ '^\d+$' THEN idade::integer 
                    ELSE NULL 
                END,
                CASE 
                    WHEN upper(sexo) LIKE 'M%' THEN 'M' 
                    ELSE 'F' 
                END,
                nacionalidade,
                raca,
                CASE 
                    WHEN upper(portador_deficiencia) IN ('S', 'SIM', 'Y', 'YES', '1', 'TRUE') THEN TRUE
                    ELSE FALSE
                END
            FROM temp_mundo_trabalho
            WHERE id_aluno IS NOT NULL
            ON CONFLICT (id_aluno) DO NOTHING
        """)
        print(f"Alunos transferidos: {cursor.rowcount} registros")

        # 5. Transferir matrículas
        cursor.execute(f"""
            INSERT INTO public.matricula_ifb (
                co_ciclo_matricula, periodo, situacao_matricula,
                tipo_cota, atestado_baixarenda, ano, id_aluno,
                co_curso, unidade_ensino, co_escola_educacenso
            )
            SELECT
                m.co_ciclo_matricula, 
                m.periodo,
                m.situacao_matricula,
                m.tipo_cota,
                CASE 
                    WHEN upper(m.atestado_baixarenda) IN ('S', 'SIM', 'Y', 'YES', '1', 'TRUE') THEN TRUE
                    ELSE FALSE
                END,
                CASE WHEN m.ano ~ '^\d+$' THEN m.ano::integer ELSE NULL END,
                m.id_aluno,  -- Corrigido: usar id_aluno direto da temp table
                m.co_curso, 
                m.unidade_ensino,
                {CODIGO_IFB}
            FROM temp_mundo_trabalho m
            WHERE 
                m.co_ciclo_matricula IS NOT NULL AND 
                m.co_ciclo_matricula != ''
        """)
        print(f"Matrículas transferidas: {cursor.rowcount} registros")

        return True

    except Exception as e:
        print(f"Erro durante transferência: {e}")
        raise


# Conectar ao banco de dados
try:
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        host=host,
        password=password
    )
    cursor = conn.cursor()
    print("Conexão estabelecida com sucesso!")

    # Processar arquivo mundo_trabalho
    print("\nProcessando mundo_trabalho_csv.zip...")
    with zipfile.ZipFile('../dados/tratados/arquivo_limpo_csv.zip') as z:
        with z.open('arquivo_limpo.csv') as f:
            csv_file = io.TextIOWrapper(f, encoding='latin1')
            reader = csv.DictReader(csv_file, delimiter=';')

            # Criar tabela temporária
            cursor.execute("""
                CREATE TEMPORARY TABLE temp_mundo_trabalho (
                    id_aluno integer,  
                    co_ciclo_matricula varchar(50),
                    unidade_ensino varchar(100),
                    periodo varchar(30),
                    ds_eixo_tecnologico varchar(200),
                    co_curso varchar(50),
                    no_curso varchar(200),
                    co_tipo_curso varchar(10),
                    tipo_curso varchar(100),
                    co_tipo_nivel varchar(10),
                    ds_tipo_nivel varchar(100),
                    carga_horaria varchar(10),
                    situacao_matricula varchar(50),
                    tipo_cota varchar(50),
                    atestado_baixarenda varchar(10),
                    ano varchar(10),
                    modalidade_ensino varchar(100),
                    data_nascimento varchar(8),
                    idade varchar(10),
                    sexo varchar(20),
                    nacionalidade varchar(50),
                    raca varchar(50),
                    portador_deficiencia varchar(10),
                    data_admissao_declarada varchar(8),
                    vl_remun_media_nom varchar(20),
                    vl_remun_media_sm varchar(20),
                    vl_remun_dezembro_nom varchar(20),
                    vl_remun_dezembro_sm varchar(20),
                    tempo_emprego varchar(10),
                    qtd_hora_contr varchar(10),
                    vl_ultima_remuneracao_ano varchar(20),
                    vl_salario_contratual varchar(20),
                    razao_social varchar(200),
                    municipio_vinculo varchar(100),
                    motivo_desligamento varchar(200),
                    mes_desligamento varchar(20),
                    tipo_vinculo_empregaticio varchar(100),
                    indicador_vinculo_ativo varchar(10),
                    ano_rais varchar(10)
                    )
            """)

            # Preparar query de inserção
            insert_query = """
                INSERT INTO temp_mundo_trabalho VALUES (
                    %(Aluno)s, %(co_ciclo_matricula)s, %(unidade_ensino)s,
                    %(periodo)s, %(ds_eixo_tecnologico)s, %(co_curso)s,
                    %(no_curso)s, %(co_tipo_curso)s, %(tipo_curso)s,
                    %(co_tipo_nivel)s, %(ds_tipo_nivel)s, %(carga_horaria)s,
                    %(situacao_matricula)s, %(tipo_cota)s, %(atestado_baixarenda)s,
                    %(ano)s, %(modalidade_ensino)s, %(Data de Nascimento)s,
                    %(Idade)s, %(Sexo)s, %(Nacionalidade)s, %(Raça)s,
                    %(Portador de Deficiência)s, %(Data Admissão Declarada)s,
                    %(Vl Remun Média Nom)s, %(Vl Remun Média SM)s,
                    %(Vl Remun Dezembro Nom)s, %(Vl Remun Dezembro SM)s,
                    %(Tempo Emprego)s, %(Qtd Hora Contr)s,
                    %(Vl Última Remuneração Ano)s, %(Vl Salário Contratual)s,
                    %(Razão Social)s, %(Município)s, %(Motivo desligamento)s,
                    %(Mês Desligamento)s, %(Tipo de vínculo empregatício)s,
                    %(Indicador de vínculo ativo em 31_12)s, %(Ano_RAIS)s
                )
            """

            # Inserir linhas em lotes
            batch_size = 1000
            batch = []

            for row in reader:
                try:
                    # Limpar e preparar os dados
                    cleaned_row = {
                        'Aluno': clean_text(row.get('Aluno')),
                        'co_ciclo_matricula': clean_text(row.get('co_ciclo_matricula')),
                        'unidade_ensino': clean_text(row.get('unidade_ensino')),
                        'periodo': clean_text(row.get('periodo')),
                        'ds_eixo_tecnologico': clean_text(row.get('ds_eixo_tecnologico')),
                        'co_curso': clean_text(row.get('co_curso')),
                        'no_curso': clean_text(row.get('no_curso')),
                        'co_tipo_curso': clean_text(row.get('co_tipo_curso')),
                        'tipo_curso': clean_text(row.get('tipo_curso')),
                        'co_tipo_nivel': clean_text(row.get('co_tipo_nivel')),
                        'ds_tipo_nivel': clean_text(row.get('ds_tipo_nivel')),
                        'carga_horaria': clean_text(row.get('carga_horaria')),
                        'situacao_matricula': clean_text(row.get('situacao_matricula')),
                        'tipo_cota': clean_text(row.get('tipo_cota')),
                        'atestado_baixarenda': clean_text(row.get('atestado_baixarenda')),
                        'ano': clean_text(row.get('ano')),
                        'modalidade_ensino': clean_text(row.get('modalidade_ensino')),
                        'Data de Nascimento': clean_text(row.get('Data de Nascimento')),
                        'Idade': clean_text(row.get('Idade')),
                        'Sexo': clean_text(row.get('Sexo')),
                        'Nacionalidade': clean_text(row.get('Nacionalidade')),
                        'Raça': clean_text(row.get('Raça')),
                        'Portador de Deficiência': clean_text(row.get('Portador de Deficiência')),
                        'Data Admissão Declarada': clean_text(row.get('Data Admissão Declarada')),
                        'Vl Remun Média Nom': clean_text(row.get('Vl Remun Média Nom')),
                        'Vl Remun Média SM': clean_text(row.get('Vl Remun Média SM')),
                        'Vl Remun Dezembro Nom': clean_text(row.get('Vl Remun Dezembro Nom')),
                        'Vl Remun Dezembro SM': clean_text(row.get('Vl Remun Dezembro SM')),
                        'Tempo Emprego': clean_text(row.get('Tempo Emprego')),
                        'Qtd Hora Contr': clean_text(row.get('Qtd Hora Contr')),
                        'Vl Última Remuneração Ano': clean_text(row.get('Vl Última Remuneração Ano')),
                        'Vl Salário Contratual': clean_text(row.get('Vl Salário Contratual')),
                        'Razão Social': clean_text(row.get('Razão Social')),
                        'Município': clean_text(row.get('Município')),
                        'Motivo desligamento': clean_text(row.get('Motivo desligamento')),
                        'Mês Desligamento': clean_text(row.get('Mês Desligamento')),
                        'Tipo de vínculo empregatício': clean_text(row.get('Tipo de vínculo empregatício')),
                        'Indicador de vínculo ativo em 31_12': clean_text(row.get('Indicador de vínculo ativo em 31_12')),
                        'Ano_RAIS': clean_text(row.get('Ano_RAIS'))
                    }

                    batch.append(cleaned_row)

                    if len(batch) >= batch_size:
                        cursor.executemany(insert_query, batch)
                        conn.commit()
                        batch = []
                        print(f"{reader.line_num} linhas processadas...")

                except Exception as e:
                    print(f"\nERRO na linha {reader.line_num}:")
                    print(f"Conteúdo da linha: {row}")
                    print(f"Erro detalhado: {str(e)}")
                    raise

            # Inserir o último lote
            if batch:
                cursor.executemany(insert_query, batch)
                conn.commit()

            print(
                f"Total de {reader.line_num} linhas importadas do mundo_trabalho")

    # Transferir dados para tabelas permanentes
    transfer_mundo_trabalho_data(cursor)
    conn.commit()

    # Verificação final
    print("\nVerificando dados importados...")
    cursor.execute("SELECT COUNT(*) FROM public.aluno_ifb")
    print(f"Total alunos: {cursor.fetchone()[0]}")
    cursor.execute("SELECT COUNT(*) FROM public.matricula_ifb")
    print(f"Total matrículas: {cursor.fetchone()[0]}")
    cursor.execute("SELECT COUNT(*) FROM public.vinculo_empregaticio")
    print(f"Total vínculos: {cursor.fetchone()[0]}")

    print("\nImportação concluída com sucesso!")

except Exception as e:
    print(f"Erro geral: {e}")
    if 'conn' in locals():
        conn.rollback()
finally:
    if 'conn' in locals() and conn is not None:
        conn.close()
    print("Conexão encerrada.")
