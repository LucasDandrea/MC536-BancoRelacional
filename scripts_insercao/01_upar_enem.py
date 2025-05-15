import psycopg2
import zipfile
import io
import csv
from datetime import datetime
from getpass import getpass

# Configurações de conexão
dbname = ""
user = ""
host = ""
password = ""

def check_numeric(value, precision, scale, field_name):
    """Verifica se um valor numérico está dentro dos limites da coluna"""
    if value is None or value == '':
        return None
    
    try:
        num = float(value)
        max_val = 10 ** (precision - scale)
        if abs(num) >= max_val:
            raise ValueError(f"Valor {num} excede a precisão permitida ({precision},{scale}) para o campo {field_name}")
        return num
    except ValueError as e:
        print(f"Valor inválido encontrado: {value}")
        raise

def transfer_enem_data(cursor):
    print("\nIniciando transferência para tabelas permanentes...")
    
    try:
        # 1. Localizações (mantido igual)
        cursor.execute("""
            INSERT INTO public.localizacao (uf, municipio)
            SELECT DISTINCT ON (SG_UF_ESCOLA, NO_MUNICIPIO_ESCOLA)
                SG_UF_ESCOLA, NO_MUNICIPIO_ESCOLA
            FROM temp_enem
            WHERE NOT EXISTS (
                SELECT 1 FROM public.localizacao 
                WHERE uf = temp_enem.SG_UF_ESCOLA 
                AND municipio = temp_enem.NO_MUNICIPIO_ESCOLA
            )
        """)


        # 2. Transferir escolas - atualiza se existir, insere se não existir
        cursor.execute("""
            INSERT INTO public.escola (
                co_escola_educacenso, no_escola_educacenso, 
                tp_dependencia_adm_escola, tp_localizacao_escola,
                inse, id_loc
            )
            SELECT DISTINCT ON (e.CO_ESCOLA_EDUCACENSO)
                e.CO_ESCOLA_EDUCACENSO, e.NO_ESCOLA_EDUCACENSO,
                e.TP_DEPENDENCIA_ADM_ESCOLA, e.TP_LOCALIZACAO_ESCOLA,
                e.INSE, l.id_loc
            FROM temp_enem e
            JOIN public.localizacao l ON e.SG_UF_ESCOLA = l.uf AND e.NO_MUNICIPIO_ESCOLA = l.municipio
            WHERE NOT EXISTS (
                SELECT 1 FROM public.escola 
                WHERE co_escola_educacenso = e.CO_ESCOLA_EDUCACENSO
            )
            ORDER BY e.CO_ESCOLA_EDUCACENSO, e.NU_ANO DESC  -- Pega o registro mais recente
        """)
        print(f"Escolas transferidas: {cursor.rowcount} registros")

        # 3. Atualizar escolas existentes com dados mais recentes (se necessário)
        cursor.execute("""
            UPDATE public.escola e
            SET 
                no_escola_educacenso = t.NO_ESCOLA_EDUCACENSO,
                tp_dependencia_adm_escola = t.TP_DEPENDENCIA_ADM_ESCOLA,
                tp_localizacao_escola = t.TP_LOCALIZACAO_ESCOLA,
                inse = t.INSE,
                id_loc = l.id_loc
            FROM (
                SELECT DISTINCT ON (CO_ESCOLA_EDUCACENSO)
                    CO_ESCOLA_EDUCACENSO, NO_ESCOLA_EDUCACENSO,
                    TP_DEPENDENCIA_ADM_ESCOLA, TP_LOCALIZACAO_ESCOLA,
                    INSE, SG_UF_ESCOLA, NO_MUNICIPIO_ESCOLA
                FROM temp_enem
                ORDER BY CO_ESCOLA_EDUCACENSO, NU_ANO DESC
            ) t
            JOIN public.localizacao l ON t.SG_UF_ESCOLA = l.uf AND t.NO_MUNICIPIO_ESCOLA = l.municipio
            WHERE e.co_escola_educacenso = t.CO_ESCOLA_EDUCACENSO
        """)
        print(f"Escolas atualizadas: {cursor.rowcount} registros")


        # 3. Indicadores escolares (COM porte e formação docente por ano)
        cursor.execute("""
            INSERT INTO public.indicadores_escolares (
                nu_ano, nu_taxa_permanencia, nu_taxa_aprovacao,
                nu_taxa_reprovacao, nu_taxa_abandono, co_escola_educacenso,
                porte_escola, pc_formacao_docente
            )
            SELECT 
                NU_ANO, NU_TAXA_PERMANENCIA, NU_TAXA_APROVACAO,
                NU_TAXA_REPROVACAO, NU_TAXA_ABANDONO, CO_ESCOLA_EDUCACENSO,
                PORTE_ESCOLA, PC_FORMACAO_DOCENTE
            FROM temp_enem
            ORDER BY CO_ESCOLA_EDUCACENSO, NU_ANO
        """)
        print("\nIniciando transferência para tabelas permanentes...")

        # 3. Transferir desempenho ENEM (permite múltiplos registros por escola/ano)
        cursor.execute("""
            INSERT INTO public.desempenho_enem (
                nu_ano, nu_media_cn, nu_media_ch, nu_media_lp, 
                nu_media_mt, nu_media_red, nu_media_obj, nu_media_tot, 
                co_escola_educacenso
            )
            SELECT 
                NU_ANO, NU_MEDIA_CN, NU_MEDIA_CH, NU_MEDIA_LP,
                NU_MEDIA_MT, NU_MEDIA_RED, NU_MEDIA_OBJ, NU_MEDIA_TOT,
                CO_ESCOLA_EDUCACENSO
            FROM temp_enem
        """)
        print(f"Desempenhos ENEM transferidos: {cursor.rowcount} registros")

        # 5. Transferir participação ENEM (permite múltiplos registros por escola/ano)
        cursor.execute("""
            INSERT INTO public.participacao_enem (
                nu_ano, nu_matriculas, nu_participantes_nec_esp,
                nu_participantes, nu_taxa_participacao, co_escola_educacenso
            )
            SELECT 
                NU_ANO, NU_MATRICULAS, NU_PARTICIPANTES_NEC_ESP,
                NU_PARTICIPANTES, NU_TAXA_PARTICIPACAO, CO_ESCOLA_EDUCACENSO
            FROM temp_enem
        """)
        print(f"Participações ENEM transferidas: {cursor.rowcount} registros")

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
    
    # 1. Processar arquivo MICRODADOS_ENEM_ESCOLA
    print("\nProcessando microdados_enem_por_escola.zip...")
    with zipfile.ZipFile('../dados/brutos/microdados_enem_por_escola.zip') as z:
        with z.open('DADOS/MICRODADOS_ENEM_ESCOLA.csv') as f:
            # Ler o arquivo CSV diretamente do ZIP
            csv_file = io.TextIOWrapper(f, encoding='latin1')
            reader = csv.DictReader(csv_file, delimiter=';')
            
            # Pular linhas anteriores a 2009 (linha 67620)
            for _ in range(67619):  # 0-based index
                next(reader)
            
            # Criar tabela temporária com precisão aumentada para NU_TAXA_PERMANENCIA
            cursor.execute("""
                CREATE TEMPORARY TABLE temp_enem (
                    NU_ANO integer,
                    CO_UF_ESCOLA varchar(2),
                    SG_UF_ESCOLA varchar(2),
                    CO_MUNICIPIO_ESCOLA varchar(10),
                    NO_MUNICIPIO_ESCOLA varchar(100),
                    CO_ESCOLA_EDUCACENSO varchar(20),
                    NO_ESCOLA_EDUCACENSO varchar(200),
                    TP_DEPENDENCIA_ADM_ESCOLA smallint,
                    TP_LOCALIZACAO_ESCOLA smallint,
                    NU_MATRICULAS integer,
                    NU_PARTICIPANTES_NEC_ESP integer,
                    NU_PARTICIPANTES integer,
                    NU_TAXA_PARTICIPACAO numeric(5,2),
                    NU_MEDIA_CN numeric(6,2),
                    NU_MEDIA_CH numeric(6,2),
                    NU_MEDIA_LP numeric(6,2),
                    NU_MEDIA_MT numeric(6,2),
                    NU_MEDIA_RED numeric(6,2),
                    NU_MEDIA_OBJ numeric(6,2),
                    NU_MEDIA_TOT numeric(6,2),
                    INSE varchar(10),
                    PC_FORMACAO_DOCENTE numeric(5,2),
                    NU_TAXA_PERMANENCIA numeric(6,2),  -- PRECISÃO ALTERADA DE 5,2 PARA 6,2
                    NU_TAXA_APROVACAO numeric(5,2),
                    NU_TAXA_REPROVACAO numeric(5,2),
                    NU_TAXA_ABANDONO numeric(5,2),
                    PORTE_ESCOLA varchar(50)
                )
            """)
            
            # Preparar query de inserção
            insert_query = """
                INSERT INTO temp_enem VALUES (
                    %(NU_ANO)s, %(CO_UF_ESCOLA)s, %(SG_UF_ESCOLA)s, %(CO_MUNICIPIO_ESCOLA)s,
                    %(NO_MUNICIPIO_ESCOLA)s, %(CO_ESCOLA_EDUCACENSO)s, %(NO_ESCOLA_EDUCACENSO)s,
                    %(TP_DEPENDENCIA_ADM_ESCOLA)s, %(TP_LOCALIZACAO_ESCOLA)s, %(NU_MATRICULAS)s,
                    %(NU_PARTICIPANTES_NEC_ESP)s, %(NU_PARTICIPANTES)s, %(NU_TAXA_PARTICIPACAO)s,
                    %(NU_MEDIA_CN)s, %(NU_MEDIA_CH)s, %(NU_MEDIA_LP)s, %(NU_MEDIA_MT)s,
                    %(NU_MEDIA_RED)s, %(NU_MEDIA_OBJ)s, %(NU_MEDIA_TOT)s, %(INSE)s,
                    %(PC_FORMACAO_DOCENTE)s, %(NU_TAXA_PERMANENCIA)s, %(NU_TAXA_APROVACAO)s,
                    %(NU_TAXA_REPROVACAO)s, %(NU_TAXA_ABANDONO)s, %(PORTE_ESCOLA)s
                )
            """
            
            # Inserir linhas em lotes
            batch_size = 1000
            batch = []
            
            for row in reader:
                try:
                    # Converter e validar valores numéricos
                    cleaned_row = {
                        'NU_ANO': int(row['NU_ANO']) if row['NU_ANO'] else None,
                        'CO_UF_ESCOLA': row['CO_UF_ESCOLA'],
                        'SG_UF_ESCOLA': row['SG_UF_ESCOLA'],
                        'CO_MUNICIPIO_ESCOLA': row['CO_MUNICIPIO_ESCOLA'],
                        'NO_MUNICIPIO_ESCOLA': row['NO_MUNICIPIO_ESCOLA'],
                        'CO_ESCOLA_EDUCACENSO': row['CO_ESCOLA_EDUCACENSO'],
                        'NO_ESCOLA_EDUCACENSO': row['NO_ESCOLA_EDUCACENSO'],
                        'TP_DEPENDENCIA_ADM_ESCOLA': int(row['TP_DEPENDENCIA_ADM_ESCOLA']) if row['TP_DEPENDENCIA_ADM_ESCOLA'] else None,
                        'TP_LOCALIZACAO_ESCOLA': int(row['TP_LOCALIZACAO_ESCOLA']) if row['TP_LOCALIZACAO_ESCOLA'] else None,
                        'NU_MATRICULAS': int(row['NU_MATRICULAS']) if row['NU_MATRICULAS'] else None,
                        'NU_PARTICIPANTES_NEC_ESP': int(row['NU_PARTICIPANTES_NEC_ESP']) if row['NU_PARTICIPANTES_NEC_ESP'] else None,
                        'NU_PARTICIPANTES': int(row['NU_PARTICIPANTES']) if row['NU_PARTICIPANTES'] else None,
                        'NU_TAXA_PARTICIPACAO': check_numeric(row['NU_TAXA_PARTICIPACAO'], 5, 2, 'NU_TAXA_PARTICIPACAO'),
                        'NU_MEDIA_CN': check_numeric(row['NU_MEDIA_CN'], 6, 2, 'NU_MEDIA_CN'),
                        'NU_MEDIA_CH': check_numeric(row['NU_MEDIA_CH'], 6, 2, 'NU_MEDIA_CH'),
                        'NU_MEDIA_LP': check_numeric(row['NU_MEDIA_LP'], 6, 2, 'NU_MEDIA_LP'),
                        'NU_MEDIA_MT': check_numeric(row['NU_MEDIA_MT'], 6, 2, 'NU_MEDIA_MT'),
                        'NU_MEDIA_RED': check_numeric(row['NU_MEDIA_RED'], 6, 2, 'NU_MEDIA_RED'),
                        'NU_MEDIA_OBJ': check_numeric(row['NU_MEDIA_OBJ'], 6, 2, 'NU_MEDIA_OBJ'),
                        'NU_MEDIA_TOT': check_numeric(row['NU_MEDIA_TOT'], 6, 2, 'NU_MEDIA_TOT'),
                        'INSE': row['INSE'],
                        'PC_FORMACAO_DOCENTE': check_numeric(row['PC_FORMACAO_DOCENTE'], 5, 2, 'PC_FORMACAO_DOCENTE'),
                        'NU_TAXA_PERMANENCIA': check_numeric(row['NU_TAXA_PERMANENCIA'], 6, 2, 'NU_TAXA_PERMANENCIA'),  # PRECISÃO ALTERADA
                        'NU_TAXA_APROVACAO': check_numeric(row['NU_TAXA_APROVACAO'], 5, 2, 'NU_TAXA_APROVACAO'),
                        'NU_TAXA_REPROVACAO': check_numeric(row['NU_TAXA_REPROVACAO'], 5, 2, 'NU_TAXA_REPROVACAO'),
                        'NU_TAXA_ABANDONO': check_numeric(row['NU_TAXA_ABANDONO'], 5, 2, 'NU_TAXA_ABANDONO'),
                        'PORTE_ESCOLA': row['PORTE_ESCOLA']
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
                    # Mostrar valores problemáticos
                    for k, v in row.items():
                        if k in ['NU_TAXA_PERMANENCIA']:  # Foco no campo problemático
                            print(f"Campo {k}: {v} (Tamanho: {len(v) if v else 0})")
                    raise  # Re-lança a exceção para interromper a execução
            
            # Inserir o último lote
            if batch:
                cursor.executemany(insert_query, batch)
                conn.commit()
            
            print(f"Total de {reader.line_num} linhas importadas do ENEM")
    
    # Transferir dados do ENEM para tabelas permanentes
    transfer_enem_data(cursor)
    conn.commit()  
    print("\nImportação concluída com sucesso!")

except Exception as e:
    print(f"Erro geral: {e}")
    conn.rollback()
finally:
    if conn is not None:
        conn.close()
    print("Conexão encerrada.")