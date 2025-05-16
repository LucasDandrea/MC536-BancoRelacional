import psycopg2
import csv
from datetime import datetime

# Configurações de conexão
dbname = ""
user = ""
host = ""
password = ""



def parse_custom_date(date_str):
    """Converte datas no formato 7082018 (7/08/2018) ou 15102018 (15/10/2018) para date"""
    if not date_str or date_str.strip() == '':
        return None

    try:
        # Padroniza o tamanho para 8 dígitos (adiciona zero à esquerda se necessário)
        date_str = date_str.zfill(8)

        day = int(date_str[:2])
        month = int(date_str[2:4])
        year = int(date_str[4:8])

        return datetime(year, month, day).date()
    except ValueError as e:
        print(f"Erro ao converter data '{date_str}': {e}")
        return None


def clean_numeric(value, is_integer=False):
    """Limpa e converte valores numéricos, tratando vírgula como separador decimal"""
    if not value or value.strip() == '':
        return None

    try:
        # Remove possíveis pontos de milhar e substitui vírgula decimal por ponto
        cleaned = value.replace('.', '').replace(',', '.')

        if is_integer:
            # Para inteiros, arredondamos o valor
            return int(round(float(cleaned)))
        else:
            return float(cleaned)
    except ValueError as e:
        print(f"Valor numérico inválido '{value}': {e}")
        return None


def import_vinculo_data(cursor, csv_file):
    """Importa dados do arquivo CSV para a tabela vinculo_empregaticio"""
    print("\nIniciando importação para tabela vinculo_empregaticio...")

    try:
        # Verificar se o arquivo CSV existe
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                if not reader.fieldnames:
                    raise ValueError(
                        "O arquivo CSV está vazio ou mal formatado")
        except FileNotFoundError:
            raise FileNotFoundError(f"Arquivo {csv_file} não encontrado")

        # Criar tabela temporária
        cursor.execute("""
            CREATE TEMPORARY TABLE temp_vinculo (
                ano_rais integer,
                razao_social varchar(200),
                tipo_vinculo_empregaticio varchar(200),
                data_admissao_declarada varchar(20),  -- Será convertida depois
                tempo_emprego numeric(10,1),          -- Alterado para numeric para aceitar decimais
                motivo_desligamento varchar(200),
                mes_desligamento varchar(20),
                indicador_vinculo_ativo varchar(3),   -- SIM/NÃO
                id_aluno integer,
                id_loc integer,                       -- Agora temos o ID direto
                id_remuneracao integer
            )
        """)

        # Preparar query de inserção na temporária
        insert_temp_query = """
            INSERT INTO temp_vinculo VALUES (
                %(ano_rais)s, %(razao_social)s, %(tipo_vinculo_empregaticio)s,
                %(data_admissao_declarada)s, %(tempo_emprego)s, %(motivo_desligamento)s,
                %(mes_desligamento)s, %(indicador_vinculo_ativo)s, %(id_aluno)s,
                %(id_loc)s, %(id_remuneracao)s
            )
        """

        # Processar o arquivo CSV
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            batch = []
            batch_size = 1000

            for row in reader:
                try:
                    # Limpar e validar os dados
                    cleaned_row = {
                        'ano_rais': clean_numeric(row['ano_rais'], is_integer=True),
                        'razao_social': row['razao_social'].strip() if row['razao_social'] else None,
                        'tipo_vinculo_empregaticio': row['tipo_vinculo_empregaticio'].strip() if row['tipo_vinculo_empregaticio'] else None,
                        'data_admissao_declarada': row['data_admissao_declarada'].strip() if row['data_admissao_declarada'] else None,
                        # Agora aceita decimais
                        'tempo_emprego': clean_numeric(row['tempo_emprego']),
                        'motivo_desligamento': row['motivo_desligamento'].strip() if row['motivo_desligamento'] and row['motivo_desligamento'].strip() != '' else None,
                        'mes_desligamento': row['mes_desligamento'].strip() if row['mes_desligamento'] and row['mes_desligamento'].strip() != '' else None,
                        'indicador_vinculo_ativo': 'SIM' if row['indicador_vinculo_ativo'].upper() == 'SIM' else 'NÃO',
                        'id_aluno': clean_numeric(row['id_aluno'], is_integer=True),
                        'id_loc': clean_numeric(row['id_loc'], is_integer=True),
                        'id_remuneracao': clean_numeric(row['id_remuneracao'], is_integer=True)
                    }

                    batch.append(cleaned_row)

                    if len(batch) >= batch_size:
                        cursor.executemany(insert_temp_query, batch)
                        batch = []
                        print(f"{reader.line_num} linhas processadas...")

                except Exception as e:
                    print(f"\nERRO na linha {reader.line_num}:")
                    print(f"Conteúdo da linha: {row}")
                    print(f"Erro detalhado: {str(e)}")
                    raise

            # Inserir o último lote
            if batch:
                cursor.executemany(insert_temp_query, batch)

        # Inserir dados na tabela vinculo_empregaticio com todas as FKs
        cursor.execute("""
            INSERT INTO public.vinculo_empregaticio (
                ano_rais, razao_social, tipo_vinculo_empregaticio,
                data_admissao_declarada, tempo_emprego, motivo_desligamento,
                mes_desligamento, indicador_vinculo_ativo, id_aluno,
                id_loc, id_remuneracao
            )
            SELECT 
                v.ano_rais, v.razao_social, v.tipo_vinculo_empregaticio,
                parse_custom_date(v.data_admissao_declarada), 
                v.tempo_emprego,  -- Agora aceita valores decimais
                NULLIF(v.motivo_desligamento, ''), NULLIF(v.mes_desligamento, ''),
                CASE WHEN v.indicador_vinculo_ativo = 'SIM' THEN TRUE ELSE FALSE END,
                v.id_aluno, v.id_loc, v.id_remuneracao
            FROM temp_vinculo v
        """)
        print(
            f"Total de {cursor.rowcount} registros inseridos na tabela vinculo_empregaticio")

        # Verificar registros com problemas de FK
        cursor.execute("""
            SELECT COUNT(*) 
            FROM public.vinculo_empregaticio 
            WHERE id_loc IS NULL
        """)
        count = cursor.fetchone()[0]
        if count > 0:
            print(
                f"AVISO: {count} registros com id_loc nulo ou inválido")

        return True

    except Exception as e:
        print(f"Erro durante importação: {e}")
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

    # Criar função auxiliar para parse de datas no PostgreSQL
    cursor.execute("""
        CREATE OR REPLACE FUNCTION parse_custom_date(date_str varchar) 
        RETURNS date AS $$
        DECLARE
            day_part int;
            month_part int;
            year_part int;
        BEGIN
            IF date_str IS NULL OR date_str = '' THEN
                RETURN NULL;
            END IF;
            
            -- Padroniza para 8 dígitos
            date_str := LPAD(date_str, 8, '0');
            
            day_part := CAST(SUBSTRING(date_str FROM 1 FOR 2) AS int);
            month_part := CAST(SUBSTRING(date_str FROM 3 FOR 2) AS int);
            year_part := CAST(SUBSTRING(date_str FROM 5 FOR 4) AS int);
            
            RETURN MAKE_DATE(year_part, month_part, day_part);
        EXCEPTION WHEN OTHERS THEN
            RETURN NULL;
        END;
        $$ LANGUAGE plpgsql;
    """)
    conn.commit()

    # Importar dados (altere para o caminho correto do seu arquivo)
    import_vinculo_data(cursor, '../dados/tratados/vinculo.csv')
    conn.commit()
    print("\nImportação concluída com sucesso!")

except Exception as e:
    print(f"Erro geral: {e}")
    if conn:
        conn.rollback()
finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
    print("Conexão encerrada.")
