import psycopg2
import csv
from getpass import getpass

# Configurações de conexão (mantendo seu padrão)
dbname = ""
user = ""
host = ""
password = ""  


def check_numeric(value, precision, scale, field_name):
    """Função idêntica à que você já está usando"""
    if value is None or value == '':
        return None

    try:
        num = float(value)
        max_val = 10 ** (precision - scale)
        if abs(num) >= max_val:
            raise ValueError(
                f"Valor {num} excede a precisão permitida ({precision},{scale}) para o campo {field_name}")
        return num
    except ValueError as e:
        print(f"Valor inválido encontrado: {value}")
        raise


def import_remuneracao_data(cursor, csv_file):
    """Importa dados do arquivo CSV para a tabela remuneracao"""
    print("\nIniciando importação para tabela remuneracao...")

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

        # Criar tabela temporária (seguindo seu padrão)
        cursor.execute("""
            CREATE TEMPORARY TABLE temp_remuneracao (
                vl_remun_media_nom numeric(10,2),
                vl_remun_media_sm numeric(10,2),
                vl_remun_dezembro_nom numeric(10,2),
                vl_remun_dezembro_sm numeric(10,2),
                qtd_hora_contr integer,
                vl_ultima_remuneracao_ano numeric(10,2),
                vl_salario_contratual numeric(10,2)
            )
        """)

        # Preparar query de inserção na temporária
        insert_temp_query = """
            INSERT INTO temp_remuneracao VALUES (
                %(vl_remun_media_nom)s, %(vl_remun_media_sm)s,
                %(vl_remun_dezembro_nom)s, %(vl_remun_dezembro_sm)s,
                %(qtd_hora_contr)s, %(vl_ultima_remuneracao_ano)s,
                %(vl_salario_contratual)s
            )
        """

        # Processar o arquivo CSV
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            batch = []
            batch_size = 1000  # Mesmo batch size que você usou

            for row in reader:
                try:
                    # Limpar e validar os dados (seguindo seu padrão)
                    cleaned_row = {
                        'vl_remun_media_nom': check_numeric(row['vl_remun_media_nom'], 10, 2, 'vl_remun_media_nom'),
                        'vl_remun_media_sm': check_numeric(row['vl_remun_media_sm'], 10, 2, 'vl_remun_media_sm'),
                        'vl_remun_dezembro_nom': check_numeric(row['vl_remun_dezembro_nom'], 10, 2, 'vl_remun_dezembro_nom'),
                        'vl_remun_dezembro_sm': check_numeric(row['vl_remun_dezembro_sm'], 10, 2, 'vl_remun_dezembro_sm'),
                        'qtd_hora_contr': int(row['qtd_hora_contr']) if row['qtd_hora_contr'] else None,
                        'vl_ultima_remuneracao_ano': check_numeric(row['vl_ultima_remuneracao_ano'], 10, 2, 'vl_ultima_remuneracao_ano'),
                        'vl_salario_contratual': check_numeric(row['vl_salario_contratual'], 10, 2, 'vl_salario_contratual')
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

        # Transferir da temporária para a tabela permanente
        cursor.execute("""
            INSERT INTO public.remuneracao (
                vl_remun_media_nom, vl_remun_media_sm,
                vl_remun_dezembro_nom, vl_remun_dezembro_sm,
                qtd_hora_contr, vl_ultima_remuneracao_ano,
                vl_salario_contratual
            )
            SELECT 
                vl_remun_media_nom, vl_remun_media_sm,
                vl_remun_dezembro_nom, vl_remun_dezembro_sm,
                qtd_hora_contr, vl_ultima_remuneracao_ano,
                vl_salario_contratual
            FROM temp_remuneracao
        """)
        print(
            f"Total de {cursor.rowcount} registros importados para remuneracao")

        return True

    except Exception as e:
        print(f"Erro durante importação: {e}")
        raise


# Conectar ao banco de dados (mantendo seu padrão)
try:
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        host=host,
        password=password
    )
    cursor = conn.cursor()
    print("Conexão estabelecida com sucesso!")

    # Importar dados (altere para o caminho correto do seu arquivo)
    import_remuneracao_data(cursor, '../dados/tratados/remuneracao.csv')
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
