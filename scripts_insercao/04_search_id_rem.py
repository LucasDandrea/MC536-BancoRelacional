import sys
import numpy as np
from datetime import datetime
import logging
import pandas as pd
import psycopg2

# Configurações de conexão (mantendo seu padrão)
dbname = ""
user = ""
host = ""
password = ""  

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('remuneracao_import.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Colunas relevantes para a tabela remuneracao
REMUNERACAO_COLS = [
    'vl_remun_media_nom',
    'vl_remun_media_sm',
    'vl_remun_dezembro_nom',
    'vl_remun_dezembro_sm',
    'qtd_hora_contr',
    'vl_ultima_remuneracao_ano',
    'vl_salario_contratual'
]


def prepare_value(val):
    """Prepara valores para comparação, tratando nulos e formatos especiais"""
    if pd.isna(val) or val in (None, '', 'NA', 'NaN', 'nan'):
        return None
    if isinstance(val, str):
        val = val.strip()
        if ',' in val:  # Trata vírgula decimal
            try:
                return float(val.replace(',', '.'))
            except ValueError:
                return None
        if val == '':
            return None
    try:
        return float(val) if '.' in str(val) else int(val)
    except (ValueError, TypeError):
        return val


def find_matching_id(cursor, row, exact_match=True):
    """
    Encontra o ID correspondente na tabela remuneracao
    :param exact_match: Se True, busca match exato; se False, busca aproximado
    """
    if exact_match:
        query = """
        SELECT id_remuneracao FROM public.remuneracao
        WHERE 
            (vl_remun_media_nom IS NULL AND %s IS NULL OR ABS(vl_remun_media_nom - %s) < 0.01) AND
            (vl_remun_media_sm IS NULL AND %s IS NULL OR ABS(vl_remun_media_sm - %s) < 0.01) AND
            (vl_remun_dezembro_nom IS NULL AND %s IS NULL OR ABS(vl_remun_dezembro_nom - %s) < 0.01) AND
            (vl_remun_dezembro_sm IS NULL AND %s IS NULL OR ABS(vl_remun_dezembro_sm - %s) < 0.01) AND
            (qtd_hora_contr IS NULL AND %s IS NULL OR qtd_hora_contr = %s) AND
            (vl_ultima_remuneracao_ano IS NULL AND %s IS NULL OR ABS(vl_ultima_remuneracao_ano - %s) < 0.01) AND
            (vl_salario_contratual IS NULL AND %s IS NULL OR ABS(vl_salario_contratual - %s) < 0.01)
        LIMIT 1
        """
    else:
        query = """
        SELECT id_remuneracao, 
               (COALESCE(ABS(vl_remun_media_nom - %s), 1000) +
                COALESCE(ABS(vl_remun_media_sm - %s), 1000) +
                COALESCE(ABS(vl_remun_dezembro_nom - %s), 1000)) as diff
        FROM public.remuneracao
        ORDER BY diff
        LIMIT 1
        """

    params = []
    for col in REMUNERACAO_COLS:
        val = prepare_value(row[col])
        if exact_match:
            # Cada coluna é comparada duas vezes na query exata
            params.extend([val, val])
        else:
            params.append(val)

    try:
        cursor.execute(query, params)
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        logger.error(f"Erro na busca por ID: {str(e)}")
        return None


def main():
    try:
        logger.info("Iniciando processo de associação de IDs de remuneração")
        start_time = datetime.now()

        # 1. Conectar ao banco de dados
        logger.info("Estabelecendo conexão com o banco de dados...")
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            host=host,
            password=password
        )
        logger.info("Conexão estabelecida com sucesso")

        # 2. Carregar dados do CSV
        logger.info("Carregando arquivo remuneracao.csv...")
        try:
            # Carrega apenas colunas relevantes, tratando valores ausentes
            df = pd.read_csv(
                '../dados/tratados/remuneracao.csv',
                usecols=lambda x: x in REMUNERACAO_COLS,
                dtype={
                    'vl_remun_media_nom': 'float64',
                    'vl_remun_media_sm': 'float64',
                    'vl_remun_dezembro_nom': 'float64',
                    'vl_remun_dezembro_sm': 'float64',
                    'qtd_hora_contr': 'Int64',
                    'vl_ultima_remuneracao_ano': 'float64',
                    'vl_salario_contratual': 'float64'
                },
                na_values=['', 'NA', 'NaN', 'nan', 'N/A']
            )

            # Remove linhas completamente vazias
            df.dropna(how='all', inplace=True)
            logger.info(f"Total de registros carregados: {len(df)}")
        except Exception as e:
            logger.error(f"Erro ao carregar CSV: {str(e)}")
            raise

        # 3. Processar cada linha e encontrar IDs correspondentes
        logger.info("Iniciando busca de IDs correspondentes...")
        ids = []
        not_found_count = 0
        cursor = conn.cursor()

        for index, row in df.iterrows():
            try:
                # Primeiro tenta match exato
                row_id = find_matching_id(cursor, row, exact_match=True)

                if row_id is None:
                    not_found_count += 1
                    # Tenta match aproximado
                    row_id = find_matching_id(cursor, row, exact_match=False)

                    if row_id:
                        logger.warning(
                            f"Linha {index+1}: Usando ID aproximado {row_id}")
                    else:
                        logger.warning(
                            f"Linha {index+1}: Nenhum ID encontrado (exato ou aproximado)")

                ids.append(row_id)

                # Log de progresso a cada 1000 linhas
                if (index + 1) % 1000 == 0:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    logger.info(
                        f"Progresso: {index+1}/{len(df)} linhas | "
                        f"IDs encontrados: {index+1-not_found_count} | "
                        f"Tempo decorrido: {elapsed:.2f}s"
                    )

            except Exception as e:
                logger.error(f"Erro ao processar linha {index+1}: {str(e)}")
                ids.append(None)

        # 4. Adicionar IDs ao DataFrame e salvar
        df['id_remuneracao'] = ids

        df.to_csv('../dados/tratados/remuneracao_com_id.csv', index=False, encoding='utf-8')

        # 5. Estatísticas finais
        total_time = (datetime.now() - start_time).total_seconds()
        success_count = len([x for x in ids if x is not None])
        failure_count = len(df) - success_count

        logger.info("\n" + "="*50)
        logger.info("RELATÓRIO FINAL")
        logger.info(f"Total de linhas processadas: {len(df)}")
        logger.info(f"IDs encontrados (exatos): {len(df) - not_found_count}")
        logger.info(
            f"IDs encontrados (aproximados): {not_found_count - failure_count}")
        logger.info(f"IDs não encontrados: {failure_count}")
        logger.info(f"Tempo total de execução: {total_time:.2f} segundos")
        logger.info(
            f"Velocidade média: {len(df)/total_time:.2f} linhas/segundo")
        logger.info("="*50)

        return df

    except Exception as e:
        logger.error(f"Erro no processo principal: {str(e)}", exc_info=True)
        return None

    finally:
        if 'conn' in locals() and conn is not None:
            conn.close()
            logger.info("Conexão com o banco de dados fechada")


if __name__ == "__main__":
    main()
