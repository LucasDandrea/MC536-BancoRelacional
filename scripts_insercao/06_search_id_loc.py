import pandas as pd
import psycopg2
import logging
from datetime import datetime
import sys

# Configurações de conexão
dbname = ""
user = ""
host = ""
password = ""


# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('localizacao_import.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Colunas relevantes para a tabela localizacao
LOCALIZACAO_COLS = ['uf', 'municipio']



def find_matching_location_id(cursor, uf_prep, municipio_prep, exact_match=True):
    """
    Encontra o ID correspondente na tabela localizacao
    :param exact_match: Se True, busca match exato; se False, busca aproximado
    """

    if exact_match:
        query = """
        SELECT id_loc FROM public.localizacao
        WHERE 
            (uf IS NULL AND %s IS NULL OR uf = %s) AND
            (municipio IS NULL AND %s IS NULL OR municipio = %s)
        LIMIT 1
        """
        params = [uf_prep, uf_prep, municipio_prep, municipio_prep]
    else:
        query = """
        SELECT id_loc FROM public.localizacao
        WHERE 
            (uf = %s OR %s IS NULL) AND
            (municipio ILIKE %s OR %s IS NULL)
        LIMIT 1
        """
        params = [uf_prep, uf_prep, f"%{municipio_prep}%", municipio_prep]

    try:
        cursor.execute(query, params)
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        logger.error(f"Erro na busca por localização: {str(e)}")
        return None


def main():
    try:
        logger.info("Iniciando processo de associação de IDs de localização")
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
        try:
            df = pd.read_csv(
                "../dados/tratados/localizacao.csv",
                usecols=LOCALIZACAO_COLS,
                dtype={'uf': 'str', 'municipio': 'str'},
                na_values=['', 'NA', 'NaN', 'nan', 'N/A']
            )

            # Remove linhas completamente vazias
            df.dropna(how='all', subset=LOCALIZACAO_COLS, inplace=True)
            logger.info(f"Total de registros carregados: {len(df)}")
        except Exception as e:
            logger.error(f"Erro ao carregar CSV: {str(e)}")
            raise

        # 3. Processar cada linha e encontrar IDs correspondentes
        logger.info("Iniciando busca de IDs de localização...")
        ids = []
        not_found_count = 0
        cursor = conn.cursor()

        for index, row in df.iterrows():
            try:
                # Primeiro tenta match exato
                loc_id = find_matching_location_id(
                    cursor,
                    row['uf'],
                    row['municipio'],
                    exact_match=True
                )

                if loc_id is None:
                    not_found_count += 1
                    # Tenta match aproximado
                    loc_id = find_matching_location_id(
                        cursor,
                        row['uf'],
                        row['municipio'],
                        exact_match=False
                    )

                    if not loc_id:
                        logger.warning(
                            f"Linha {index+1}: Nenhum ID encontrado para "
                            f"UF: {row['uf']}, Município: {row['municipio']}"
                        )

                ids.append(int(loc_id))

                # Log de progresso a cada 1000 linhas
                if (index + 1) % 1000 == 0:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    logger.info(
                        f"Progresso: {index+1}/{len(df)} linhas | "
                        f"Tempo decorrido: {elapsed:.2f}s"
                    )

            except Exception as e:
                logger.error(f"Erro ao processar linha {index+1}: {str(e)}")
                ids.append(None)

        # 4. Adicionar IDs ao DataFrame e salvar
        df['id_loc'] = ids

        df.to_csv('../dados/tratados/localizacao_com_id.csv', index=False, encoding='utf-8')
        

        # 5. Estatísticas finais
        total_time = (datetime.now() - start_time).total_seconds()
        success_count = len([x for x in ids if x is not None])
        failure_count = len(df) - success_count

        logger.info("\n" + "="*50)
        logger.info("RELATÓRIO FINAL - LOCALIZAÇÃO")
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
