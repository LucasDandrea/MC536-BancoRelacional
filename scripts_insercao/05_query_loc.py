import psycopg2
from psycopg2 import OperationalError

# Configurações de conexão (mantendo seu padrão)
dbname = ""
user = ""
host = ""
password = ""

def conectar_banco():
    try:
        # Substitua estas variáveis pelas suas credenciais
        connection = psycopg2.connect(
            dbname=dbname,
            user=user,
            host=host,
            password=password
        )
        print("Conexão ao PostgreSQL bem-sucedida!")
        return connection
    except OperationalError as e:
        print(f"Erro ao conectar ao PostgreSQL: {e}")
        return None


def executar_query(connection):
    #Query para adicionar manualmente as localizações que não foram declaradas pelo dataset de escola por enem, mas estão presentes no vinculo empregatício dos alunos do IFB
    query = """
    INSERT INTO localizacao (uf, municipio) VALUES
    ('MA', 'Tufilândia'),
    ('GO', 'Flores de Goiás'),
    ('PA', 'Santa Maria das Barreiras'),
    ('MA', 'Santo Amaro do Maranhão'),
    ('MT', 'Serra Nova Dourada');
    """

    try:
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        print("Query executada com sucesso! Registros inseridos.")
    except Exception as e:
        print(f"Erro ao executar a query: {e}")
        connection.rollback()
    finally:
        if cursor:
            cursor.close()


def main():
    # Conectar ao banco
    conn = conectar_banco()

    if conn is not None:
        try:
            # Executar a query
            executar_query(conn)
        finally:
            # Fechar a conexão
            conn.close()
            print("Conexão com PostgreSQL foi fechada.")


if __name__ == "__main__":
    main()
