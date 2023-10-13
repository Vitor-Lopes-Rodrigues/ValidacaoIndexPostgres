import psycopg2
from datetime import datetime, timedelta
import time


def verificar_indices(conexao, dias_limite=90, arquivo_saida='/CaminhoDaMaquina/indices_nao_escaneados.txt'):
    print("Conexão estabelecida.")

    while True:
        consulta_sql = """
        SELECT
            schemaname,
            relname AS table_name,
            indexrelname AS index_name,
            idx_scan
        FROM pg_stat_user_indexes;
        """

        with conexao.cursor() as cursor:
            cursor.execute(consulta_sql)
            resultados = cursor.fetchall()

            indices_nao_usados = []

            for resultado in resultados:
                schema, tabela, index_name, idx_scan = resultado
                if idx_scan == 0 and not foi_usado_recentemente(conexao, schema, tabela, index_name, dias_limite):
                    indices_nao_usados.append((schema, tabela, index_name))

            if indices_nao_usados:
                with open(arquivo_saida, 'a') as arquivo:
                    arquivo.write(f"\nÍndices não usados nos últimos {dias_limite} dias\nSalvo a cada 2 semanas,({datetime.now()}).\n")
                    for schema, tabela, index_name in indices_nao_usados:
                        arquivo.write(f"Tabela: {tabela}, Índice: {index_name}, Schema: {schema}\n")
                print("Dados salvos no arquivo.")

        # Intervalo entre as verificações (por exemplo, a cada 2 semanas)
        intervalo_dias = 14  # 2 semanas
        intervalo_segundos = intervalo_dias * 24 * 60 * 60

        print(f"Aguardando próximo intervalo de verificação ({datetime.now()}).")
        time.sleep(intervalo_segundos)


def foi_usado_recentemente(conexao, schema, tabela, index_name, dias_limite):
    consulta_sql = """
    SELECT
        idx_scan
    FROM pg_stat_user_indexes
    WHERE
        schemaname = %s
        AND relname = %s
        AND indexrelname = %s;
    """

    with conexao.cursor() as cursor:
        cursor.execute(consulta_sql, (schema, tabela, index_name))
        resultado = cursor.fetchone()

        if resultado and resultado[0] > 0:
            # Se o índice foi usado nos últimos dias_limite dias, retorna True
            return True

    return False


if __name__ == "__main__":
    try:
        conexao = psycopg2.connect(
            host="host",
            database="database",
            user="user",
            password="password"
        )
        verificar_indices(conexao)
    except Exception as e:
        print(f"Erro ao conectar: {e}")
    finally:
        conexao.close()
        print("Conexão encerrada.")
