import psycopg2

class Interface_db_postgre:

    user = ""
    password = ""
    host = ""
    database = ""
    
    def __init__(self, user, password, host, database):
        """Construtor da classe Interface_db_postgre utilizando o modulo psycopg2

        Args:
            user (string): usuario para conexao ao banco de dados
            senha (string): senha para acesso ao banco de dados
            host (string): endereco do host
            banco (string): nome do banco de dados
        """
        
        try:
            self.user = user
            self.password = password
            self.host = host
            self.database = database
        except Exception as e:
            print("Error cannot start Postgre: ", str(e))

    def connect(self):
        """Método para conectar ao banco de dados

        Returns:
            con : conector sql
            cursor : cursor para leitura do banco de dados
        """
        try:
            con = psycopg2.connect(user=self.user, password=self.password, host=self.host, database=self.database)
            cursor = con.cursor()
            return con, cursor
        except Exception as e:
            print("Error cannot connect to Postgre: ", str(e))
    
    def disconnect(self, con, cursor):
        """Método para desconectar do banco de dados

        Args:
            con : conector sql
            cursor : cursor para leitura do banco de dados
        """
        try:
            cursor.close()
            con.commit()
            con.close()
        except Exception as e:
            print("Error cannot disconnect to Postgre: ", str(e))

    def select(self, query):
        """Método para pesquisa no banco de dados

        Args:
            query (string): query para pesquisa dados no banco
        Returns:
            cursor.fetchall(): retorna tudo que for encontrado pelo cursor
        """
        try:
            con, cursor = self.connect()
            cursor.execute(query)
            return cursor.fetchall()
        except Exception as e:
            print("Error in def select: ", str(e))
        finally:
            self.disconnect(con, cursor)

    def execute(self, query):
        """Método para inserir, alterar ou deletar um dado no banco de dados

        Args:
            query (string): query para buscar dados no banco de dados
        Returns:
            result: retorna o resultado da operação
        """
        try:
            con, cursor = self.connect()
            result = cursor.execute(query)
            con.commit()
            return result
        except Exception as e:
            print("Error in def execute: ", str(e))
        finally:
            self.disconnect(con, cursor)                 
            
def get_db_info():
    """Funcao com as informacoes necessarias para o acesso ao banco de dados
    Returns:
        user = usuario do SQL
        password = senha de conexao do SQL
        host = endereco do banco de dados SQL
        database = banco de dados que sera utilizado
    """
    try:
        user = "postgres"
        password = "Michel2009!"
        host = "127.0.0.1"
        database = "telecom"
        return user, password, host, database
    except Exception as e:
        print("Error in def get_db_info: ", str(e))