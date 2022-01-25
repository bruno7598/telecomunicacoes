from cassandra.cluster import Cluster

class Interface_db_cassandra:
    
    keyspace = ""
    cluster = None
    session = None
    cassandra_host = ""
    
    def __init__(self, keyspace, cassandra_host="127.0.0.1",):
        """Construtor da classe Interface_db_cassandra utilizando o modulo casandra.cluster
        
        Args:
            keyspace (string): nome do banco de dados
        """

        try:
            self.cassandra_host = cassandra_host
            self.keyspace = keyspace
        except Exception as e:
            print("Error cannot start Cassandra: ", str(e))
            
    def connect(self):
        """Método para conectar ao banco de dados
        
        Args:
            cluster : cluster do Cassandra
        Returns:
            session : session do Cassandra
        """
        
        try:
            if self.cluster == None:
                self.cluster = Cluster([self.cassandra_host], port=9042)
                self.session = self.cluster.connect(self.keyspace)
            if self.session == None:
                raise Exception("Error cannot connect to Cassandra:")
            return self.session
        except Exception as e:
            print("Error in def connect: ", str(e))
               
    def select(self, query):
        """Método para pesquisa na keyspace

        Args:
            query (string): query para pesquisar dados na keyspace
        Returns:
            data: retorna os dados da keyspace
        """
        try:
            data = self.connect().execute(query)
            data = self.fetchall(data)
            return data
        except Exception as e:
            print("Error in def select: ", str(e))
                
    def fetchall(self, data):
        """Método para trazer uma lista com todos os dados da keyspace
            
        Returns:
           list(): retorna uma lista com os dados da keyspace
        """
        try:
            list = []
            for i in data:
                list.append(i)
            return list
        except Exception as e:
            print("Error in def fetchall: ", str(e))
            
    def execute(self, query):
        """Método para inserir, alterar ou deletar um dado na keyspace

        Args:
            query (string): query para buscar dados na keyspace             
        """

        try:
            self.connect().execute(query)
        except Exception as e:
            print("Error in def execute: ", str(e))
            
def get_db_info():
    """Funcao com as informacoes necessarias para o acesso ao banco de dados
    
    Returns:
        keyspace = banco de dados que sera utilizado
    """

    try:
        keyspace = "telecom"
        return keyspace
    except Exception as e:
        print("Error in def get_db_info: ", str(e))