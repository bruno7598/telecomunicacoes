from cassandra.cluster import Cluster

class Interface_db_cassandra:
    
    keyspace = ""
    
    def __init__(self, keyspace):
        """ Construtor da classe Interface_db_cassandra  utilizando o modulo casandra.cluster
        
        Args:
            keyspace (string): nome do banco
        """
        try:
            self.keyspace = keyspace
        except Exception as e:
            print(str(e))
            
    def connector(self):
        """Função para conectar ao banco
        
        Args:
            cluster : cluster do Cassandra

        Returns:
            session : session do Cassandra
        """
        
        try:
            cluster = Cluster()
            session = cluster.connect(self.keyspace)
            return session
        except Exception as e:
            print(str(e))
               
    def select(self, query):
        """Função para uma busca na keyspace

        Args:
            query (string): query para buscar dados na keyspace
            
        Returns:
            data: retorna os dados da keyspace
        """
        try:
            data = self.connector().execute(query)
            data = self.fetchall(data)
            return data
        except Exception as e:
            print(str(e))
                
    def fetchall(self, data):
        """Função para trazer uma lista com todos os dados da keyspace
            
        Returns:
           list(): retorna uma lista com os dados da keyspace
        """
        try:
            list = []
            for i in data:
                list.append(i)
            return list
        except Exception as e:
            print(str(e))
            
    def execute(self, query):
        """Função para inserir, alterar ou deletar um dado na keyspace

        Args:
            query (string): query pronta para buscar dados na keyspace             
        """
        try:
            self.connector().execute(query)
        except Exception as e:
            print(str(e))
            
def get_db_info():
    """
    Funcao com as informacoes necessarias para o acesso ao banco de dados
    
    Returns:
        keyspace = banco de dados que sera utilizado
    """

    try:
        keyspace = "telecomunicacoes"
        return keyspace
    except Exception as e:
        print(str(e))