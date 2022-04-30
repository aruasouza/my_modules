import pandas as pd
import mysql.connector as mysql
import numpy as np

class connection:
    def __init__(self,database,password,host='localhost',user='root'):
        self.connection = mysql.connect(host = host,user = user,passwd = password,database = database)
        self.cursor = self.connection.cursor()

#É definida a classe Tabela, utilizada para inserir o dataframe em uma tabela de uma database MySQL
#Ao mesmo tempo é criado um objeto que permite que esses dados sejam facilmente extraídos
class Tabela:
    #Função construtora do objeto. Caso a tabela já exista na database os dados são selecionados
    #e armazenados em self.data se a tabela estiver vazia os dados são inseridos
    #Se a tabela não existir na database ela é criada e os dados são inseridos
    def __init__(self,cursor,nome,df):
        self.atributos = df.columns.values.tolist()
        self.nome = nome
        self.cursor = cursor
        try:
            comando = 'SELECT * FROM ' + self.nome + ';'
            self.cursor.execute(comando)
            self.data = self.cursor.fetchall()
            if self.data == []:
                empty = True
            else:
                empty = False
        except mysql.ProgrammingError:
            empty = True
            list_tipos = []
            for item in self.atributos:
                if type(df[item][0]) == np.int64:
                    list_tipos.append('INT')
                elif type(df[item][0]) == np.float64:
                    list_tipos.append('DOUBLE')
                elif type(df[item][0]) == str:
                    list_tipos.append('VARCHAR(50)')
            atributos_str = '('
            for i in range(len(self.atributos)):
                atributos_str += self.atributos[i] + ' ' + list_tipos[i] + ','
            atributos_str = atributos_str[:-1] + ')'
            comando = 'CREATE TABLE ' + nome + ' ' + atributos_str + ';'
            #print(comando)
            self.cursor.execute(comando)
        finally:
            if empty:
                for i in range(len(df)):
                    lista_dados = list(df.iloc[i])
                    nomes = '('
                    for nome in self.atributos:
                        nomes += nome + ','
                    nomes = nomes[:-1] + ')'
                    lista = '('
                    for dado in lista_dados:
                        if type(dado) != str:
                            lista += str(dado) + ','
                        else:
                            dado = '"' + dado + '"'
                            lista += dado + ','
                    lista = lista[:-1] + ')'
                    comando = 'INSERT INTO ' + self.nome + ' ' + nomes + ' VALUES ' + lista + ';'
                    #print(comando)
                    self.cursor.execute(comando)
                self.select()
    #Função que apaga os dados da tabela    
    def clear(self):
        comando = 'DELETE FROM ' + self.nome + ';'
        self.cursor.execute(comando)
        self.select()
    #Função que retorna os dados da tabela como uma lista de tuplas    
    def select(self):
        comando = 'SELECT * FROM ' + self.nome + ';'
        self.cursor.execute(comando)
        self.data = self.cursor.fetchall()
        return self.data
    #Função que retorna uma coluna de uma tabela como uma lista    
    def coluna(self,nome):
        comando = 'SELECT ' + nome + ' FROM ' + self.nome + ';'
        self.cursor.execute(comando)
        col = self.cursor.fetchall()
        lista = list()
        for i in range(len(col)):
            lista.append(col[i][0])
        return lista
    #Função que retorna várias colunas como um dicionário de listas
    def get_colunas(self,lista):
        mapa = dict()
        for item in lista:
            mapa[item] = self.coluna(item)
        return mapa
    #Função que retorna todos od dados como um dataframe
    def get_dataframe(self):
        mapa = dict()
        for item in self.atributos:
            mapa[item] = self.coluna(item)
        df = pd.DataFrame(mapa)
        return df