# Análise dos Filmes da Marvel

## Introdução
Este notebook realiza a análise de dados dos filmes da Marvel utilizando Apache Spark e Pandas. A base de dados `Marvel_Movies.csv` contém informações detalhadas sobre receitas, críticas e orçamentos dos filmes do universo cinematográfico da Marvel.

## Configuração do Ambiente

### Instalação do Java
Para utilizar o Apache Spark, é necessário instalar o Java. Certifique-se de que a versão do Java é compatível com a versão do Spark que você está utilizando. Neste caso, estamos utilizando o Java 17.

A instalação é feita diretamente no ambiente do Colab com o comando abaixo:
```bash
!apt-get install openjdk-17-jdk-headless -qq > /dev/null
!java --version
```

### Instalação do Apache Spark
Baixamos e descompactamos a versão 3.5.1 do Apache Spark, que é compatível com o Java 17. 
```bash
!wget -q https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
!tar -xf spark-3.5.1-bin-hadoop3.tgz
!rm -rf spark-3.5.1-bin-hadoop3.tgz
```

### Configuração das Variáveis de Ambiente
Configuramos as variáveis de ambiente necessárias para o Spark. Essas variáveis indicam ao sistema onde encontrar as instalações do Java e do Spark.
```python
import os
os.environ['JAVA_HOME'] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ['SPARK_HOME'] = "/content/spark-3.5.1-bin-hadoop3"
```

### Instalação do FindSpark
O pacote `findspark` é instalado para facilitar a integração do Spark com o Jupyter Notebook.
```bash
!pip install -q findspark
```

### Inicialização do Spark
Inicializamos o Spark e criamos uma sessão para manipular os dados.
```python
import findspark
findspark.init()

from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").appName('Exercicio').getOrCreate()
```

## Carregamento e Pré-processamento dos Dados
Carregamos o arquivo CSV contendo os dados dos filmes da Marvel:
```python
marvel_movies_df = spark.read.csv("/content/Marvel_Movies.csv", header=True)
marvel_movies_df.show()
```

## Análise das Receitas

### Receita Mundial
Calculamos a receita mundial total e a média:
```python
from pyspark.sql import functions as F

receita_mundial = marvel_movies_df.select(F.sum('worldwide gross ($m)').alias("Receita mundial"))
media_receita_mundial = marvel_movies_df.select(F.mean('worldwide gross ($m)').alias("Média da receita mundial"))
receita_mundial.show()
media_receita_mundial.show()
```

### Receita Doméstica
Calculamos a receita doméstica total e a média:
```python
receita_domestica = marvel_movies_df.select(F.sum('domestic gross ($m)').alias("Receita doméstica"))
media_receita_domestica = marvel_movies_df.select(F.mean('domestic gross ($m)').alias("Média da receita doméstica"))
receita_domestica.show()
media_receita_domestica.show()
```

### Receita Internacional
Calculamos a receita internacional total e a média:
```python
receita_internacional = marvel_movies_df.select(F.sum('international gross ($m)').alias("Receita internacional"))
media_receita_internacional = marvel_movies_df.select(F.mean('international gross ($m)').alias("Média da receita internacional"))
receita_internacional.show()
media_receita_internacional.show()
```

## Visualização dos Dados
Para exibir os resultados de maneira mais visual, utilizamos gráficos:

### Receita Mundial
```python
import pandas as pd
import matplotlib.pyplot as plt

df_receita_mundial = receita_mundial.toPandas()
df_media_receita_mundial = media_receita_mundial.toPandas()

fig, axs = plt.subplots(2, 1, figsize=(10, 8))

df_receita_mundial.plot(kind='bar', ax=axs[0], legend=None)
axs[0].set_title('Receita Mundial')
axs[0].set_ylabel('Receita ($m)')
df_media_receita_mundial.plot(kind='bar', ax=axs[1], legend=None, color='orange')
axs[1].set_title('Média da Receita Mundial')
axs[1].set_ylabel('Receita ($m)')

plt.show()
```

### Receita Doméstica
```python
df_receita_domestica = receita_domestica.toPandas()
df_media_receita_domestica = media_receita_domestica.toPandas()

fig, axs = plt.subplots(2, 1, figsize=(10, 8))

df_receita_domestica.plot(kind='bar', ax=axs[0], legend=None)
axs[0].set_title('Receita Doméstica')
axs[0].set_ylabel('Receita ($m)')
df_media_receita_domestica.plot(kind='bar', ax=axs[1], legend=None, color='orange')
axs[1].set_title('Média da Receita Doméstica')
axs[1].set_ylabel('Receita ($m)')

plt.show()
```

### Receita Internacional
```python
df_receita_internacional = receita_internacional.toPandas()
df_media_receita_internacional = media_receita_internacional.toPandas()

fig, axs = plt.subplots(2, 1, figsize=(10, 8))

df_receita_internacional.plot(kind='bar', ax=axs[0], legend=None)
axs[0].set_title('Receita Internacional')
axs[0].set_ylabel('Receita ($m)')
df_media_receita_internacional.plot(kind='bar', ax=axs[1], legend=None, color='orange')
axs[1].set_title('Média da Receita Internacional')
axs[1].set_ylabel('Receita ($m)')

plt.show()
```

## Análise por Ano
Agrupamos as receitas por ano e plotamos:
```python
receitas_ano = marvel_movies_df.groupby('ano')[['receita_mundial_milhoes', 'receita_domestica_milhoes', 'receita_internacional_milhoes']].sum().reset_index()

plt.figure(figsize=(10, 6))
plt.bar(receitas_ano['ano'] - 0.2, receitas_ano['receita_mundial_milhoes'], width=0.2, label='Receita Mundial (milhões)')
plt.bar(receitas_ano['ano'], receitas_ano['receita_domestica_milhoes'], width=0.2, label='Receita Doméstica (milhões)')
plt.bar(receitas_ano['ano'] + 0.2, receitas_ano['receita_internacional_milhoes'], width=0.2, label='Receita Internacional (milhões)')
plt.xlabel('Ano')
plt.ylabel('Receita (milhões)')
plt.title('Receita Mundial, Doméstica e Internacional por Ano')
plt.legend()
plt.show()
```

## Análise de Pontuações
Analisamos a pontuação dos críticos e do público por filme:
```python
marvel_movies_df['percentual_criticos'] = marvel_movies_df['percentual_criticos'].str.rstrip('%').astype(float)
marvel_movies_df['percentual_publico'] = marvel_movies_df['percentual_publico'].str.rstrip('%').astype(float)
marvel_movies_df = marvel_movies_df.sort_values(by='ano')

plt.figure(figsize=(12, 6))
plt.bar(marvel_movies_df['filme'], marvel_movies_df['percentual_criticos'], color='blue', alpha=0.7, label='Críticos')
plt.xlabel('Filme')
plt.ylabel('Pontuação dos Críticos (%)')
plt.title('Pontuação dos Críticos por Filme')
plt.xticks(rotation=90)
plt.legend()
plt.tight_layout()
plt.show()

plt.figure(figsize=(12, 6))
plt.bar(marvel_movies_df['filme'], marvel_movies_df['percentual_publico'], color='green', alpha=0.7, label='Público')
plt.xlabel('Filme')
plt.ylabel('Pontuação do Público (%)')
plt.title('Pontuação do Público por Filme')
plt.xticks(rotation=90)
plt.legend()
plt.tight_layout()
plt.show()
```

## Percentual do Orçamento Recuperado
Analisamos o percentual do orçamento recuperado por filme:
```python
marvel_movies_df['percentual_orcamento_recuperado'] = marvel_movies_df['percentual_orcamento_recuperado'].str.rstrip('%').astype(float)
marvel_movies_df = marvel_movies_df.sort_values(by='ano')

plt.figure(figsize=(12, 6))
plt.bar(marvel_movies_df['filme'], marvel_movies_df['percentual_orcamento_recuperado'], color='purple', alpha=0.7, label='Percentual do Orçamento Recuperado')
plt.xlabel('Filme')
plt.ylabel('Percentual do Orçamento Recuperado (%)')
plt.title('Percentual do Orçamento Recuperado por Filme')
plt.xticks(rotation=90)
plt.legend()
plt.tight_layout()
plt.show()
```

## Conclusão
Esta análise fornece insights detalhados sobre o desempenho financeiro dos filmes da Marvel, além das avaliações críticas e do público. As visualizações ajudam a identificar tendências e comparações entre diferentes métr
