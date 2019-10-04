# Desafio Técnico Semantix

## Questões

- **Qual o objetivo do comando cache em Spark?**

O objetivo do cache é otimizar iteratividade com os dados intermediários, resultado do processamento anteriores, evitando a repetição de leitura e transformação dos dados processados inicialmente, o dados que sofrem ações (RDD) são carregados em outro objeto (RDD) na memória, que possui um tamanho menor que o arquivo original, e podem ser reutilizados sempre que necessário, sem a necessidade de reavaliar o arquivo inicial, ler, e transformar novamente.
Um RDD que é acessado diversas vezes pela aplicação e não possui cash, é reavaliado a cada ação que recebe, reprocessando os dados desde sua origem.

- **O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê?**

Pelo fato de o spark trabalhar com o processamento em memória, estendendo ao modelo do mapreduce que processa os dados, lê e escreve os resultados em disco, e muitas vezes os resultados lidos e escritos em discos precisarão ser reutilizados através de funções iterativas, o que gera uma sobrecarga que torna o processo menos performático que quando processado e armazenados em memória.

- **Qual é a função do SparkContext ?**
Ser uma interface entre o spark e a aplicação em execução, é um objeto que permite a utilização dos recursos do spark dentro da aplicação, como funções, ação de transformação e operações.

- **Explique com suas palavras o que é Resilient Distributed Datasets (RDD).**
Um RDD é um objeto em memoria que simula um conjunto de dados o qual o spark irá processar, é imutável, no sofre alteção em sua estrutura original, sempre que é submetido a alguma transformação uma replica é gerada com os resultados da ação de transformação.
Um RDD é tolerante a falhas, e pode ser distribuido para o processamento por diversos workers(nodes) sem perder dados.

- **GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?**
Pelo fato de o **GroupByKey** enviar em grupo os diversos itens que possuem a mesma correspondência na chave dos valores/dados para os workers, enquanto que o **ReduceByKey** agrega as chaves e soma os valores de mesma correpondencia de indice, retornando apenas uma saída para cada chave-valor distinto, pois foram agregados antes da saída, dessa forma dados mais atômicos são transmitidos pela rede e coletados pelos workers.

## Explique o que o código Scala abaixo faz.


~~~Scala
val textFile = sc.textFile("hdfs://...")
val counts = textFile.flatMap(line => line.split(" "))
    .map(word => (word, 1))
    .reduceByKey(_ + _)
counts.saveAsTextFile("hdfs://...")
~~~

> Na primeira linha do código ~~~ val textFile = sc.textFile("hdfs://...") ~~~
> O objeto textFile recebe os dados buscados no arquivo através do path declarado.
