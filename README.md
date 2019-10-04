# Desafio Técnico Semantix

## Questões

### Qual o objetivo do comando cache em Spark?

O objetivo do cache é permitir a reutilização dos dados já transformados, melhorando a performance em ações de iteratividades, evitando a repetição de leitura e transformação dos dados processados inicialmente, o dados que sofrem ações (RDD) são carregados em outro objeto (RDD) na memória, que possui um tamanho menor que o arquivo original, e podem ser reutilizados sempre que necessário, demandando menos processamento.
Um RDD que é acessado diversas vezes pela aplicação e não possui cash, é reavaliado a cada ação que recebe, reprocessando os dados desde sua origem.

### O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê?

Pelo fato de o spark trabalhar com o processamento em memória, estendendo ao modelo do mapreduce que processa os dados, lê e escreve os resultados em disco, e muitas vezes os resultados lidos e escritos em discos precisarão ser reutilizados através de funções iterativas, o que gera uma sobrecarga que torna o processo menos performático que quando processado e armazenados em memória.

### Qual é a função do SparkContext ?

Ser uma interface entre o spark e a aplicação em execução, é um objeto que permite a utilização dos recursos do spark dentro da aplicação, como funções, ação de transformação e operações.

### Explique com suas palavras o que é Resilient Distributed Datasets (RDD).

Um RDD é um objeto em memoria que simula um conjunto de dados que o spark irá processar de forma distribuída, é imutável, no sofre alteção em sua estrutura original, sempre que é submetido a alguma transformação uma replica é gerada com os resultados da ação de transformação.
Um RDD é tolerante a falhas, e pode ser distribuido para o processamento por diversos workers(nodes) sem perder dados.

### GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?
O **GroupByKey** trafega mais dados pela rede pois ele envia uma lista de valores sem agregação para uma partição, já o **ReduceByKey** agrega os dados em chave valor, enviando apenas um saída para cada valor distinto. 

## Explique o que o código Scala abaixo faz.


~~~Scala
01   val textFile = sc.textFile("hdfs://...")
02   val counts = textFile.flatMap(line => line.split(" "))
03       .map(word => (word, 1))
04       .reduceByKey(_ + _)
05   counts.saveAsTextFile("hdfs://...")
~~~

> (01) **Na primeira linha do código**
> O objeto textFile (RDD) recebe os dados buscados no arquivo em disco através do path declarado.

> (02) **Na segunda linha do código**
> É feita a contagem total de palavras, identificando os limites de cada paralavra por um espaço.

> (03) **Na terceira linha do código**
> É aplicada a função Map que atribui a quantidade de 1 (uma) unidade para cada palavra contabilizada na contagem.

> (04) **Na quarta linha do código**
> É aplicada a função Reduce para agregar as palavras iguais e somar suas quantidades em um único par (chave-valor).

> (05) **Na quinta linha do código**
> O resultado é gravado em um aquivo de texto e salva em disco.

