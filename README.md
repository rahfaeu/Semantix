# Desafio Técnico Semantix

## Questões

- Qual o objetivo do comando cache em Spark?

O objetivo do cache é otimizar iteratividade com os dados intermediários, resultado do processamento anteriores, evitando a repetição de leitura e transformação dos dados processados inicialmente, o dados que sofrem ações (RDD) são carregados em outro objeto (RDD) na memória, que possui um tamanho menor que o arquivo original, e podem ser reutilizados sempre que necessário, sem a necessidade de reavaliar o arquivo inicial, ler, e transformar novamente.
Um RDD que é acessado diversas vezes pela aplicação e não possui cash, é reavaliado a cada ação que recebe, reprocessando os dados desde sua origem.

- O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê?
Pelo fato de o spark trabalhar com o processamento direcionando os resultados para a memória, estendendo ao modelo do mapreduce que processa os dados, lê e escreve os resultados em disco, e muitas vezes os resultados lidos e escritos em discos precisarão ser reutilizados através de funções iterativas, o que gera um sobrecarga que torna o processo menos performático que o quando processado e armazenados em memória.
