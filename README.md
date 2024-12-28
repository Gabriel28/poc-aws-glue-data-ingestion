# POC AWS GLUE DATA INGESTION

------

POC criada para ingestão de dados no glue criando um dataframe a partir de vários arquivos 
.json e escrevendo em apache parquet utilizando awsrangler.


## AWS Services utilizados:

-------
- S3
- Glue
- Athena para consumo dos dados.

## Teste Local

---
#### Clone o repo e crie uma venv
``
python -m venv venv
``

#### Instalar dependências
``
 pip install -r resources/req.txt
``

#### Config Envs
> Deve criar um arquivo .env igual ao envexemple e informar os parâmetros no arquivo

#### Run
``
python poc.py
``



