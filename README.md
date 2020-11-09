# Juriscol Airflow


## Local

### Virtual env

python -m venv env
source env/bin/activate

### Requirements

Install them from `requirements.txt`:

    pip install -r requirements.txt

Install spacy:
    
    python -m spacy download es_core_news_sm


## COnfig MONGO
Create file called .env in folder juriscol with next varialbes

    - MONGO_USER=user
    - MONGO_PASS=pass
    - MONGO_HOST=host(ip:port)
    - MONGO_DB=db
