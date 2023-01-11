#! /bin/bash
python3 -m pip install psycopg2-binary pandas
docker pull postgres
docker container run -d --name postgres -e POSTGRES_USER=admin -e POSTGRES_PASSWORD=admin -e POSTGRES_DB=postgres -p 5432:5432 postgres

