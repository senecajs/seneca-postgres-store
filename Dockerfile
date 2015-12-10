FROM postgres

ADD scripts/dbschema.sql /docker-entrypoint-initdb.d
