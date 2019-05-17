FROM python:3.6-alpine
MAINTAINER joep.vermaat@naturalis.nl

#USER root
COPY requirements.txt /tmp/
RUN apk update \
    && \
    apk add \
    py3-dateutil \
    py3-yaml \
    postgresql-dev \
    && apk add --virtual buildpsyco musl-dev \
    python3-dev \
    libffi-dev \
    gcc \
    git \
    && pip3 install --upgrade pip \
    && pip3 install --no-cache-dir psycopg2 psycopg2cffi==2.7.7 psycopg2-binary \
    && pip3 install --requirement /tmp/requirements.txt -U \
    && pip3 install --upgrade requests \
    && apk del buildpsyco
ADD . /usr/src
WORKDIR /usr/src

CMD ["percolator"]

