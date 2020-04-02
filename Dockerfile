FROM python:3.6.3

MAINTAINER Dan Napierski (ISI) <dan.napierski@toptal.com>

# Create app directory
WORKDIR /gaia-knowledge-graph/src/

# Install app dependencies
RUN apt-get upgrade && apt-get update && apt-get -y install apt-utils && apt-get -y install unzip git nano tree
RUN pip install --upgrade pip

COPY . .
RUN pip install -r requirements.txt

CMD [ "/bin/bash", "" ]

