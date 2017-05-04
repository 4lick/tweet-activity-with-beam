FROM python:2.7
MAINTAINER Alick Paul <alick.paul@gmail.com>

# Git install
RUN apt-get install -y git

# Apache Beam Download and install
RUN cd /tmp/ \
    && git clone https://github.com/apache/beam.git \
    && cd beam/sdks/python/ \
    && python setup.py sdist \
    && pip install --upgrade pip \
    && pip install dist/apache-beam-*.tar.gz \
    && pip install tweepy \
    && pip install kafka-python

# ADD kafka Host
#RUN echo "`/sbin/ip route|awk '/default/ { print $3 }'` kafka-external" >> /etc/hosts 
    
# Exec script
VOLUME /src
WORKDIR /src/

CMD [ "python", "./basic.py" ]

