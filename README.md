#  Activity on Twitter with Apache Beam

- Prerequirement: 
* add 127.0.0.1 kafka in your /etc/hosts

## Build Docker Apache Beam Python SDK / Tweepy / Kafka-python
```
docker build . -t beam-python 
```

## Run Push Tweets Stream to File *(Docker/Python)*

```
cd data-loading/tweets-file
docker run -v "`pwd`/src:/src" --rm -it --name tweets-file beam-python
```
- Generate tweets.json in $PWD/src 
- move $PWD/src/tweets.json to /tmp

### Run CountTweet Pipeline *(bounded)*

- *direct-runner*

```
mvn clean compile exec:java -Dexec.mainClass=fr.ippon.beam.demo.CountTweet -Dexec.args="--input=/tmp/tweets.json" -Pdirect-runner
```

- *Flink local*

```
mvn clean compile exec:java -Dexec.mainClass=fr.ippon.beam.demo.CountTweet -Dexec.args="--input=/tmp/tweets.json" -Pflink-runner
```

### Run TopTweet Pipeline *(bounded)*

- *direct-runner*

```
mvn clean compile exec:java -Dexec.mainClass=fr.ippon.beam.demo.TopTweet -Dexec.args="--input=/tmp/tweets.json" -Pdirect-runner
```

- *Flink local*

```
mvn clean compile exec:java -Dexec.mainClass=fr.ippon.beam.demo.TopTweet -Dexec.args="--input=/tmp/tweets.json" -Pflink-runner
```

## Start Kafka cluster:
```
docker-compose up -d 
```

### Run TopTweet Pipeline with Kafka Topic *(unbounded)*

- *direct-runner*

```
mvn clean compile exec:java -Dexec.mainClass=fr.ippon.beam.demo.KafkaTopTweet -Pdirect-runner
```

- *Flink local*

```
mvn clean compile exec:java -Dexec.mainClass=fr.ippon.beam.demo.KafkaTopTweet -Pflink-runner
```

## TODO : Complete ELK Stack fork: https://github.com/deviantony/docker-elk

 

