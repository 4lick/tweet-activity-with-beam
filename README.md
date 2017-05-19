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
mvn clean compile exec:java -Dexec.mainClass=fr.ippon.beam.demo.CountTweet -Dexec.args="--input=/tmp/tweets.json --output=/tmp/lead/tweets.csv" -Pdirect-runner
```

- *Flink local*

```
mvn clean compile exec:java -Dexec.mainClass=fr.ippon.beam.demo.CountTweet -Dexec.args="--input=/tmp/tweets.json --output=/tmp/lead/tweets.csv --runner=FlinkRunner" -Pflink-runner
```

### Run TopTweet Pipeline *(bounded)*

- *direct-runner*

```
mvn clean compile exec:java -Dexec.mainClass=fr.ippon.beam.demo.TopTweet -Dexec.args="--input=/tmp/tweets.json --output=/tmp/tweets/leadboard" -Pdirect-runner
```

- *Flink local*

```
mvn clean compile exec:java -Dexec.mainClass=fr.ippon.beam.demo.TopTweet -Dexec.args="--input=/tmp/tweets.json --runner=FlinkRunner --output=/tmp/tweets/leadboard" -Pflink-runner
```

## Start Kafka cluster:
```
docker-compose up -d 
```

## Tweets Stream to Kafka Topic *(Docker/Python)*

```
cd data-loading/tweets-kafka
docker run -v "`pwd`/src:/src" --rm -it --add-host kafka:172.17.0.1 --name tweets-kafka beam-python
```


### Run TopTweet Pipeline with Kafka Topic *(unbounded)*

- *direct-runner*

```
mvn clean compile exec:java -Dexec.mainClass=fr.ippon.beam.demo.KafkaTopTweet -Pdirect-runner
```

- *Flink local*

```
mvn clean compile exec:java -Dexec.mainClass=fr.ippon.beam.demo.KafkaTopTweet  -Pflink-runner
```

## TODO : Complete ELK Stack fork: https://github.com/deviantony/docker-elk

 

