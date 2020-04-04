# Query-Flink-KafkaStream

The project concerns answering some queries on a dataset made up of comments and queries. The queries have been resolved in Flink and Kafka Stream.


### Prerequisites

```
java 8
docker compose
```

### Install

To start the application:

- start the docker compose 
```
docker-compose.yml
```
in the path ``` src / main / java / docker / kafka / ```

- then start redis container
```
docker run --name redis:latest -d redis
```


## Authors

* **Valentino Perrone & Anthony Pusceddu & Federica Montesano**
