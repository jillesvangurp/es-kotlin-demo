# Demo for Es Kotlin Wrapper

Some simple examples of using the [Es Kotlin Wrapper](https://github.com/jillesvangurp/es-kotlin-wrapper-client).

Note. this project may not reflect all the upstream changes.

## Usage

Start elasticsearch. You can use docker to do this:

```bash
docker run -e "discovery.type=single-node" -p "9200:9200" docker.elastic.co/elasticsearch/elasticsearch-oss:7.4.0
```

Then simply run the main methods from your IDE.