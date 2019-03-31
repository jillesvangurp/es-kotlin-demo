package io.inbot.eskotlindemo

import org.apache.http.HttpHost
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient

fun main() {
    var restClientBuilder = RestClient.builder(HttpHost("localhost", 9200, "http"))
    val restHighLevelClient = RestHighLevelClient(restClientBuilder)

    // The RestHighLevelClient is a closable resource, so let kotlin close it
    restHighLevelClient.use {
        println("ES is ${restHighLevelClient.cluster().health(ClusterHealthRequest(),RequestOptions.DEFAULT).status}")
    }

    // We can do better ...

    // This works because we have sane default values for parameters that work with localhost:9200
    RestHighLevelClient().use { client ->
        println("ES is ${client.cluster().health(ClusterHealthRequest(),RequestOptions.DEFAULT).status}")
    }

    // but you can override the default parameters of course
    RestHighLevelClient(
        host = "localhost",
        port = 9200,
        https = false,
        user = null,
        password = null
    ).use { client ->
        // The RestHighLevelClient is a closable resource, so let kotlin close it
        println("ES is ${client.cluster().health(ClusterHealthRequest(),RequestOptions.DEFAULT).status}")
    }
}
