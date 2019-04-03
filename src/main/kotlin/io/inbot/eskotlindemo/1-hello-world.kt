package io.inbot.eskotlindemo

import org.apache.http.HttpHost
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient

fun main() {
    val restClientBuilder = RestClient.builder(
        HttpHost("localhost", 9200, "http")
    )
    val restHighLevelClient = RestHighLevelClient(restClientBuilder)

    restHighLevelClient.use {
        // The RestHighLevelClient is a closable resource, so let kotlin close it after we use it
        println("ES is ${restHighLevelClient.cluster().health(ClusterHealthRequest(),RequestOptions.DEFAULT).status}")
    }

    // We can do better ...
    RestHighLevelClient().use { client ->
        // This works because we have sane default values for parameters that work with localhost:9200
        println("ES is ${client.cluster().health(ClusterHealthRequest(),RequestOptions.DEFAULT).status}")
    }

    RestHighLevelClient(
        host = "localhost",
        port = 9200,
        https = false,
        user = null,
        password = null
    ).use { client ->
        // but you can override the default parameters of course
        println("ES is ${client.cluster().health(ClusterHealthRequest(),RequestOptions.DEFAULT).status}")
    }
}
