package io.inbot.eskotlindemo

import org.apache.http.HttpHost
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient

fun main() {
    var restClientBuilder = RestClient.builder(HttpHost("localhost", 9200, "http"))
    val client = RestHighLevelClient(restClientBuilder)
    client.use {
        println(
            "ES is currently ${client.cluster().health(
                ClusterHealthRequest(),
                RequestOptions.DEFAULT
            ).status.name}"
        )
    }

    // We can do better ...

    // This works because we have default values for parameters where that makes sense if you are on localhost
    RestHighLevelClient().use { client ->
        // The RestHighLevelClient is a closable resource, so let kotlin close it
        println(
            "ES is currently ${client.cluster().health(
                ClusterHealthRequest(),
                RequestOptions.DEFAULT
            ).status.name}"
        )
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
        println(
            "ES is currently ${client.cluster().health(
                ClusterHealthRequest(),
                RequestOptions.DEFAULT
            ).status.name}"
        )
    }
}
