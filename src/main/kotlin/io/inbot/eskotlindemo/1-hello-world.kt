package io.inbot.eskotlindemo

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestHighLevelClient

fun main() {
    // This works because we have default values for parameters where that makes sense
    RestHighLevelClient().use { client ->
        // The RestHighLevelClient is a closable resource, so let kotlin close it
        val status = client.cluster().health(ClusterHealthRequest(), RequestOptions.DEFAULT)
        println("ES is currently ${status.status.name}")
    }

    // you can override the default parameters of course
    RestHighLevelClient(host = "localhost", port = 9200).use { client ->
        // The RestHighLevelClient is a closable resource, so let kotlin close it
        val status = client.cluster().health(ClusterHealthRequest(), RequestOptions.DEFAULT)
        println("ES is currently ${status.status.name}")
    }

}
