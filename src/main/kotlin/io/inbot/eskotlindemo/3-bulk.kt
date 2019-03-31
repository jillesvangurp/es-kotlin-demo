package io.inbot.eskotlindemo

import com.fasterxml.jackson.databind.ObjectMapper
import io.inbot.eskotlinwrapper.IndexDAO
import io.inbot.eskotlinwrapper.JacksonModelReaderAndWriter
import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.client.crudDao
import org.elasticsearch.common.xcontent.XContentType

fun main() {
    RestHighLevelClient().use { client ->
        val thingDao = createDaoAndIndex(client)

        thingDao.bulk(bulkSize = 10, refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE) {
            for (i in 0.rangeTo(300)) {
                index("$i", Thing("thing #$i", i.toLong()))
            }

            delete("42")
        }
    }
}

private fun createDaoAndIndex(client: RestHighLevelClient): IndexDAO<Thing> {
    val thingDao = client.crudDao(
        "bulkthings",
        JacksonModelReaderAndWriter(Thing::class, ObjectMapper().findAndRegisterModules())
    )
    thingDao.deleteIndex()
    thingDao.createIndex {
        source(this::class.java.getResource("/thing-settings.json").readText(), XContentType.JSON)
    }
    return thingDao
}