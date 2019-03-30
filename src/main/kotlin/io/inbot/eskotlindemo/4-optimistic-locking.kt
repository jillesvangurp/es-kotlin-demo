package io.inbot.eskotlindemo

import com.fasterxml.jackson.databind.ObjectMapper
import io.inbot.eskotlinwrapper.JacksonModelReaderAndWriter
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.client.crudDao
import org.elasticsearch.common.xcontent.XContentType

fun main() {
    RestHighLevelClient().use { client ->

        val thingDao = client.crudDao("optimistically",
            JacksonModelReaderAndWriter(Thing::class, ObjectMapper().findAndRegisterModules())
        )
        thingDao.deleteIndex()
        thingDao.createIndex {
            source(this::class.java.getResource("/thing-settings.json").readText(), XContentType.JSON)
        }

        thingDao.index("1", Thing("A Thing"))
        try {
            thingDao.index("1", Thing("A Conflicting Thing"))
        } catch (e: ElasticsearchStatusException) {
            println("it conflicted: ${e.status().status} ${e.message}")
        }
        thingDao.index("1", Thing("A Changed Thing Blindly overwritten"), create = false)
        println(thingDao.get("1")?.name)

        val (_, getResponse) = thingDao.getWithGetResponse("1")
            ?: throw IllegalStateException("it should be there")

        thingDao.index(
            "1",
            Thing("Trust me, I know what I'm doing"),
            create = false,
            seqNo = getResponse.seqNo,
            primaryTerm = getResponse.primaryTerm
        )

        try {
            thingDao.index(
                "id",
                Thing("Trust me, I know what I'm doing"),
                create = false,
                seqNo = 666, // !!!
                primaryTerm = getResponse.primaryTerm
            )
        } catch (e: ElasticsearchStatusException) {
            println("it conflicted: ${e.status().status} ${e.message}")
        }

        thingDao.update("1", maxUpdateTries = 5) { currentVersion ->
            currentVersion.copy(name="Update with retries makes this painless")
        }

        // we can also do this in bulk
        thingDao.bulk(
            bulkSize = 3,
            refreshPolicy = WriteRequest.RefreshPolicy.WAIT_UNTIL,
            // the default handler actually takes care of retries but where's the fun in that :-)
            itemCallback = { operation, resp ->
                if (resp.isFailed) {
                    logger.info(":-( ${resp.id} : ${resp.failureMessage}")
                } else {
                    logger.info(":-) ${resp.id} ${resp.opType.name}")
                }
            }
        ) {
            index("666", Thing("This is Fine"))
            index("666", Thing("Not Fine"))
            index("666", Thing("This is Fine again"), create = false)
            // if we have an original, we can try to update it
            update("666", 666, 666, Thing("This is Not Fine at all")) {
                it.copy("Changed Name")
            }
            // or just fetch the latest and use the correct version
            getAndUpdate("666") {
                it.copy("Fine")
            }

            delete("666")
        }
    }
}