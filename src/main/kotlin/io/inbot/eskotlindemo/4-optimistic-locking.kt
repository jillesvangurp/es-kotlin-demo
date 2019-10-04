package io.inbot.eskotlindemo

import com.fasterxml.jackson.databind.ObjectMapper
import io.inbot.eskotlinwrapper.IndexDAO
import io.inbot.eskotlinwrapper.JacksonModelReaderAndWriter
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.client.create
import org.elasticsearch.client.crudDao
import org.elasticsearch.common.xcontent.XContentType

fun main() {
    create().use { client ->

        val thingDao = createDaoAndIndex(client)

        // Lets do some conflicting stuff ...
        thingDao.index("1", Thing("A Thing"))
        try {
            thingDao.index("1", Thing("A Conflicting Thing"))
        } catch (e: ElasticsearchStatusException) {
            println("it conflicted: ${e.status().status} ${e.message}")
        }

        thingDao.index("1", Thing("A Changed Thing Blindly overwritten"), create = false)

        println(thingDao.get("1"))



        // now do it properly with OPTIMISTIC locking
        val (theOldThing, getResponse) =
            thingDao.getWithGetResponse("1") ?: throw IllegalStateException("it should be there")

        println(theOldThing)

        // only index if the seq_no matches
        thingDao.index(
            "1",
            Thing("Trust me, I know what I'm doing"),
            create = false,
            seqNo = getResponse.seqNo,
            primaryTerm = getResponse.primaryTerm
        )

        // this is what happens if you have the wrong seqNo
        try {
            thingDao.index(
                "id",
                Thing("Don't trust me, I have no clue what I'm doing"),
                create = false,
                seqNo = 666, // !!!
                primaryTerm = getResponse.primaryTerm
            )
        } catch (e: ElasticsearchStatusException) {
            println("it conflicted: ${e.status().status} ${e.message}")
        }

        // and this does the right thing for you
        thingDao.update("1", maxUpdateTries = 5) { currentVersion ->
            currentVersion.copy(name="Update with retries makes this painless")
        }

        thingDao.index("666", Thing("A thing"))
        thingDao.index("999", Thing("A thingy"))
        thingDao.index("42", Thing("Another thing"))
        thingDao.index("xxx", Thing("we will bulk delete this thing"))


        // we can also do this in bulk
        thingDao.bulk(
            bulkSize = 3,
            refreshPolicy = WriteRequest.RefreshPolicy.WAIT_UNTIL,
            // the default handler actually takes care of retries but where's the fun in that :-)
            itemCallback = { _, resp ->
                if (resp.isFailed) {
                    println(":-( ${resp.id} : ${resp.failureMessage}")
                } else {
                    println(":-) ${resp.id} ${resp.opType.name}")
                }
            }
        ) {
            index("666", Thing("Not Fine"))
            index("666", Thing("This is Fine again"), create = false)

            // if we have an original, we can try to update it
            update("999", 100, 3, Thing("This is Not Fine at all")) {
                it.copy(name = "Changed Thing")
            }

            // or just fetch the latest and use the correct version
            getAndUpdate("42") {
                it.copy(name="Everything is Fine")
            }

            delete("xxx")
        }

        println(thingDao.get("666"))
        println(thingDao.get("999"))
        println(thingDao.get("42"))
        println(thingDao.get("xxx"))
    }
}

private fun createDaoAndIndex(client: RestHighLevelClient): IndexDAO<Thing> {
    val thingDao = client.crudDao(
        "optimistically",
        JacksonModelReaderAndWriter(Thing::class, ObjectMapper().findAndRegisterModules())
    )
    thingDao.deleteIndex()
    thingDao.createIndex {
        source(this::class.java.getResource("/thing-settings.json").readText(), XContentType.JSON)
    }
    return thingDao
}