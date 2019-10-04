package io.inbot.eskotlindemo

import com.fasterxml.jackson.databind.ObjectMapper
import io.inbot.eskotlinwrapper.IndexDAO
import io.inbot.eskotlinwrapper.JacksonModelReaderAndWriter
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.client.create
import org.elasticsearch.client.crudDao
import org.elasticsearch.common.xcontent.XContentType

// A model class for our Thing
data class Thing(val name: String, val amount: Long = 42)

fun main() {
    create().use { client ->
        // lets use jackson to serialize our Thing, other serializers
        // can be supported by implementing ModelReaderAndWriter
        val modelReaderAndWriter = JacksonModelReaderAndWriter(Thing::class, ObjectMapper().findAndRegisterModules())

        // Create a Data Access Object
        val thingDao = client.crudDao("things", modelReaderAndWriter)

        // Any one remember their n-tier architectures ?
        val thingService = ThingService(thingDao)

        // do some stuff with it
        thingService.recreateTheIndex()

        thingService.indexingThings()

        thingService.upsertingThings()

        thingService.deletingThings()

        thingService.updatingThings()
    }
}


class ThingService(private val thingDao: IndexDAO<Thing>) {

    fun recreateTheIndex() {
        thingDao.deleteIndex()

        // first lets create the things index
        thingDao.createIndex {
            val settingsJson = this::class.java.getResource("/thing-settings.json").readText()
            source(settingsJson, XContentType.JSON)
        }
    }

    fun indexingThings() {
        val thing1 = Thing("my 1st thing", 1)

        thingDao.index("1", thing1)

        println(thingDao.get("1"))

        println(thingDao.get("idontexist") ?: "returns null because it doesn't exist")
    }

    fun upsertingThings() {
        try {
            thingDao.index("1", Thing("This thing won't work", 0))
        } catch (e: ElasticsearchStatusException) {
            println("we already had one of those and es returned ${e.status().status}")
        }
        thingDao.index(
            id = "1",
            obj = Thing("A different thing cause we can upsert", 0),
            create = false
        )
    }

    fun deletingThings() {
        thingDao.delete("1")
        println(thingDao.get("1"))
    }

    fun updatingThings() {
        thingDao.index("2", Thing("Another thing"))

        println(thingDao.get("2"))

        thingDao.update("2") { currentThing ->
            currentThing.copy(name = "an updated thing", amount = 666)
        }

        println(thingDao.get("2"))
    }
}

