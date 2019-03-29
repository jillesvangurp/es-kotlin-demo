package io.inbot.eskotlindemo

import com.fasterxml.jackson.databind.ObjectMapper
import io.inbot.eskotlinwrapper.IndexDAO
import io.inbot.eskotlinwrapper.JacksonModelReaderAndWriter
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.client.crudDao
import org.elasticsearch.common.xcontent.XContentType

// A model class for our Thing
data class Thing(
    val name: String,
    val amount: Long=42
)

class ThingService(val thingDao: IndexDAO<Thing>) {
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

        // not there yet, so it returns null
        println(thingDao.get("1")?.name ?: "No such thing")

        // so lets fix that
        thingDao.index("1", thing1)

        // and now it is there
        println(thingDao.get("1"))
    }

    fun upsertingThings() {
        try {
            // this won't work
            thingDao.index("1", Thing("An different thing", 0))
        } catch (e: ElasticsearchStatusException) {
            println("we already had one of those and es returned ${e.status().status}")
        }
        // upserts work
        thingDao.index("1", Thing("An different thing", 0), create = false)
    }

    fun deletingThings() {
        thingDao.delete("1")
        println(thingDao.get("1"))
    }

    fun updatingThings() {
        thingDao.index("2", Thing("Another thing"))

        // 42
        println(thingDao.get("2"))

        thingDao.update("2") { currentThing ->
            // we fetched the current thing and returning a modified version
            currentThing.copy(amount=666)
        }

        // 666
        println(thingDao.get("2"))
    }

}

fun main() {
    RestHighLevelClient().use { client ->
        // lets use jackson to serialize our Thing, other serializers
        // can be supported by implementing ModelReaderAndWriter
        val modelReaderAndWriter
                = JacksonModelReaderAndWriter(Thing::class, ObjectMapper().findAndRegisterModules())

        val thingDao = client.crudDao("things", modelReaderAndWriter)

        val thingService = ThingService(thingDao)

        thingService.recreateTheIndex()

        thingService.indexingThings()

        thingService.upsertingThings()

        thingService.deletingThings()

        thingService.updatingThings()
    }
}