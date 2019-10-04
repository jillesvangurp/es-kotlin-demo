package io.inbot.eskotlindemo

import com.fasterxml.jackson.databind.ObjectMapper
import com.jillesvangurp.iterables.BlobIterable
import io.inbot.eskotlinwrapper.JacksonModelReaderAndWriter
import io.inbot.xmltools.PooledXmlParser
import io.inbot.xmltools.XPathExpressionCache
import io.inbot.xmltools.XpathBrowserFactory
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.newFixedThreadPoolContext
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.client.create
import org.elasticsearch.client.crudDao
import org.elasticsearch.common.xcontent.XContentType
import java.io.FileInputStream
import java.util.concurrent.atomic.AtomicLong


data class SimpleWikiPediaPage(
    val id: Long,
    val timestamp: String,
    val url: String,
    val title: String,
    val description: String,
    val text: String
)


val logger = KotlinLogging.logger { }
fun main() {


    // yes, it's over engineered but it works ;-)
    val xpbf = XpathBrowserFactory(PooledXmlParser(20, 20),
        XPathExpressionCache(20, 10000, 1000, 20))

    val start = System.currentTimeMillis()
    create(useSniffer = false).use { client ->
        val articleDao = client.crudDao("simplewikipedia-sample", // I did one earlier ...
            JacksonModelReaderAndWriter(SimpleWikiPediaPage::class, ObjectMapper().findAndRegisterModules())
        )

        articleDao.deleteIndex()
        articleDao.createIndex {
            source(this::class.java.getResource("/simple-wikpedia.json").readText(), XContentType.JSON)
        }

        runBlocking {
            var count=0
            articleDao.bulkAsync(bulkSize = 500) {
                // download your own copy here (~220 MB):
                // https://dumps.wikimedia.org/simplewiki/latest/simplewiki-latest-pages-meta-current.xml.bz2
                val file = "/Users/jillesvangurp/Downloads/simplewiki-latest-pages-meta-current.xml.bz2"
                val reader =
                    BZip2CompressorInputStream(FileInputStream(file)).reader()

                BlobIterable(reader, "<page", "</page>").iterator()
                    .asSequence()
                    .map { parsePage(xpbf, it) }
                    .filterNotNull()
                    .take(1000)
                    .forEach {
                        count++
                        index(it.id.toString(), it)
                        if(count % 500 == 0) {
                            logger.info { "processed $count pages" }
                        }
                    }
            }
        }
        logger.info("Done indexing in ${(System.currentTimeMillis() - start) / 1000} seconds!")
        logger.info {  "found ${articleDao.search { }.totalHits} documents in the index" }
    }

}

private fun parsePage(
    xpbf: XpathBrowserFactory,
    rawText: String?
): SimpleWikiPediaPage? {
//    println(rawText)
    val browser = xpbf.browse(rawText)

    return try {
        val title = browser.getString("/page/title").orElse("")
        if (!title.contains(":")) {
            val id = browser.getString("/page/revision/id").orElse("")
            val text = browser.getString("/page/revision/text").orElse("")
            if (!(text.startsWith("#") || text.startsWith("["))) {
                val timestamp = browser.getString("/page/revision/timestamp").orElse("")

                val description = text.reader().readLines()
                    .firstOrNull { !(it.startsWith("[") || it.startsWith("{") || it.startsWith("#")) } ?: ""


                SimpleWikiPediaPage(
                    id = id.toLong(),
                    title = title,
                    url= "https://simple.wikipedia.org/wiki/${title.replace(" ","_")}",
                    description = description,
                    text = text,
                    timestamp = timestamp
                )
            } else {
                null
            }
        } else {
            null
        }
    } catch (e: Exception) {
        logger.error(rawText)
        return null
    }
}