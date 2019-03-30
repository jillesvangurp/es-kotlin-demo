package io.inbot.eskotlindemo

import com.fasterxml.jackson.databind.ObjectMapper
import com.jillesvangurp.iterables.BlobIterable
import io.inbot.eskotlinwrapper.JacksonModelReaderAndWriter
import io.inbot.xmltools.PooledXmlParser
import io.inbot.xmltools.XPathExpressionCache
import io.inbot.xmltools.XpathBrowserFactory
import mu.KotlinLogging
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.client.crudDao
import org.elasticsearch.common.xcontent.XContentType
import java.io.FileInputStream


data class SimpleWikiPediaPage(
    val id: Long,
    val timestamp: String,
    val title: String,
    val description: String
)

val logger = KotlinLogging.logger { }
fun main() {
    val reader =
        BZip2CompressorInputStream(FileInputStream("/Users/jillesvangurp/Downloads/simplewiki-20170820-pages-meta-current.xml.bz2")).reader()

    // yes, it's over engineered ;-)
    val xpbf = XpathBrowserFactory(PooledXmlParser(20, 20), XPathExpressionCache(20, 10000, 1000, 20))

    val start = System.currentTimeMillis()
    RestHighLevelClient().use { client ->
        // lets use jackson to serialize our Thing, other serializers
        // can be supported by implementing ModelReaderAndWriter
        val modelReaderAndWriter =
            JacksonModelReaderAndWriter(SimpleWikiPediaPage::class, ObjectMapper().findAndRegisterModules())

        val articleDao = client.crudDao("simplewikipedia", modelReaderAndWriter)

        articleDao.deleteIndex()
        articleDao.createIndex {
            source(this::class.java.getResource("/simple-wikpedia.json").readText(), XContentType.JSON)
        }

        var count = 0
        articleDao.bulk(
            bulkSize = 50,
            refreshPolicy = WriteRequest.RefreshPolicy.NONE,
            retryConflictingUpdates = 0
        ) {
            BlobIterable(reader, "<page", "</page>").iterator()
                .asSequence()
                .take(1000)
                .map { parsePage(xpbf, it) }
                .filterNotNull()
                .forEach {
                    index(it.id.toString(), it)
                    if (count % 100 == 0) {
                        logger.info("processed $count articles")
                    }
                    count++
                }
        }
    }
    logger.info("Done in ${(System.currentTimeMillis() - start) / 1000} seconds!")

}

private fun parsePage(
    xpbf: XpathBrowserFactory,
    it: String?
): SimpleWikiPediaPage? {
    val browser = xpbf.browse(it)

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
                    description = description,
                    timestamp = timestamp
                )
            } else {
                null
            }
        } else {
            null
        }
    } catch (e: Exception) {
        logger.error(it)
        return null
    }
}