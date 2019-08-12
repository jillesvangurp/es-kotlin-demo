package io.inbot.eskotlindemo

import com.fasterxml.jackson.databind.ObjectMapper
import com.jillesvangurp.iterables.BlobIterable
import io.inbot.eskotlinwrapper.JacksonModelReaderAndWriter
import io.inbot.xmltools.PooledXmlParser
import io.inbot.xmltools.XPathExpressionCache
import io.inbot.xmltools.XpathBrowserFactory
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.newFixedThreadPoolContext
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.client.RestHighLevelClient
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
    RestHighLevelClient().use { client ->
        val articleDao = client.crudDao("simplewikipedia-sample", // I did one earlier ...
            JacksonModelReaderAndWriter(SimpleWikiPediaPage::class, ObjectMapper().findAndRegisterModules())
        )

        articleDao.deleteIndex()
        articleDao.createIndex {
            source(this::class.java.getResource("/simple-wikpedia.json").readText(), XContentType.JSON)
        }

//        var count = 0
        articleDao.bulk(
            bulkSize = 500,
            refreshPolicy = WriteRequest.RefreshPolicy.NONE,
            retryConflictingUpdates = 0
        ) {
            // our input
            val file = "/Users/jillesvangurp/Downloads/simplewiki-20170820-pages-meta-current.xml.bz2"
            val reader =
                BZip2CompressorInputStream(FileInputStream(file)).reader()

            runBlocking {
                val inputChannel = Channel<String?>()
                val outputChannel = Channel<SimpleWikiPediaPage?>()
                val pageReads = AtomicLong()
                val processCount = AtomicLong()
                val skipCount = AtomicLong()
                val indexCount = AtomicLong()
                launch {
                    BlobIterable(reader, "<page", "</page>").iterator()
                        .asSequence()
//                        .take(10000) // whole thing would take a few minutes
                        .forEach {
                            inputChannel.send(it)
                            if (pageReads.incrementAndGet() % 1000 == 0L) {
                                logger.info("read ${pageReads.get()} articles")
                            }
                        }
                    logger.info("DONE read ${pageReads.get()} articles")
                    inputChannel.close()
                }
                launch(newFixedThreadPoolContext(6, "pageprocessor")) {
                    for(page in inputChannel) {
                        val processed = parsePage(xpbf, page)
                        outputChannel.send(processed)
                        if (processCount.incrementAndGet() % 1000 == 0L ) {
                            logger.info("processed ${processCount.get()} articles")
                        }

                    }
                    outputChannel.close()
                    logger.info("DONE processed ${processCount.get()} articles")

                }
                launch {
                    for(doc in outputChannel) {
                        if (doc !=null) {
                            index(doc.id.toString(), doc)
                            if (indexCount.incrementAndGet() % 1000 == 0L) {
                                logger.info("indexed ${indexCount.get()} articles; skipped ${skipCount.get()}")
                            }
                        } else {
                            skipCount.incrementAndGet()
                        }
                    }
                    logger.info("DONE indexed ${indexCount.get()} articles; skipped ${skipCount.get()}")
                }
            }

//            BlobIterable(reader, "<page", "</page>").iterator()
//                .asSequence()
////                .take(10000) // whole thing would take a few minutes
//                .map { parsePage(xpbf, it) }
//                .filterNotNull() // filter out all the parser failures
//                .forEach {
//                    index(it.id.toString(), it)
//                    if (count % 1000 == 0) {
//                        logger.info("processed $count articles")
//                    }
//                    count++
//                }
        }
    }
    logger.info("Done in ${(System.currentTimeMillis() - start) / 1000} seconds!")

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