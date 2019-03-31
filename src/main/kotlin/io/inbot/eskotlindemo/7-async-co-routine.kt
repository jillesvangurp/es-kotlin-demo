package io.inbot.eskotlindemo

import com.fasterxml.jackson.databind.ObjectMapper
import io.inbot.eskotlinwrapper.JacksonModelReaderAndWriter
import kotlinx.coroutines.runBlocking
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.client.crudDao
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder
import org.elasticsearch.search.aggregations.support.ValueType
import org.elasticsearch.search.builder.SearchSourceBuilder

fun main() {
    RestHighLevelClient().use { client ->

        // index it first before running this :-)
        val articleDao = client.crudDao(
            "simplewikipedia",
            JacksonModelReaderAndWriter(SimpleWikiPediaPage::class, ObjectMapper().findAndRegisterModules())
        )

        // create a co-routine
        runBlocking {
            // Do an async search
            articleDao.searchAsync {
                source(
                    SearchSourceBuilder.searchSource().size(0).aggregation(
                        TermsAggregationBuilder(
                            "descriptions",
                            ValueType.STRING
                        ).field("description")
                            .size(100)
                    )
                )
            }.searchResponse.aggregations.get<Terms>("descriptions").buckets.forEach {
                println("${it.key} : ${it.docCount}")
            }
        }
    }
}