package io.inbot.eskotlindemo

import com.fasterxml.jackson.databind.ObjectMapper
import io.inbot.eskotlinwrapper.JacksonModelReaderAndWriter
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.runBlocking
import org.elasticsearch.client.create
import org.elasticsearch.client.crudDao
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder
import org.elasticsearch.search.aggregations.support.ValueType
import org.elasticsearch.search.builder.SearchSourceBuilder

@InternalCoroutinesApi
fun main() {
    create().use { client ->
        
        val articleDao = client.crudDao(
            "simplewikipedia-sample",
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