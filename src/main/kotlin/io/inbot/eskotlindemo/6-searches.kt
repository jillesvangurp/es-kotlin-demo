package io.inbot.eskotlindemo

import com.fasterxml.jackson.databind.ObjectMapper
import io.inbot.eskotlinwrapper.JacksonModelReaderAndWriter
import org.elasticsearch.action.search.source
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.client.crudDao

fun main() {
    RestHighLevelClient().use { client ->
        // lets use jackson to serialize our Thing, other serializers
        // can be supported by implementing ModelReaderAndWriter
        val modelReaderAndWriter =
            JacksonModelReaderAndWriter(SimpleWikiPediaPage::class, ObjectMapper().findAndRegisterModules())

        // index it first before running this :-)
        val articleDao = client.crudDao("simplewikipedia", modelReaderAndWriter)

        println("${articleDao.search {  }.totalHits}")

        val keyword ="search"
        articleDao.search {
            source(
                """
{
    "size":100,
    "query": {
        "bool": {
            "should":[
                {
                    "term":{
                        "title":{
                            "value":"$keyword",
                            "boost":2
                        }
                    }
                },
                {
                    "term":{
                        "description":"$keyword"
                    }
                }
            ]
        }
    }
}
"""
            )
        }.mappedHits.forEach {
            println("${it.title} - ${it.url}")
        }

//        articleDao.search {
//            source(
//                SearchSourceBuilder.searchSource().size(0).aggregation(
//                    TermsAggregationBuilder(
//                        "descriptions",
//                        ValueType.STRING
//                    ).field("description")
//                        .size(100)
//                )
//            )
//        }.searchResponse.aggregations.get<Terms>("descriptions").buckets.forEach {
//            println("${it.key} : ${it.docCount}")
//        }
    }
}
