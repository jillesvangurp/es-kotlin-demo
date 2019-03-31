package io.inbot.eskotlindemo

import com.fasterxml.jackson.databind.ObjectMapper
import io.inbot.eskotlinwrapper.JacksonModelReaderAndWriter
import org.elasticsearch.action.search.source
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.client.crudDao

fun main() {
    RestHighLevelClient().use { client ->

        // index it first before running this :-)
        val articleDao = client.crudDao("simplewikipedia",
            JacksonModelReaderAndWriter(SimpleWikiPediaPage::class, ObjectMapper().findAndRegisterModules())
        )

        val keyword ="search"
        // you could copy paste this from Kibana Dev tools ...
        val queryJson = """
{
    "size":20,
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
        // do the search
        val results = articleDao.search {
            source(queryJson)
        }
        println("We found ${results.totalHits} searching for '$keyword'")
        results.mappedHits.forEach {
            println("${it.title} - ${it.url}")
        }

        // if you want to know how many articles we indexed:
        println("Total number of simple wiki pedia articles ${articleDao.search {  }.totalHits}")
    }
}
