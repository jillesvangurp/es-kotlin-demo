package io.inbot.eskotlindemo

import com.fasterxml.jackson.databind.ObjectMapper
import io.inbot.eskotlinwrapper.JacksonModelReaderAndWriter
import org.elasticsearch.action.search.source
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.client.crudDao

fun main() {
    RestHighLevelClient().use { client ->

        // maybe index it first before searching s:-)
        val articleDao = client.crudDao("simplewikipedia",
            JacksonModelReaderAndWriter(SimpleWikiPediaPage::class, ObjectMapper().findAndRegisterModules())
        )

        val keyword ="search"
        // you could copy paste this from Kibana Dev tools ...
        val queryJson = """
{
    "size":5,
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
        println("We found ${results.totalHits} searching for '$keyword'. First page of results:")
        results.mappedHits.forEach {
            println("${it.title} - ${it.url}")
        }

//        println()
//        println("Lets get all of it by scrolling ...")
//        val scrollingResults = articleDao.search(scrolling = true) {
//            source(queryJson)
//        }
//        println("We found ${scrollingResults.totalHits} searching for '$keyword'")
//        scrollingResults.mappedHits.forEach {
//            println("${it.title} - ${it.url}")
//        }


        println()
        // if you want to know how many articles we indexed:
        println("Total number of simple wiki pedia articles ${articleDao.search {  }.totalHits}")
    }
}
