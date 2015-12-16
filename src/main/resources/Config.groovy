environments {

  twitterSpout.parallelism = 1
  csvParserBolt.parallelism = 2
  solrBolt.tickRate = 5

  eventsimSpout.parallelism = 1
  collectionPerTimeFrameSolrBolt.parallelism = 2
  collectionPerTimeFrameSolrBolt.tickRate = 5

  maxPendingMessages = -1

  test {
    env.name = "test"
  }

  development {
    env.name = "development"

    spring.zkHost = "localhost:9983"
    spring.defaultCollection = "gettingstarted"
    spring.fieldGuessingEnabled = true

    spring.maxBufferSize = 500
    spring.bufferTimeoutMs = 500

    spring.fs.defaultFS = "hdfs://localhost:9000"
    spring.hdfsDirPath = "/user/timpotter/csv_files"
    spring.hdfsGlobFilter = "*.csv"

    spring.fusionEndpoints = "http://localhost:8764"
    spring.fusionUser = "admin"
    spring.fusionPassword = "password123"
    spring.fusionRealm = "native"
    spring.fusionUpdatePath = "/api/apollo/index-pipelines/conn_solr/collections/twitterCollection/index"

    spring.eventsimFileToParse = "src/test/resources/eventsim-sample.json"
    spring.eventsimNumShards = 2;
    spring.eventsimReplicationFactor = 1;
    spring.eventsimConfigName = "eventsim"
  }

  staging {
    env.name = "staging"

    spring.zkHost = "zkhost:2181"
    spring.defaultCollection = "staging_collection"
    spring.fieldGuessingEnabled = false

    spring.maxBufferSize = 100
    spring.bufferTimeoutMs = 500
  }

  production {
    env.name = "production"

    spring.zkHost = "zkhost1:2181,zkhost2:2181,zkhost3:2181"
    spring.defaultCollection = "prod_collection"
    spring.fieldGuessingEnabled = false

    spring.maxBufferSize = 100
    spring.bufferTimeoutMs = 500
  }
}