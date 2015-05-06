environments {

  twitterSpout.parallelism = 1
  csvParserBolt.parallelism = 2
  solrBolt.tickRate = 5

  maxPendingMessages = -1

  test {
    env.name = "test"
  }

  development {
    env.name = "development"

    spring.zkHost = "localhost:9983"
    spring.defaultCollection = "gettingstarted"
    spring.fieldGuessingEnabled = true

    spring.fs.defaultFS = "hdfs://localhost:9000"
    spring.hdfsDirPath = "/user/timpotter/csv_files"
    spring.hdfsGlobFilter = "*.csv"
  }

  staging {
    env.name = "staging"

    spring.zkHost = "zkhost:2181"
    spring.defaultCollection = "staging_collection"
    spring.fieldGuessingEnabled = false
  }

  production {
    env.name = "production"

    spring.zkHost = "zkhost1:2181,zkhost2:2181,zkhost3:2181"
    spring.defaultCollection = "prod_collection"
    spring.fieldGuessingEnabled = false
  }
}