Lucidworks Storm/Solr Integration
========

Tools for building Storm topologies for indexing data into SolrCloud.

Features
========

* Send objects from a Storm topology into Solr

Getting Started
========

First, build the Jar for this project:

`mvn clean package`

This will generate the `target/storm-solr-1.0.jar` that contains the Storm JAR needed to deploy a topology.

Twitter-to-Solr Example
========

Before we get into the details of how to build a Storm topology that indexes documents into Solr, let's run an example:

Run on local workstation in development mode:
```
#!/bin/bash
STORM_HOME=/dev/tools/apache-storm-0.9.4
java -classpath $STORM_HOME/lib/*:target/storm-solr-1.0.jar com.lucidworks.storm.StreamingApp \
  example.twitter.TwitterToSolrTopology -localRunSecs 90
```

This command will launch a local Storm cluster and run a topology defined by
`com.lucidworks.storm.example.twitter.TwitterToSolrTopology` in development mode for 90 seconds (-localRunSecs).

Run on a distributed Storm cluster:

```
storm jar storm-solr-1.0.jar com.lucidworks.storm.StreamingApp example.twitter.TwitterToSolrTopology -env staging
```

The `-env` flag used in the previous example specifies the environment configuration to use when launching the
topology. See the *Environment-specific Configuration* section below for more information about configuring
topologies for specific runtime environments.

As the SolrBolt indexes documents, it will write basic metrics to the Storm log so that you can monitor performance
and health of the bolt.

Developing a Storm Topology
========

The `com.lucidworks.storm.StreamingApp` provides a simple framework for implementing Storm applications in Java or
Groovy. This helper class saves you from having to duplicate boilerplate code needed to run a Storm application,
giving you more time to focus on the business logic of your application. To leverage this framework, you need to
develop a concrete class that implements the `com.lucidworks.storm.StormTopologyFactory` interface. Let's look
at an example that indexes tweets from Twitter into SolrCloud

TwitterToSolrTopology
-------------

Groovy class that implements the `com.lucidworks.storm.StormTopologyFactory` interface to index tweets into Solr:

```
class TwitterToSolrTopology implements StormTopologyFactory {

  String getName() { return "twitter-to-solr" }

  StormTopology build(StreamingApp app) throws Exception {
    SpringSpout twitterSpout = new SpringSpout("twitterDataProvider", new Fields("id","tweet"));
    SpringBolt solrBolt = new SpringBolt("solrBoltAction", app.tickRate("solrBolt"));

    TopologyBuilder builder = new TopologyBuilder()
    builder.setSpout("twitterSpout", twitterSpout, app.parallelism("twitterSpout"))
    builder.setBolt("solrBolt", solrBolt, app.parallelism("solrBolt")).shuffleGrouping("twitterSpout")

    return builder.createTopology()
  }
}
```

The `build` method is where you will define the spout(s) and bolts for your Storm topology. In this simple example,
we have a simple Spout that streams tweets from the Twitter API into Storm. The tweets are processed by a single
Bolt named `solrBolt`, which is an instance of `SpringBolt`.

SpringBolt
-------------

The `com.lucidworks.storm.spring.SpringBolt` class allows you to implement your bolt logic as a simple
Spring-managed POJO. In the example above, the `SpringBolt` class delegates message processing logic to a
Spring-managed bean with id `solrBoltAction`. The `solrBoltAction` bean is defined in the Spring container
configuration file `resources/spring.xml` as:

```
  <bean id="solrBoltAction" class="com.lucidworks.storm.solr.SolrBoltAction">
    <property name="batchSize" value="100"/>
    <property name="bufferTimeoutMs" value="1000"/>
  </bean>
```

The `SpringBolt` framework provides clean separation of concerns and allows you to leverage the full power of the
Spring framework for developing your Storm topology. Moreover, this approach makes it easier to test your bolt action
logic in JUnit outside of the Storm framework.

The `SolrBoltAction` bean also depends on an instance of the `CloudSolrClient` class from SolrJ to be auto-wired
by the Spring framework:

```
  <bean id="cloudSolrClient" class="org.apache.solr.client.solrj.impl.CloudSolrClient">
    <constructor-arg index="0" value="${zkHost}"/>
    <property name="defaultCollection" value="${defaultCollection}"/>
  </bean>
```

The `zkHost` and `defaultCollection` properties are defined in `resources/Config.groovy`

Environment-specific Configuration
-------------

The Spring-driven framework allows you to keep all environment-specific configuration properties in the same
configuration file: Config.groovy. Don't worry if you don't know Groovy, the syntax of the Config.groovy file is
very easy to understand and allows you to cleanly separate properties for the following environments:
test, dev, staging, and production. Put simply, this approach allows you to run the topology in multiple environments
using a simple command-line switch to specify the environment settings that should be applied -env.

```
environments {

    twitterSpout.parallelism = 1
    solrBolt.parallelism = 2
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
```

Notice that all dynamic variables used in the resources/spring.xml must be prefixed with "spring." in Config.groovy.

You should also configure all Storm-topology related properties in the Config.groovy file as well.

The easiest approach is to update the resources/Config.groovy with the configuration settings for each of your
environments and then rebuild the Job JAR. However, you can also override the Config.groovy using the -config
command-line option when deploying the topology.

SolrInputDocumentMapper
-------------

The `SolrBoltAction` uses an implementation of `com.lucidworks.storm.solr.SolrInputDocumentMapper` to transform
messages into SolrInputDocument objects. The default implementation uses Java reflection to read data from a Java object
to populate the fields of the SolrInputDocument. When using the default mapper, you must have dynamic fields enabled in
your Solr schema.xml or have Solr's field guessing feature enabled for your collection, which is enabled by default
for the data_driven_schema_configs configset. If using field guessing, then you need to define the mapper as:

```
  <bean id="solrInputDocumentMapper" class="com.lucidworks.storm.solr.DefaultSolrInputDocumentMapper">
    <property name="fieldGuessingEnabled" value="${fieldGuessingEnabled}"/>
  </bean>
```

The ${fieldGuessingEnabled} variable will be resolved from the Config.groovy configuration file at runtime.

Alternatively, you can use the JsonDocumentMapper which sends arbitrary JSON documents to the /update/json/docs
request handler.
