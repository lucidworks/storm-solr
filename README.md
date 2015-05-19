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

To begin, let’s understand how to run a topology in Storm. Effectively, there are two basic modes of running a Storm topology: local and cluster mode. Local mode is great for testing your topology locally before pushing it out to a remote Storm cluster, such as staging or production. For starters, you need to compile and package your code and all of its dependencies into a single JAR with an main class that runs your topology.

For this project, I chose the Maven Shade plugin to create the unified JAR with dependencies. The benefit of the Shade plugin is that it can relocate classes into different packages at the byte-code level to avoid dependency conflicts. This comes in quite handy if your application depends on 3rd party libraries that conflict with classes on the Storm classpath. You can look at the project pom.xml file for specific details about I use the Shade plugin. For now, let it suffice to say that the project makes it very easy to build a Storm JAR for your application. Once you have a unified JAR (storm-solr-1.0.jar), you’re ready to run your topology in Storm.

To run locally, you need to have some code that starts up a local cluster in the JVM, submits your topology, and then waits for some configurable amount of time before shutting it all back down, as shown in the following code:

```
LocalCluster cluster = new LocalCluster();
cluster.submitTopology(topologyName, stormConf, topologyFactory.build(this));
try {
 Thread.sleep(localRunSecs * 1000);
} catch (InterruptedException ie) { ... }
cluster.killTopology(topologyName);
cluster.shutdown();
```

However, if you want to run your topology in a remote cluster, such as production, you need to do something like:
```
StormSubmitter.submitTopology(topologyName, stormConf, topologyFactory.build(this));
```
Consequently, the first thing you’ll need is some way to decide whether to run locally or submit to a remote cluster using some command-line switch when invoking your application. You’ll also need some way to pass along environment specific configuration options. For instance, the Solr you want to index into will be different between local, staging, and production. Let it suffice to say that launching and configuring a Storm topology ends up requiring a fair amount of common boilerplate code. To address this need, our project provides the com.lucidworks.storm.StreamingApp class that does the following:

* Separates the process of defining a Storm topology from the process of running a Storm topology in different environments. This lets you focus on defining a topology for your specific requirements.
* Provides a clean mechanism for separating environment-specific configuration settings.
* Minimizes duplicated boilerplate code when developing multiple topologies and gives you a common place to insert reusable logic needed for all of your topologies.

To use StreamingApp, you simply need to implement the StormTopologyFactory interface, which defines the spouts and bolts in your topology:
```
public interface StormTopologyFactory {
 String getName();

 StormTopology build(StreamingApp app) throws Exception;
}
```

Developing a Storm Topology
========

Let's look at an example that indexes tweets from Twitter into SolrCloud

TwitterToSolrTopology
-------------

```
class TwitterToSolrTopology implements StormTopologyFactory {

  static final Fields spoutFields = new Fields("id", "tweet")

  String getName() { return "twitter-to-solr" }

  StormTopology build(StreamingApp app) throws Exception {
    // setup spout and bolts for accessing Spring-managed POJOs at runtime
    SpringSpout twitterSpout = new SpringSpout("twitterDataProvider", spoutFields);
    SpringBolt solrBolt = new SpringBolt("solrBoltAction", app.tickRate("solrBolt"));

    // wire up the topology to read tweets and send to Solr
    TopologyBuilder builder = new TopologyBuilder()
    builder.setSpout("twitterSpout", twitterSpout, app.parallelism("twitterSpout"))
    builder.setBolt("solrBolt", solrBolt, app.parallelism("solrBolt"))
           .shuffleGrouping("twitterSpout")

    return builder.createTopology()
  }
}
```

A couple of things should stand out to you in this listing. First, there’s no command-line parsing, environment-specific configuration handling, or any code related to running this topology. All that you see here is code defining a StormTopology. Second, the code is quite easy to understand because it only does one thing. Lastly, this class is written in Groovy instead of Java, which helps keep things nice and tidy and I find Groovy to be more enjoyable to write. Of course if you don’t want to use Groovy, you can use Java, as the framework supports both seamlessly.

We’ll get into the specific details of the implementation shortly, but first, let’s see how to run the TwitterToSolrTopology using the StreamingApp framework. For local mode, you would do:

```
java -classpath $STORM_HOME/lib/*:target/storm-solr-1.0.jar com.lucidworks.storm.StreamingApp \
  example.twitter.TwitterToSolrTopology -localRunSecs 90
```

The command above will run the TwitterToSolrTopology for 90 seconds on your local workstation and then shutdown. All the setup work is provided by the StreamingApp class. To submit to a remote cluster, you would do:

```
$STORM_HOME/bin/storm jar target/storm-solr-1.0.jar com.lucidworks.storm.StreamingApp \
   example.twitter.TwitterToSolrTopology -env staging
```

Notice that I’m using the -env flag to indicate I’m running in my staging environment. It’s common to need to run a Storm topology in different environments, such as test, staging, and production, so that’s built into the StreamingApp framework.

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

SpringSpout
-------------

In Storm, a spout produces a stream of tuples. The TwitterToSolrTopology example uses an instance of SpringSpout and a Twitter data provider to stream tweets into the topology:

```
SpringSpout twitterSpout = new SpringSpout("twitterDataProvider", spoutFields);
```

SpringSpout allows you to focus on the application-specific logic needed to generate data without having to worry about Storm specific implementation details. As you might have guessed, the data provider is a Spring-managed POJO that implements the StreamingDataProvider interface:

```
public interface StreamingDataProvider {
 void open(Map stormConf);

 boolean next(NamedValues record) throws Exception;
}
```

Take a look at the TwitterDataProvider implementation provided in the project as a starting point for implementing a Spring-managed bean for your topology.


Unit Testing
-------------

When writing a unit test, you don’t want to have to spin up a Storm cluster to test application-specific logic that doesn’t depend on Storm. Recall that one of the benefits of using this framework is that it separates business logic from Storm boilerplate code. Let’s look at some code from the unit test for our SolrBoltAction implementation.

```
@Test
public void testBoltAction() throws Exception {
  // Spring @Autowired property at runtime
  SolrBoltAction sba = new SolrBoltAction(cloudSolrServer);
  sba.setMaxBufferSize(1); // to avoid buffering docs

  // Mock the Storm tuple
  String docId = "1";
  TestDoc testDoc = new TestDoc(docId, "foo", 10);
  Tuple mockTuple = mock(Tuple.class);
  when(mockTuple.size()).thenReturn(2);
  when(mockTuple.getString(0)).thenReturn(docId);
  when(mockTuple.getValue(1)).thenReturn(testDoc);
  SpringBolt.ExecuteResult result = sba.execute(mockTuple, null);
  assertTrue(result == SpringBolt.ExecuteResult.ACK);
  cloudSolrServer.commit();
  ...
}
```

The first thing that you should notice is the unit test doesn’t need a Storm cluster to run. This makes your tests run quickly and helps isolate bugs since there are fewer runtime dependencies in this test. It’s also important to notice that the SolrBoltAction implementation is not running in a Spring-managed container in this unit test. We’re just creating the instance directly using the new operator. This is good test design as well since you don’t want to create a Spring container for every unit test and testing the Spring configuration is not the responsibility of this particular unit test.

The unit test is also using Mockito to mock the Storm Tuple object that is passed into SolrBoltAction. Mockito makes it easy to mock complex objects in a unit test. The key take-away here is that the unit test focuses on verifying the SolrBoltAction implementation without having to worry about Storm or Spring.

Environment-specific Configuration
-------------

I’ve implemented several production Storm topologies in the past couple years and one pattern that keeps emerging is the need to manage configuration settings for different environments. For instance, we’ll need to index into a different SolrCloud cluster for staging and production. To address this need, the Spring-driven framework allows you to keep all environment-specific configuration properties in the same configuration file: Config.groovy.

Don't worry if you don't know Groovy, the syntax of the Config.groovy file is very easy to understand and allows you to cleanly separate properties for the following environments: test, dev, staging, and production. Put simply, this approach allows you to run the topology in multiple environments using a simple command-line switch to specify the environment settings that should be applied -env. Here’s an example of Config.groovy that shows how to organize properties for the test, development, staging, and production environments:

```
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
```

Notice that all dynamic variables used in the resources/storm-solr-spring.xml must be prefixed with "spring." in Config.groovy. For instance, the ${zkHost} setting in storm-solr-spring.xml resolves to the spring.zkHost property in Config.groovy.

You can also configure all Storm-topology related properties in the Config.groovy file. For instance, if you need to change the topology.max.task.parallelism property for your topology, you can set that in Config.groovy.

When adapting the project to your own requirements, the easiest approach is to update the resources/Config.groovy with the configuration settings for each of your environments and then rebuild the Job JAR. However, you can also specify a different Config.groovy file by using the -config command-line option when deploying the topology, such as:

```
$STORM_HOME/bin/storm jar target/storm-solr-1.0.jar com.lucidworks.storm.StreamingApp \
   example.twitter.TwitterToSolrTopology -env staging -config MyConfig.groovy
```

Metrics
-------------

Storm provides high-level metrics for bolts and spouts, but if you need more visibility into the inner workings of your application-specific logic, then it’s common to use the Java metrics library, see: https://dropwizard.github.io/metrics/3.1.0/. Fortunately, there are open source options for integrating metrics with Spring, see: https://github.com/ryantenney/metrics-spring.

The Spring context configuration file resources/storm-solr-spring.xml comes pre-configured with all the infrastructure needed to inject metrics into your bean implementations:
```
<metrics:metric-registry id="metrics"/>
<metrics:annotation-driven metric-registry="metrics"/>
<metrics:reporter type="slf4j" metric-registry="metrics" period="1m"/>
```
By default, the project is configured to log metrics once a minute to the Storm log using the slf4j reporter.

When implementing your StreamingDataAction (bolt) or StreamingDataProvider (spout), you can have Spring auto-wire metrics objects using the @Metric annotation when declaring metrics-related member variables. For instance, the SolrBoltAction class uses a Timer to track how long it takes to send batches to Solr.
```
@Metric
public Timer sendBatchToSolr;
```
The SolrBoltAction class provides several examples of how to use metrics in your bean implementations.

Before moving on to some Solr specific features in the framework, I want drive home one more point. The example Twitter topology we’ve been working with in this blog is quite trivial. In practice, most topologies are more complex and have many spouts and bolts, typically written by multiple developers. Moreover, topologies tend to evolve over time to incorporate data from new systems and requirements. Using this framework will help you craft complex topologies in a simple, maintainable fashion.

Field Mapping
-------------

The SolrBoltAction bean takes care of sending documents to SolrCloud in an efficient manner, but it only works with SolrInputDocument objects from SolrJ. It’s unlikely that your Storm topology will be working with SolrInputDocument objects natively, so the SolrBoltAction bean delegates mapping of input Tuples to SolrInputDocument objects to a Spring-managed bean that implements the com.lucidworks.storm.solr.SolrInputDocumentMapper interface. This fits nicely with our design approach of separating concerns in our topology.

The default implementation provided in the project (DefaultSolrInputDocumentMapper) uses Java reflection to read data from a Java object to populate the fields of the SolrInputDocument. In the Twitter example, the default implementation uses Java reflection to read data from a Twitter4J Status object to populate dynamic fields on a SolrInputDocument instance.

When using the default mapper, you must have dynamic fields enabled in your Solr schema.xml or have Solr's field guessing feature enabled for your collection, which is enabled by default for the data_driven_schema_configs configset. The default mapper bean is defined in the resources/storm-solr-spring.xml file as:

```
  <bean id="solrInputDocumentMapper"
        class="com.lucidworks.storm.solr.DefaultSolrInputDocumentMapper">
    <property name="fieldGuessingEnabled" value="${fieldGuessingEnabled}"/>
  </bean>
```

As discussed above, the ${fieldGuessingEnabled} variable will be resolved from the Config.groovy configuration file at runtime.

It should be clear, however, that you can inject your own SolrInputDocumentMapper implementation into the bolt bean using Spring if the default implementation does not meet your needs.