package com.lucidworks.storm;

import groovy.util.ConfigSlurper;
import groovy.util.ConfigObject;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;

import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.env.MapPropertySource;

/**
 * Serves as a common framework for running Storm topologies. Specifically, this driver
 * provides three key benefits for Storm topologies:
 * <p/>
 * 1) Standard configuration setup with command-line options and Config.groovy
 * 2) Allows us to execute multiple Storm topologies from a single JAR file
 * 3) Separates the running of a topology (a framework task) from the building
 * of a topology definition (a custom task)
 */
public class StreamingApp {

  private static final Logger appLog = Logger.getLogger(StreamingApp.class);

  private static final Map<String, ApplicationContext> globalSpringContexts = new HashMap<String, ApplicationContext>();

  public static enum ENV {
    development, test, staging, production
  }

  /**
   * Returns the global singleton to the Spring framework ApplicationContext per Storm Topology per JVM.
   */
  public static final ApplicationContext spring(Map stormConf) {
    String stormId = (String) stormConf.get("storm.id");
    String springXml = (String) stormConf.get("springXml");
    if (springXml == null) springXml = "storm-solr-spring.xml";

    // one per JVM
    ApplicationContext spring = null;
    synchronized (StreamingApp.class) {
      spring = globalSpringContexts.get(stormId);
      if (spring == null) {

        InputStream res = StreamingApp.class.getClassLoader().getResourceAsStream(springXml);
        if (res != null) {
          appLog.info("Classpath resource '" + springXml + "' FOUND by classloader: " + StreamingApp.class.getClassLoader());
        } else {
          appLog.warn("Classpath resource '" + springXml + "' not found by classloader: " + StreamingApp.class.getClassLoader());
        }

        ClassPathXmlApplicationContext ctxt = new ClassPathXmlApplicationContext(new String[]{springXml}, false /* don't refresh yet */);
        // inject the Spring closure from the Storm config map into the Spring context for property resolution
        if (ctxt instanceof ConfigurableApplicationContext) {
          Map<String, Object> springProps = new HashMap<String, Object>();
          for (Object key : stormConf.keySet()) {
            if (!(key instanceof String))
              continue;
            Object valu = stormConf.get(key);
            if (valu == null)
              continue;

            String propId = (String) key;
            if (propId.startsWith("spring.")) {
              springProps.put(propId.substring(7), valu);
            }
          }
          if (!springProps.isEmpty())
            ((ConfigurableApplicationContext) ctxt).getEnvironment()
              .getPropertySources().addFirst(new MapPropertySource("STORM_CONF", springProps));
        }
        ctxt.refresh();
        spring = ctxt;
        globalSpringContexts.put(stormId, spring);
      }
    }
    return spring;
  }

  private static final DateFormat TIMESTAMP_FORMATTER =
    DateFormat.getDateTimeInstance(DateFormat.LONG, DateFormat.LONG);

  public static final String timestamp() {
    return timestamp(System.currentTimeMillis());
  }

  public static final String timestamp(long timeMs) {
    return TIMESTAMP_FORMATTER.format(new Date(timeMs));
  }

  public static final Throwable getRootCause(Throwable thr) {
    if (thr == null)
      return null;
    Throwable rootCause = thr;
    Throwable cause = thr;
    while ((cause = cause.getCause()) != null)
      rootCause = cause;
    return rootCause;
  }

  /**
   * Main entry point to the Client execution environment.
   *
   * @param args Command-line args parsed using Commons CLI (GnuParser)
   */
  public static void main(String[] args) throws Exception {
    // the first argument must be the name of the topo to execute
    if (args == null || args.length < 1 || args[0].startsWith("-")) {
      System.err.println("Must specify a StormTopologyFactory implementation class name!\n" +
        "Example:\n\tstorm jar lw-storm.jar com.lucidworks.storm.StreamingApp StormTopologyFactoryClassName [options]");
      System.exit(1);
    }

    String topoClassName = args[0];
    if (topoClassName.indexOf(".") == -1) {
      topoClassName = "com.lucidworks.storm." + topoClassName;
    } else if (!topoClassName.startsWith("com.lucidworks.storm.")) {
      topoClassName = "com.lucidworks.storm." + topoClassName;
    }
    ClassLoader cl = StreamingApp.class.getClassLoader();
    Class<StormTopologyFactory> topoClass =
      (Class<StormTopologyFactory>) cl.loadClass(topoClassName);
    StormTopologyFactory topologyFactory = topoClass.newInstance();

    // all but the first arg is treated as args to the topo
    String[] actionArgs = new String[args.length - 1];
    for (int i = 0; i < actionArgs.length; i++) {
      actionArgs[i] = args[i + 1];
    }

    // run the topology through this driver
    (new StreamingApp(topologyFactory, actionArgs)).run();
  }

  /**
   * Parses the command-line arguments passed by the user.
   *
   * @param topo
   * @param args
   * @return CommandLine The Apache Commons CLI object.
   */
  public static CommandLine processCommandLineArgs(StormTopologyFactory topo, String[] args) {
    Options options = new Options();

    options.addOption("w", "localRunSecs", true, "Number of seconds to sleep after submitting topology in development mode; default: 30");
    options.addOption("e", "env", true, "Environment; default is 'development'");
    options.addOption("h", "help", false, "Print this message");
    options.addOption("v", "verbose", false, "Generate verbose log messages");
    options.addOption("c", "config", true, "Optional path to Config.groovy");

    CommandLine cli = null;
    try {
      cli = (new GnuParser()).parse(options, args, true);
    } catch (ParseException exp) {
      boolean hasHelpArg = false;
      if (args != null && args.length > 0) {
        for (int z = 0; z < args.length; z++) {
          if ("-h".equals(args[z]) || "-help".equals(args[z])) {
            hasHelpArg = true;
            break;
          }
        }
      }
      if (!hasHelpArg) {
        System.err.println("Failed to parse command-line arguments due to: " + exp.getMessage());
      }
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(topo.getClass().getSimpleName(), options);
      System.exit(1);
    }

    if (cli.hasOption("help")) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(topo.getClass().getSimpleName(), options);
      System.exit(0);
    }

    return cli;
  }

  public static Option buildOption(String argName, String shortDescription) {
    return buildOption(argName, shortDescription, null); // no default, required
  }

  @SuppressWarnings("static-access")
  public static Option buildOption(String argName, String shortDescription, String defaultValue) {
    if (defaultValue != null)
      shortDescription += (" Default is " + defaultValue);
    return OptionBuilder.hasArg().isRequired((defaultValue == null)).withDescription(shortDescription).create(argName);
  }

  protected CommandLine cli;
  protected StormTopologyFactory topo;
  protected Logger log;
  protected Config stormConf;
  protected List<Closeable> closeables;
  protected ENV env;

  public StreamingApp(StormTopologyFactory topo, String[] args) throws IOException {
    this.topo = topo;
    log = Logger.getLogger(topo.getClass());
    cli = processCommandLineArgs(topo, args);
    env = ENV.valueOf(cli.getOptionValue("env", "development"));
    stormConf = initStormConfig(cli);
    stormConf.setDebug(cli.hasOption("verbose"));
  }

  public ENV getEnv() {
    return env;
  }

  public int parallelism(String component) {
    Object val = null;
    Object componentProps = (component != null) ? stormConf.get(component) : null;
    if (componentProps != null && componentProps instanceof Map) {
      Map map = (Map) componentProps;
      val = map.get("parallelism");
    } else {
      val = stormConf.get(component + ".parallelism"); // flattened
    }
    return (val != null && val instanceof Number) ? ((Number) val).intValue() : 1;
  }

  public int tickRate(String component) {
    Object val = null;
    Object componentProps = (component != null) ? stormConf.get(component) : null;
    if (componentProps != null && componentProps instanceof Map) {
      Map map = (Map) componentProps;
      val = map.get("tickRate");
    } else {
      val = stormConf.get(component + ".tickRate");
    }
    return (val != null && val instanceof Number) ? ((Number) val).intValue() : 0;
  }

  /**
   * Builds and runs a StormTopology in the configured environment (development|staging|production)
   */
  public void run() throws Exception {
    log.info(String.format("Running %s in %s mode.", topo.getName(), env));

    String topologyName = topo.getName();
    if (ENV.development == env) {
      int localRunSecs = Integer.parseInt(cli.getOptionValue("localRunSecs", "30"));
      try {
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topologyName, stormConf, topo.build(this));

        log.info("Submitted " + topologyName + " to LocalCluster at " + timestamp() + " ... sleeping for " +
          localRunSecs + " seconds before terminating.");
        try {
          Thread.sleep(localRunSecs * 1000);
        } catch (InterruptedException ie) {
          Thread.interrupted();
        }

        log.info("Killing " + topologyName);
        cluster.killTopology(topologyName);

        cluster.shutdown();
        log.info("Shut down LocalCluster at " + timestamp());
      } catch (Exception exc) {
        Throwable rootCause = getRootCause(exc);
        log.error("Storm topology " + topologyName + " failed due to: " + rootCause, rootCause);
        throw exc;
      } finally {
        cleanup();
      }
      System.exit(0);
    } else {
      StormSubmitter.submitTopology(topologyName, stormConf, topo.build(this));
    }
  }

  /**
   * Provide the initialized Storm Config
   */
  public Config getStormConfig() {
    return stormConf;
  }

  /**
   * Bootstrap a Storm Config object from a Groovy Config class.
   */
  @SuppressWarnings("unchecked")
  protected Config initStormConfig(CommandLine cl) throws IOException {
    String env = cl.getOptionValue("env", "development");
    File configFile = getOptionalConfigFile(cl);
    stormConf = getConfig(env, configFile);

    Map<String, String> otherArgs = getUnrecognizedArgs(cl.getArgs());
    if (otherArgs != null && !otherArgs.isEmpty()) {
      log.info("Adding overrides to Storm config: " + otherArgs);
      stormConf.putAll(otherArgs);
    }

    return stormConf;
  }

  /**
   * Returns a config file that's passed in as a command line arg
   *
   * @param cl
   * @return
   * @throws FileNotFoundException
   */
  private File getOptionalConfigFile(CommandLine cl) throws FileNotFoundException {
    File configFile = null;
    String config = cl.getOptionValue("config", null);
    if (config != null) {
      configFile = new File(config);
      if (!configFile.exists()) {
        throw new FileNotFoundException(configFile.getAbsolutePath());
      }
    }
    return configFile;
  }

  public static Config getConfig(String env) throws IOException {
    return getConfig(env, null);
  }

  public static Config getConfig(String env, File config) throws IOException {
    ConfigObject configObject = new ConfigSlurper(env).parse(readGroovyConfigScript(config));
    Config stormConf = new Config();
    Map flatten = configObject.flatten();
    stormConf.putAll(flatten);

    Map<String, Class> dataTypes = new HashMap<String, Class>();
    dataTypes.put("topology.workers", Integer.class);
    dataTypes.put("topology.acker.executors", Integer.class);
    dataTypes.put("topology.message.timeout.secs", Integer.class);
    dataTypes.put("topology.max.task.parallelism", Integer.class);
    dataTypes.put("topology.stats.sample.rate", Double.class);

    // this will convert built in properties as storm uses old school properties
    for (Field field : stormConf.getClass().getFields()) {
      if (Modifier.isStatic(field.getModifiers())
        && Modifier.isPublic(field.getModifiers())) {
        String property = field.getName().toLowerCase().replace('_', '.');
        if (property.startsWith("java.")) {
          // don't mess with Java system properties here
          continue;
        }

        Object override = flatten.get(property);
        if (override != null) {
          stormConf.put(property, override);
          System.out.println("Overrode property '" + property + "' with value [" + override + "] from Config.groovy of type " + override.getClass().getName());
        }
        String system = System.getProperty(property, null);
        if (system != null) {
          if (dataTypes.containsKey(property)) {
            Class aClass = dataTypes.get(property);
            try {
              Method valueOf = aClass.getMethod("valueOf", String.class);
              stormConf.put(property, valueOf.invoke(aClass, system));
              System.out.println("Overrode property '" + property + "' with value [" + stormConf.get(property) + "] from -D System property of type " + aClass.getName());
            } catch (Exception e) {
              throw new RuntimeException(e.getMessage(), e);
            }
          } else {
            stormConf.put(property, system);
            System.out.println("Overrode property '" + property + "' with String value [" + system + "] from -D System property");
          }
        }
      }
    }

    return stormConf;
  }

  /**
   * Build a Map of any unrecognized command-line args of format: --foo=bar
   */
  protected Map<String, String> getUnrecognizedArgs(String[] addlArgs) {
    Map<String, String> unrecogArgs = new HashMap<String, String>(20);
    if (addlArgs != null) {
      for (int a = 0; a < addlArgs.length; a++) {
        String tmp = addlArgs[a].trim();
        if (tmp.startsWith("--")) {
          tmp = tmp.substring(2);
        } else if (tmp.startsWith("-")) {
          tmp = tmp.substring(1);
        }
        if (tmp.indexOf("=") != -1) {
          String[] pair = tmp.split("=");
          String name = pair[0].trim();
          String value = pair[1].trim();
          if (name.length() > 0 && value.length() > 0) {
            unrecogArgs.put(name, value);
          }
        } else {
          // is there another to read?
          if (addlArgs.length > (a + 1)) {
            unrecogArgs.put(tmp, addlArgs[a + 1].trim());
            ++a; // skip the next value in the array
          } else {
            log.warn("Skipped dangling command-line arg: " + addlArgs[a]);
          }
        }
      }
    }
    return unrecogArgs;
  }

  /**
   * Reads Config.groovy from the classpath.
   */
  private static String readGroovyConfigScript(File file) throws IOException {

    InputStream groovyConfigIn;
    if (file != null) {
      groovyConfigIn = new FileInputStream(file);
    } else {
      groovyConfigIn = StreamingApp.class.getResourceAsStream("/Config.groovy");
    }
    if (groovyConfigIn == null)
      throw new FileNotFoundException("Missing classpath resource /Config.groovy");

    StringBuilder sb = new StringBuilder();
    InputStreamReader reader = null;
    char[] ach = new char[1024];
    int r = 0;
    try {
      reader = new InputStreamReader(groovyConfigIn, "UTF-8");
      while ((r = reader.read(ach, 0, ach.length)) > 0)
        sb.append(ach, 0, r);
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (Exception ignore) {
        }
      }
    }
    return sb.toString();
  }

  /**
   * Remember to close a Closeable resource after the app finishes.
   *
   * @param closeable
   */
  public void rememberCloseable(Closeable closeable) {
    if (closeables == null) {
      closeables = new ArrayList<Closeable>();
    }
    closeables.add(closeable);
  }

  /**
   * Get access to the parsed command-line options.
   *
   * @return CommandLine
   */
  public CommandLine getCommandLine() {
    return cli;
  }

  /**
   * Closes all resources held by this driver.
   */
  protected void cleanup() {
    if (closeables != null) {
      for (Closeable next : closeables) {
        try {
          next.close();
        } catch (Exception nothingWeCanDo) {
        }
      }
    }
  }

  /**
   * Reads a File into a byte[].
   *
   * @param file
   * @return byte[] The bytes in the file.
   * @throws IOException
   */
  public byte[] readFile(File file) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    int r = 0;
    byte[] aby = new byte[256];
    FileInputStream fis = null;
    try {
      fis = new FileInputStream(file);
      while ((r = fis.read(aby)) != -1) {
        bytes.write(aby, 0, r);
      }
    } finally {
      if (fis != null) {
        try {
          fis.close();
        } catch (Exception zzz) {
        }
      }
    }
    return bytes.toByteArray();
  }

  /**
   * Saves bytes to a File.
   *
   * @param file
   * @param bytes
   * @throws IOException
   */
  public void saveToFile(File file, byte[] bytes) throws IOException {
    FileOutputStream fos = null;
    try {
      fos = new FileOutputStream(file);
      fos.write(bytes);
      fos.flush();
    } finally {
      if (fos != null) {
        try {
          fos.close();
        } catch (Exception zzz) {
        }
      }
    }
  }

  public Writer openWriter(String arg) throws IOException {
    File outputFile = new File(cli.getOptionValue(arg));
    return new OutputStreamWriter(new FileOutputStream(outputFile), "UTF-8");
  }

  public BufferedReader openReader(String arg) throws IOException {
    BufferedReader reader = null;
    String path = cli.getOptionValue(arg);
    if (path != null) {
      reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));
      rememberCloseable(reader);
    }
    return reader;
  }

  public Logger getLog() {
    return log;
  }

  private static Set<String> findClasses(String path, String packageName) throws Exception {
    Set<String> classes = new TreeSet<String>();
    if (path.startsWith("file:") && path.contains("!")) {
      String[] split = path.split("!");
      URL jar = new URL(split[0]);
      ZipInputStream zip = new ZipInputStream(jar.openStream());
      ZipEntry entry;
      while ((entry = zip.getNextEntry()) != null) {
        if (entry.getName().endsWith(".class")) {
          String className = entry.getName().replaceAll("[$].*", "")
            .replaceAll("[.]class", "").replace('/', '.');
          if (className.startsWith(packageName)) {
            classes.add(className);
          }
        }
      }
    } else {
      // when running unit tests
      File dir = new File(path);
      if (dir.isDirectory()) {
        String packagePath = packageName.replace('.', File.separatorChar);
        if (dir.getAbsolutePath().endsWith(packagePath)) {
          for (File file : dir.listFiles()) {
            if (file.getName().endsWith(".class")) {
              String className = file.getName().replaceAll("[$].*", "")
                .replaceAll("[.]class", "").replace('/', '.');
              if (className.indexOf("$") == -1)
                classes.add(packageName + "." + className);
            }
          }
        }
      }
    }
    return classes;
  }
}
