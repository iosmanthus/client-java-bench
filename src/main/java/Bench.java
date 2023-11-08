import org.apache.commons.cli.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

public class Bench {

  private static final Logger logger = LoggerFactory.getLogger(Bench.class);
  private static final String workloadFlow = "flow";
  private static final String optionType = "type";
  private static final String optionPd = "pd";
  private static final String optionThreads = "threads";
  private static final String optionDuration = "duration";
  private static final String optionPrefix = "prefix";
  private static final String optionLog = "log";
  private static final String optionTimeout = "timeout";
  private static final String optionReport = "report";

  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(Option.builder(optionType).hasArg().required().desc("workload type").build());
    options.addOption(Option.builder(optionPd).hasArg().required().desc("pd addresses").build());
    options.addOption(new Option(optionThreads, true, "thread count for the workload"));
    options.addOption(new Option(optionDuration, true, "duration to run"));
    options.addOption(new Option(optionPrefix, true, "key prefix"));
    options.addOption(new Option(optionLog, true, "path of the log file"));
    options.addOption(new Option(optionTimeout, true, "timeout in milliseconds"));
    options.addOption(new Option(optionReport, true, "path of the report file"));
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);
    Opts opts = parseOpts(cmd);

    Properties properties = getProperties(opts.log);
    LogManager.resetConfiguration();
    PropertyConfigurator.configure(properties);

    TiConfiguration conf = getTiConfiguration(opts.pd, opts.timeout);
    try (TiSession session = TiSession.create(conf)) {
      List<byte[]> splits = new ArrayList<>();
      for (int i = 0; i < 25; i++) {
        splits.add(ByteString.copyFromUtf8(String.format("%c", 'a' + i)).toByteArray());
      }
      session.splitRegionAndScatter(splits);
      RawKVClient client = session.createRawClient();
      if (Objects.equals(opts.type, workloadFlow)) {
        Flow.Builder builder = new Flow.Builder()
            .client(client)
            .reportPath(opts.reportPath)
            .prefix(opts.prefix)
            .threads(opts.threads)
            .duration(opts.duration);
        try (Flow f = builder.build()) {
          f.run();
        }
      }
    }
  }

  private static class Opts {

    String type;
    String pd;
    String prefix;
    String log;
    String reportPath;
    int threads;
    int duration;
    int timeout;
  }

  private static Opts parseOpts(CommandLine cmd) {
    Opts args = new Opts();
    args.type = cmd.getOptionValue(optionType);
    args.pd = cmd.getOptionValue(optionPd);
    args.prefix = cmd.getOptionValue(optionPrefix);
    args.log = cmd.getOptionValue(optionLog);
    args.reportPath = cmd.getOptionValue(optionReport);
    if (args.reportPath == null) {
      throw new IllegalArgumentException("report path is required");
    }
    String threads = cmd.getOptionValue(optionThreads);
    if (threads != null) {
      args.threads = Integer.parseInt(threads);
    } else {
      args.threads = 1;
    }

    String duration = cmd.getOptionValue(optionDuration);
    if (duration != null) {
      args.duration = Integer.parseInt(duration);
    } else {
      args.duration = 3600;
    }

    String timeout = cmd.getOptionValue(optionTimeout);
    if (timeout != null) {
      args.timeout = Integer.parseInt(timeout);
    } else {
      args.timeout = 400;
    }

    return args;
  }

  private static TiConfiguration getTiConfiguration(String pdAddress, int timeout) {
    TiConfiguration conf = TiConfiguration.createRawDefault(pdAddress);
    conf.setEnableAtomicForCAS(true);
    conf.setEnableGrpcForward(true);
    conf.setTimeout(150);
    conf.setForwardTimeout(200);
    conf.setRawKVReadTimeoutInMS(timeout);
    conf.setRawKVWriteTimeoutInMS(timeout);
    conf.setRawKVBatchReadTimeoutInMS(timeout);
    conf.setRawKVBatchWriteTimeoutInMS(timeout);
    conf.setRawKVWriteSlowLogInMS(300);
    conf.setRawKVReadSlowLogInMS(300);
    conf.setRawKVBatchReadSlowLogInMS(300);
    conf.setRawKVBatchWriteSlowLogInMS(300);
    conf.setCircuitBreakEnable(true);
    conf.setWarmUpEnable(true);
    conf.setMetricsEnable(true);
    conf.setApiVersion(TiConfiguration.ApiVersion.V2);
    return conf;
  }

  private static Properties getProperties(String logFilePath) {
    Properties properties = new Properties();
    properties.setProperty("log4j.rootLogger", "INFO, A1");
    properties.setProperty("log4j.appender.A1", "org.apache.log4j.ConsoleAppender");
    properties.setProperty("log4j.appender.A1.layout", "org.apache.log4j.PatternLayout");
    properties.setProperty("log4j.appender.A1.layout.ConversionPattern",
        "%d{yyyy-MM-dd HH:mm:ss,SSS} %-4r [%t] %-5p %c %x - %m%n");
    if (logFilePath != null) {
      properties.setProperty("log4j.appender.A1", "org.apache.log4j.FileAppender");
      properties.setProperty("log4j.appender.A1.File", logFilePath);
    }
    return properties;
  }
}
