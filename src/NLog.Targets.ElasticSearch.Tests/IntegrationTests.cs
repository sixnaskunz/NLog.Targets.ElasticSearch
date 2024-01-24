namespace NLog.Targets.ElasticSearch.Tests;

public class IntegrationTests(ITestOutputHelper testOutputHelper)
{
    public class BadLogException : Exception
    {
        public object[] BadArray { get; }
        public System.Reflection.Assembly BadProperty => typeof(BadLogException).Assembly;
        public object ExceptionalBadProperty => throw new System.NotSupportedException();

        public BadLogException()
        {
            BadArray = new object[] { this };
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void ExceptionSerializationTest(bool hasExceptionFieldLayout)
    {
        using OutputTextWriterTest testOutputTextWriter = new(testOutputHelper);
        InternalLogger.LogWriter = testOutputTextWriter;
        InternalLogger.LogLevel = LogLevel.Warn;

        ElasticSearchTarget elasticTarget = new();

        if (hasExceptionFieldLayout)
        {
            elasticTarget.Fields.Add(new Models.Field
            {
                Name = "exception",
                Layout = Layout.FromString("${exception:format=toString,Data:maxInnerExceptionLevel=10}"),
                LayoutType = typeof(string)
            });
        }

        LoggingRule rule = new("*", LogLevel.Info, elasticTarget);

        LoggingConfiguration config = new();
        config.LoggingRules.Add(rule);

        LogManager.ThrowExceptions = true;
        LogManager.Configuration = config;

        Logger logger = LogManager.GetLogger("Example");

        logger.Error(new BadLogException(), "Boom");

        LogManager.Flush();
    }

    [Fact]
    public void SimpleLogTest()
    {
        ElasticSearchTarget elasticTarget = new();

        LoggingRule rule = new("*", elasticTarget);
        rule.EnableLoggingForLevel(LogLevel.Info);

        LoggingConfiguration config = new();
        config.LoggingRules.Add(rule);

        LogManager.ThrowExceptions = true;
        LogManager.Configuration = config;

        Logger logger = LogManager.GetLogger("Example");

        logger.Info("Hello elasticsearch");

        LogManager.Flush();
    }

    [Fact]
    public void SimpleJsonLayoutTest()
    {
        ElasticSearchTarget elasticTarget = new()
        {
            EnableJsonLayout = true,
            Layout = new JsonLayout()
            {
                MaxRecursionLimit = 10,
                // IncludeAllProperties = true,
                IncludeEventProperties = true,
                Attributes =
                {
                    new JsonAttribute("timestamp", "${date:universaltime=true:format=o}"),
                    new JsonAttribute("lvl", "${level}"),
                    new JsonAttribute("msg", "${message}"),
                    new JsonAttribute("logger", "${logger}"),
                    new JsonAttribute("threadid", "${threadid}", false), // Skip quotes for integer-value
                }
            }
        };

        LoggingRule rule = new("*", elasticTarget);
        rule.EnableLoggingForLevel(LogLevel.Info);

        LoggingConfiguration config = new();
        config.LoggingRules.Add(rule);

        LogManager.ThrowExceptions = true;
        LogManager.Configuration = config;

        Logger logger = LogManager.GetLogger("Example");

        logger.Info("Hello elasticsearch");

        LogManager.Flush();
    }

    [Fact]
    public void ExceptionTest()
    {
        ElasticSearchTarget elasticTarget = new();

        LoggingRule rule = new("*", elasticTarget);
        rule.EnableLoggingForLevel(LogLevel.Error);

        LoggingConfiguration config = new();
        config.LoggingRules.Add(rule);

        LogManager.ThrowExceptions = true;
        LogManager.Configuration = config;

        Logger logger = LogManager.GetLogger("Example");

        ArgumentException exception = new("Some random error message");

        logger.Error(exception, "An exception occured");

        LogManager.Flush();
    }

    [Fact]
    public void ReadFromConfigTest()
    {
        LogManager.ThrowExceptions = true;
        LogManager.Configuration = new XmlLoggingConfiguration("NLog.Targets.ElasticSearch.Tests.dll.config");

        Logger logger = LogManager.GetLogger("Example");

        logger.Info("Hello elasticsearch");

        LogManager.Flush();
    }
}