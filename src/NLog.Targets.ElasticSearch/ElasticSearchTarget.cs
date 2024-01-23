namespace NLog.Targets.ElasticSearch;

/// <summary>
/// NLog Target for writing to ElasticSearch using low level client
/// </summary>
[Target("ElasticSearch")]
public class ElasticSearchTarget : TargetWithLayout, IElasticSearchTarget
{
    private IElasticLowLevelClient _client;
    private Layout _uri = "http://localhost:9200";
    private Layout _cloudId;
    private Layout _username;
    private Layout _password;
    private Layout _apiKeyId;
    private Layout _apiKey;
    private HashSet<string> _excludedProperties = new(["CallerMemberName", "CallerFilePath", "CallerLineNumber", "MachineName", "ThreadId"]);
    private JsonSerializer _jsonSerializer;
    private JsonSerializer _flatJsonSerializer;
    private readonly Lazy<JsonSerializerSettings> _jsonSerializerSettings;
    private readonly Lazy<JsonSerializerSettings> _flatSerializerSettings;

    private JsonSerializer JsonSerializer => _jsonSerializer ??= JsonSerializer.CreateDefault(_jsonSerializerSettings.Value);
    private JsonSerializer JsonSerializerFlat => _flatJsonSerializer ??= JsonSerializer.CreateDefault(_flatSerializerSettings.Value);

    private JsonLayout _documentInfoJsonLayout;

    /// <summary>
    /// Gets or sets a connection string name to retrieve the Uri from.
    ///
    /// Use as an alternative to Uri
    /// </summary>
    [Obsolete("Deprecated. Please use the configsetting layout renderer instead.", true)]
    public string ConnectionStringName { get; set; }

    /// <summary>
    /// Gets or sets the elasticsearch uri, can be multiple comma separated.
    /// </summary>
    public string Uri
    {
        get => (_uri as SimpleLayout)?.Text;
        set
        {
            _uri = value ?? string.Empty;

            if (IsInitialized)
            {
                InitializeTarget();
            }
        }
    }

    /// <summary>
    /// <inheritdoc cref="IElasticSearchTarget.CloudId"/>
    /// </summary>
    public string CloudId
    {
        get => (_cloudId as SimpleLayout)?.Text;
        set
        {
            _cloudId = value ?? string.Empty;

            if (IsInitialized)
            {
                InitializeTarget();
            }
        }
    }

    /// <summary>
    /// Set it to true if ElasticSearch uses BasicAuth
    /// </summary>
    [Obsolete]
    public bool RequireAuth { get; set; }

    /// <summary>
    /// Username for basic auth
    /// </summary>
    public string Username { get => (_username as SimpleLayout)?.Text; set => _username = value ?? string.Empty; }

    /// <summary>
    /// Password for basic auth
    /// </summary>
    public string Password { get => (_password as SimpleLayout)?.Text; set => _password = value ?? string.Empty; }

    /// <inheritdoc />
    public WebProxy Proxy { get; set; }

    /// <summary>
    /// Gets or sets the proxy address
    /// </summary>
    public Layout ProxyAddress { get; set; }

    /// <summary>
    /// Gets or sets the proxy username
    /// </summary>
    public Layout ProxyUserName { get; set; }

    /// <summary>
    /// Gets or sets the proxy password
    /// </summary>
    public Layout ProxyPassword { get; set; }

    /// <summary>
    /// Set it to true to disable proxy detection
    /// </summary>
    public bool DisableAutomaticProxyDetection { get; set; }

    /// <summary>
    /// Set it to true to disable SSL certificate validation
    /// </summary>
    public bool DisableCertificateValidation { get; set; }

    /// <summary>
    /// Set it to true to disable use of ping to checking if node is alive
    /// </summary>
    public bool DisablePing { get; set; }

    /// <summary>
    /// Set it to true to enable HttpCompression (Must be enabled on server)
    /// </summary>
    public bool EnableHttpCompression { get; set; }

    /// <summary>
    /// Set it to true to enable EnableApiVersioningHeader (Enables use of v8+ server)
    /// </summary>
    public bool EnableApiVersioningHeader { get; set; }

    /// <summary>
    /// Gets or sets the name of the elasticsearch index to write to.
    /// </summary>
    [RequiredParameter]
    public Layout Index { get; set; } = "logstash-${date:format=yyyy.MM.dd}";

    /// <summary>
    /// Obsolete and replaced by <see cref="IncludeEventProperties"/> to match NLog naming convention.
    /// Gets or sets whether to include all properties of the log event in the document
    /// </summary>
    [Obsolete("Replaced by IncludeEventProperties")]
    public bool IncludeAllProperties { get => IncludeEventProperties; set => IncludeEventProperties = value; }

    /// <summary>
    /// Gets or sets whether to include LogEvent Properties in the document
    /// </summary>
    public bool IncludeEventProperties { get; set; }

    /// <summary>
    /// Gets or sets a comma separated list of excluded properties when setting <see cref="IElasticSearchTarget.IncludeEventProperties"/>
    /// </summary>
    public string ExcludedProperties { get; set; }

    /// <summary>
    /// Gets or sets the document type for the elasticsearch index.
    /// </summary>
    public Layout DocumentType { get; set; } = "_doc";

    /// <summary>
    /// Gets or sets to only create index for the document if it does not already exist (put if absent). Required when request targets a data stream.
    /// </summary>
    /// <remarks>
    /// Elastic ver. 7.9 is required for using data streams.
    /// </remarks>
    public bool OpCodeCreate { get; set; }

    /// <summary>
    /// Gets or sets the pipeline transformation
    /// </summary>
    public Layout Pipeline { get; set; }

    /// <summary>
    /// Gets or sets a list of additional fields to add to the elasticsearch document.
    /// </summary>
    [ArrayParameter(typeof(Models.Field), "field")]
    public IList<Models.Field> Fields { get; set; } = [];

    /// <summary>
    /// Gets or sets a list of object types and their override of JsonConverter
    /// </summary>
    [ArrayParameter(typeof(ObjectTypeConvert), "typeconverter")]
    public IList<ObjectTypeConvert> ObjectTypeConverters { get; set; }

    /// <summary>
    /// Gets or sets an alternative serializer for the elasticsearch client to use.
    /// </summary>
    public IElasticsearchSerializer ElasticsearchSerializer { get; set; }

    /// <summary>
    /// Gets or sets if exceptions will be rethrown.
    ///
    /// Set it to true if ElasticSearchTarget target is used within FallbackGroup target (https://github.com/NLog/NLog/wiki/FallbackGroup-target).
    /// </summary>
    [Obsolete("No longer needed", true)]
    public bool ThrowExceptions { get; set; }

    /// <summary>
    /// Gets or sets whether it should perform safe object-reflection (-1 = Unsafe, 0 - No Reflection, 1 - Simple Reflection, 2 - Full Reflection)
    /// </summary>
    public int MaxRecursionLimit { get; set; } = -1;

    /// <summary>
    /// Take the raw output from configured JsonLayout and send as document (Instead of creating expando-object for document serialization)
    /// </summary>
    public bool EnableJsonLayout { get; set; }

    /// <summary>
    /// <inheritdoc cref="IElasticSearchTarget.ApiKeyId"/>
    /// </summary>
    public string ApiKeyId { get => (_apiKeyId as SimpleLayout)?.Text; set => _apiKeyId = value ?? string.Empty; }

    /// <summary>
    /// <inheritdoc cref="IElasticSearchTarget.ApiKey"/>
    /// </summary>
    public string ApiKey { get => (_apiKey as SimpleLayout)?.Text; set => _apiKey = value ?? string.Empty; }

    /// <summary>
    /// <inheritdoc cref="IElasticSearchTarget.IncludeDefaultFields"/>
    /// </summary>
    public bool IncludeDefaultFields { get; set; } = true;

    private static readonly char[] separator = [','];

    /// <summary>
    /// Initializes a new instance of the <see cref="ElasticSearchTarget"/> class.
    /// </summary>
    public ElasticSearchTarget()
    {
        Name = "ElasticSearch";
        // OptimizeBufferReuse = true;

        ObjectTypeConverters =
        [
            new(typeof(System.Reflection.Assembly)),     // Skip serializing all types in application
            new(typeof(System.Reflection.Module)),       // Skip serializing all types in application
            new(typeof(System.Reflection.MemberInfo)),   // Skip serializing all types in application
            new(typeof(System.IO.Stream)),               // Skip serializing Stream properties, since they throw
            new(typeof(System.Net.IPAddress)),           // Skip serializing IPAdress properties, since they throw when IPv6 address
        ];

        _jsonSerializerSettings = new Lazy<JsonSerializerSettings>(() => CreateJsonSerializerSettings(false, ObjectTypeConverters), LazyThreadSafetyMode.PublicationOnly);
        _flatSerializerSettings = new Lazy<JsonSerializerSettings>(() => CreateJsonSerializerSettings(true, ObjectTypeConverters), LazyThreadSafetyMode.PublicationOnly);
    }

    /// <inheritdoc />
    protected override void InitializeTarget()
    {
        base.InitializeTarget();

        IConnectionPool connectionPool;

        LogEventInfo eventInfo = LogEventInfo.CreateNullEvent();
        string cloudId = _cloudId?.Render(eventInfo) ?? string.Empty;

        string username = _username?.Render(eventInfo) ?? string.Empty;
        string password = _password?.Render(eventInfo) ?? string.Empty;
        string apiKeyId = _apiKeyId?.Render(eventInfo) ?? string.Empty;
        string apiKey = _apiKey?.Render(eventInfo) ?? string.Empty;

        if (!string.IsNullOrWhiteSpace(cloudId))
        {
            if (!string.IsNullOrWhiteSpace(apiKey) && !string.IsNullOrWhiteSpace(apiKeyId))
            {
                connectionPool = new CloudConnectionPool(cloudId, new ApiKeyAuthenticationCredentials(apiKeyId, apiKey));
            }
            else
            {
                connectionPool = new CloudConnectionPool(cloudId, new BasicAuthenticationCredentials(username, password));
            }
        }
        else
        {
            string uri = _uri?.Render(eventInfo) ?? string.Empty;
            IEnumerable<Uri> nodes = uri.Split(separator, StringSplitOptions.RemoveEmptyEntries).Select(url => new Uri(url));
            connectionPool = new StaticConnectionPool(nodes);
        }

        ConnectionConfiguration config = ElasticsearchSerializer == null
            ? new ConnectionConfiguration(connectionPool)
            : new ConnectionConfiguration(connectionPool, ElasticsearchSerializer);

        if (string.IsNullOrWhiteSpace(cloudId))
        {
            if (!string.IsNullOrWhiteSpace(username))
            {
                config = config.BasicAuthentication(username, password);
            }
            else
            {
                if (!string.IsNullOrWhiteSpace(apiKey) && !string.IsNullOrWhiteSpace(apiKeyId))
                {
                    config = config.ApiKeyAuthentication(apiKeyId, apiKey);
                }
            }
        }

        if (DisableAutomaticProxyDetection)
            config = config.DisableAutomaticProxyDetection();

        if (DisableCertificateValidation)
            config = config.ServerCertificateValidationCallback((o, certificate, chain, errors) => true).ServerCertificateValidationCallback(CertificateValidations.AllowAll);

        if (DisablePing)
            config = config.DisablePing();

        if (Proxy != null)
        {
            if (Proxy.Credentials == null)
            {
                throw new InvalidOperationException("Proxy credentials should be specified.");
            }

            if (!(Proxy.Credentials is NetworkCredential))
            {
                throw new InvalidOperationException($"Type {Proxy.Credentials.GetType().FullName} of proxy credentials isn't supported. Use {typeof(NetworkCredential).FullName} instead.");
            }

            NetworkCredential credential = (NetworkCredential)Proxy.Credentials;
            config = config.Proxy(Proxy.Address, credential.UserName, credential.SecurePassword);
        }
        else if (ProxyAddress != null)
        {
            string proxyAddress = ProxyAddress.Render(eventInfo);
            string proxyUserName = ProxyUserName?.Render(eventInfo) ?? string.Empty;
            string proxyPassword = ProxyPassword?.Render(eventInfo) ?? string.Empty;
            if (!string.IsNullOrEmpty(proxyAddress))
            {
                config = config.Proxy(new Uri(proxyAddress), proxyUserName, proxyPassword);
            }
        }

        if (EnableHttpCompression)
            config = config.EnableHttpCompression();

        if (EnableApiVersioningHeader)
            config = config.EnableApiVersioningHeader();

        _client = new ElasticLowLevelClient(config);

        if (!string.IsNullOrEmpty(ExcludedProperties))
            _excludedProperties = new HashSet<string>(ExcludedProperties.Split(separator, StringSplitOptions.RemoveEmptyEntries));

        if (EnableJsonLayout)
        {
            if (Layout is SimpleLayout)
            {
                InternalLogger.Info("ElasticSearch: Layout-property has type SimpleLayout, instead of the expected JsonLayout");
            }

            _documentInfoJsonLayout = new JsonLayout()
            {
                Attributes = {
                    new JsonAttribute(OpCodeCreate ? "create" : "index", new JsonLayout()
                    {
                        Attributes = {
                            new JsonAttribute("_index", Index) { EscapeForwardSlash = false },
                            new JsonAttribute("_type", DocumentType ?? new SimpleLayout("")) { EscapeForwardSlash = false },
                            new JsonAttribute("pipeline", Pipeline ?? new SimpleLayout("")) { EscapeForwardSlash = false },
                        }
                    }, encode: false)
                }
            };
        }
    }

    /// <inheritdoc />
    protected override void Write(AsyncLogEventInfo logEvent)
    {
        SendBatch([logEvent]);
    }

    /// <inheritdoc />
    protected override void Write(IList<AsyncLogEventInfo> logEvents)
    {
        SendBatch(logEvents);
    }

    private void SendBatch(ICollection<AsyncLogEventInfo> logEvents)
    {
        try
        {
            PostData payload = EnableJsonLayout ? FromPayloadWithJsonLayout(logEvents) : FormPayload(logEvents);

            BulkResponse result = _client.Bulk<BulkResponse>(payload);

            Exception exception = result.ApiCall?.Success ?? false ? null : result.OriginalException ?? new Exception("No error message. Enable Trace logging for more information.");

            if (result.ServerError != null)
            {
                InternalLogger.Error($"ElasticSearch: Server error: {result.ServerError}");
            }

            foreach (BulkResponseItemBase itemWithError in result.ItemsWithErrors)
            {
                InternalLogger.Error($"ElasticSearch: Bulk item failed: index:{itemWithError.Index} result:{itemWithError.Result} type:{itemWithError.Type} error:{itemWithError.Error}");
            }

            if (exception != null)
            {
                InternalLogger.Error(exception.FlattenToActualException(), $"ElasticSearch: Failed to send log messages. Status={result.ApiCall?.HttpStatusCode} Uri={result.ApiCall?.Uri} DebugInformation={result.DebugInformation}");
            }
            else if (InternalLogger.IsTraceEnabled)
            {
                InternalLogger.Trace("ElasticSearch: Send Log DebugInfo={0}", result.DebugInformation);
            }
            else if (InternalLogger.IsDebugEnabled)
            {
                IEnumerable<string> warnings = result.ApiCall?.DeprecationWarnings;
                if (warnings != null && warnings.Any())
                {
                    string warningInfo = string.Join(", ", result.ApiCall.DeprecationWarnings);
                    InternalLogger.Debug("ElasticSearch: Send Log Warnings={0}", warningInfo);
                }
            }

            foreach (AsyncLogEventInfo ev in logEvents)
            {
                ev.Continuation(exception);
            }
        }
        catch (Exception ex)
        {
            InternalLogger.Error(ex.FlattenToActualException(), "ElasticSearch: Error while sending log messages");
            foreach (AsyncLogEventInfo ev in logEvents)
            {
                ev.Continuation(ex);
            }
        }
    }

    private PostData FromPayloadWithJsonLayout(ICollection<AsyncLogEventInfo> logEvents)
    {
        List<string> payload = new(logEvents.Count * 2);    // documentInfo + document

        foreach (AsyncLogEventInfo ev in logEvents)
        {
            LogEventInfo logEvent = ev.LogEvent;

            string documentInfo = RenderLogEvent(_documentInfoJsonLayout, logEvent);
            string document = RenderLogEvent(Layout, logEvent);

            payload.Add(documentInfo);
            payload.Add(document);
        }

        return PostData.MultiJson(payload);
    }

    private PostData FormPayload(ICollection<AsyncLogEventInfo> logEvents)
    {
        List<object> payload = new(logEvents.Count * 2);    // documentInfo + document

        foreach (LogEventInfo logEvent in logEvents.Select(ev => ev.LogEvent))
        {
            string index = RenderLogEvent(Index, logEvent).ToLowerInvariant();
            string documentType = RenderLogEvent(DocumentType, logEvent);
            string pipeLine = RenderLogEvent(Pipeline, logEvent);

            object documentInfo = GenerateDocumentInfo(OpCodeCreate, index, documentType, pipeLine);
            Dictionary<string, object> document = GenerateDocumentProperties(logEvent);

            payload.Add(documentInfo);
            payload.Add(document);
        }

        return PostData.MultiJson(payload);
    }

    private Dictionary<string, object> GenerateDocumentProperties(LogEventInfo logEvent)
    {
        Dictionary<string, object> document = [];

        if (IncludeDefaultFields)
        {
            document.Add("@timestamp", logEvent.TimeStamp);
            document.Add("level", logEvent.Level.Name);
            document.Add("message", RenderLogEvent(Layout, logEvent));
        }

        foreach (Models.Field field in Fields)
        {
            string renderedField = RenderLogEvent(field.Layout, logEvent);

            if (string.IsNullOrWhiteSpace(renderedField))
                continue;

            try
            {
                document[field.Name] = renderedField.ToSystemType(field.LayoutType, logEvent.FormatProvider, JsonSerializer);
            }
            catch (Exception ex)
            {
                _jsonSerializer = null; // Reset as it might now be in bad state
                InternalLogger.Warn(ex, "ElasticSearch: Error while formatting field: {0}", field.Name);
            }
        }

        if (IncludeDefaultFields && logEvent.Exception != null && !document.ContainsKey("exception"))
        {
            document.Add("exception", FormatValueSafe(logEvent.Exception, "exception"));
        }

        if (IncludeEventProperties && logEvent.HasProperties)
        {
            foreach (KeyValuePair<object, object> p in logEvent.Properties)
            {
                string propertyKey = p.Key.ToString();
                if (_excludedProperties.Contains(propertyKey))
                    continue;

                if (document.ContainsKey(propertyKey))
                {
                    propertyKey += "_1";
                    if (document.ContainsKey(propertyKey))
                        continue;
                }

                document[propertyKey] = FormatValueSafe(p.Value, propertyKey);
            }
        }

        return document;
    }

    private static object GenerateDocumentInfo(bool opCodeCreate, string index, string documentType, string pipeLine)
    {
        if (string.IsNullOrEmpty(pipeLine))
        {
            if (string.IsNullOrEmpty(documentType))
            {
                if (opCodeCreate)
                    return new { create = new { _index = index } };
                else
                {
                    return new { index = new { _index = index } };
                }
            }
            else
            {
                if (opCodeCreate)
                    return new { create = new { _index = index, _type = documentType } };
                else
                    return new { index = new { _index = index, _type = documentType } };
            }
        }
        else
        {
            if (string.IsNullOrEmpty(documentType))
            {
                if (opCodeCreate)
                    return new { create = new { _index = index, pipeline = pipeLine } };
                else
                    return new { index = new { _index = index, pipeline = pipeLine } };
            }
            else
            {
                if (opCodeCreate)
                    return new { create = new { _index = index, _type = documentType, pipeline = pipeLine } };
                else
                    return new { index = new { _index = index, _type = documentType, pipeline = pipeLine } };
            }
        }
    }

    private object FormatValueSafe(object value, string propertyName)
    {
        try
        {
            JsonSerializer jsonSerializer = (MaxRecursionLimit == 0 || MaxRecursionLimit == 1) ? JsonSerializerFlat : JsonSerializer;
            return ObjectConverter.FormatValueSafe(value, MaxRecursionLimit, jsonSerializer);
        }
        catch (Exception ex)
        {
            _jsonSerializer = null; // Reset as it might now be in bad state
            _flatJsonSerializer = null;
            InternalLogger.Debug(ex, "ElasticSearch: Error while formatting property: {0}", propertyName);
            return null;
        }
    }

    private static JsonSerializerSettings CreateJsonSerializerSettings(bool specialPropertyResolver, IList<ObjectTypeConvert> objectTypeConverters)
    {
        JsonSerializerSettings jsonSerializerSettings = new() { ReferenceLoopHandling = ReferenceLoopHandling.Ignore, NullValueHandling = NullValueHandling.Ignore, CheckAdditionalContent = true };
        jsonSerializerSettings.Converters.Add(new StringEnumConverter());
        foreach (ObjectTypeConvert typeConverter in objectTypeConverters ?? Array.Empty<ObjectTypeConvert>())
        {
            JsonConverter jsonConverter = typeConverter.JsonConverter;
            if (jsonConverter != null)
                jsonSerializerSettings.Converters.Add(jsonConverter);
            else
                InternalLogger.Debug("ElasticSearch: TypeConverter for {0} has no JsonConverter", typeConverter.ObjectType);
        }
        jsonSerializerSettings.Error = (sender, args) =>
        {
            InternalLogger.Debug(args.ErrorContext.Error, $"ElasticSearch: Error serializing exception property '{args.ErrorContext.Member}', property ignored");
            args.ErrorContext.Handled = true;
        };
        if (specialPropertyResolver)
        {
            jsonSerializerSettings.ContractResolver = new FlatObjectContractResolver();
        }
        return jsonSerializerSettings;
    }
}
