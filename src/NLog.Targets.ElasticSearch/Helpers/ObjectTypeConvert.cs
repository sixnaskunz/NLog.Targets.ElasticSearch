﻿namespace NLog.Targets.ElasticSearch.Helpers;

/// <summary>
/// Additional type converter configuration
/// Initializes new instance of <see cref="ObjectTypeConvert"/> where it performs ToString for the input <paramref name="objectType"/>
/// </summary>
[NLogConfigurationItem]
public class ObjectTypeConvert(Type objectType)
{
    /// <summary>
    /// Gets or sets the ObjectType that should override <see cref="JsonConverter"/>
    /// </summary>
    public Type ObjectType { get; set; } = objectType;

    /// <summary>
    /// Gets or sets the JsonConverter to include in <see cref="JsonSerializerSettings"/>
    /// </summary>
    public JsonConverter JsonConverter
    {
        get => _jsonConverter ??= ObjectType != null ? new JsonToStringConverter(ObjectType) : null;
        set => _jsonConverter = value;
    }
    private JsonConverter _jsonConverter;

    /// <summary>
    /// Initializes new instance of <see cref="ObjectTypeConvert"/>
    /// </summary>
    public ObjectTypeConvert()
        : this(null)
    {
    }
}
