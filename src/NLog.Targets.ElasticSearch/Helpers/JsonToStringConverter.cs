namespace NLog.Targets.ElasticSearch.Helpers;

internal sealed class JsonToStringConverter(Type type) : JsonConverter
{

    /// <inheritdoc />
    public override bool CanRead { get; } = false;

    /// <inheritdoc />
    public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
    {
        if (value == null)
        {
            writer.WriteNull();
        }
        else
        {
            // Convert into a JSON object, so it can be converted back to ExpandoObject
            writer.WriteStartObject();
            writer.WritePropertyName(type.Name);
            writer.WriteValue(value.ToString());
            writer.WriteEndObject();
        }
    }

    /// <inheritdoc />
    public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
    {
        throw new NotSupportedException("Only serialization is supported");
    }

    /// <inheritdoc />
    public override bool CanConvert(Type objectType)
    {
        return type.IsAssignableFrom(objectType);
    }
}
