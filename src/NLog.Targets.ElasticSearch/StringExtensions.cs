namespace NLog.Targets.ElasticSearch;

internal static class StringExtensions
{
    public static object ToSystemType(this string field, Type type, IFormatProvider formatProvider, JsonSerializer jsonSerializer)
    {
        formatProvider ??= CultureInfo.CurrentCulture;

        return type.FullName switch
        {
            "System.Boolean" => Convert.ToBoolean(field, formatProvider),
            "System.Double" => Convert.ToDouble(field, formatProvider),
            "System.DateTime" => Convert.ToDateTime(field, formatProvider),
            "System.Int32" => Convert.ToInt32(field, formatProvider),
            "System.Int64" => Convert.ToInt64(field, formatProvider),
            "System.Object" => field.ToExpandoObject(jsonSerializer),
            _ => field,
        };
    }

    public static ExpandoObject ToExpandoObject(this string field, JsonSerializer jsonSerializer)
    {
        using JsonTextReader reader = new(new StringReader(field));
        return ((ExpandoObject)jsonSerializer.Deserialize(reader, typeof(ExpandoObject))).ReplaceDotInKeys(alwaysCloneObject: false);
    }
}