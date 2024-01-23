namespace NLog.Targets.ElasticSearch.Extensions;

internal static class ExpandoObjectExtensions
{
    /// <summary>
    /// Replaces dot ('.') character in Keys with an underscore ('_') 
    /// </summary>
    /// <returns>ExpandoObject</returns>
    public static ExpandoObject ReplaceDotInKeys(this ExpandoObject obj, bool alwaysCloneObject = true)
    {
        ExpandoObject clone = alwaysCloneObject ? new ExpandoObject() : null;
        foreach (KeyValuePair<string, object> item in obj)
        {
            switch (item.Value)
            {
                case null:
                    if (clone == null)
                        return obj.ReplaceDotInKeys();
                    break;
                case ExpandoObject expandoObject:
                    if (clone == null)
                        return obj.ReplaceDotInKeys();
                    ((IDictionary<string, object>)clone)[item.Key.Replace('.', '_')] = expandoObject.ReplaceDotInKeys();
                    break;
                default:
                    if (item.Key.Contains('.'))
                    {
                        if (clone == null)
                            return obj.ReplaceDotInKeys();
                        ((IDictionary<string, object>)clone)[item.Key.Replace('.', '_')] = item.Value;
                    }
                    else if (clone != null)
                    {
                        ((IDictionary<string, object>)clone)[item.Key] = item.Value;
                    }
                    break;
            }
        }
        return clone ?? obj;
    }
}
