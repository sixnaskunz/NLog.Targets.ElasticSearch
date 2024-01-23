namespace NLog.Targets.ElasticSearch;

internal static class ExceptionExtensions
{
    public static Exception FlattenToActualException(this Exception exception)
    {
        if (exception is not AggregateException aggregateException)
            return exception;

        AggregateException flattenException = aggregateException.Flatten();
        if (flattenException.InnerExceptions.Count == 1)
        {
            return flattenException.InnerExceptions[0];
        }

        return flattenException;
    }
}