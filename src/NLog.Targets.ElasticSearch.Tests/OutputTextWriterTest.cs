namespace NLog.Targets.ElasticSearch.Tests;

public class OutputTextWriterTest(ITestOutputHelper testOutputHelper) : TextWriter
{
    private StringBuilder stringBuilder = new();

    public override Encoding Encoding => Encoding.Unicode;

    public override void Write(char value)
    {
        stringBuilder.Append(value);
    }

    public override void WriteLine(string? value)
    {
        stringBuilder.Append(value);
        Flush();
    }

    public override void Flush()
    {
        StringBuilder sb = stringBuilder;
        if (sb.Length > 0)
        {
            stringBuilder = new StringBuilder();
            testOutputHelper.WriteLine(sb.ToString());
        }
    }

    protected override void Dispose(bool disposing)
    {
        Flush();
        base.Dispose(disposing);
    }
}