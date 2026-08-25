namespace KafkaPatterns.Infrastructure;

public static class DemoRunner
{
    public static async Task ShutdownAsync(CancellationTokenSource demoCts, params Task[] workers)
    {
        await demoCts.CancelAsync();

        try
        {
            await Task.WhenAll(workers);
        }
        catch (OperationCanceledException)
        {
           
        }
    }
}
