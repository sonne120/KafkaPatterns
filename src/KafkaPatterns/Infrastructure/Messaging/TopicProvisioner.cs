using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace KafkaPatterns.Infrastructure.Messaging;

public sealed class TopicProvisioner : IHostedService
{
    private readonly KafkaSettings _settings;
    private readonly ILogger<TopicProvisioner> _logger;
    private readonly int _partitions;
    private readonly string[] _topics;

    public TopicProvisioner(
        IOptions<KafkaSettings> settings,
        ILogger<TopicProvisioner> logger,
        int partitions,
        params string[] topics)
    {
        _settings = settings.Value;
        _logger = logger;
        _partitions = partitions;
        _topics = topics;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        await TopicAdmin.EnsureTopicsAsync(_settings.BootstrapServers, _partitions, _topics);
        _logger.LogInformation("Ensured topic(s) {Topics} with {Partitions} partition(s)",
            string.Join(", ", _topics), _partitions);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
