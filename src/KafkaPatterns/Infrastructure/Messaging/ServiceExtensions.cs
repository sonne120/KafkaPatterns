using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;
using KafkaPatterns.Infrastructure.Caching;
using KafkaPatterns.Infrastructure.Persistence;

namespace KafkaPatterns.Infrastructure.Messaging;

public static class ServiceExtensions
{
    public static IServiceCollection AddKafkaInfrastructure(this IServiceCollection services, IConfiguration config)
    {
        services.Configure<KafkaSettings>(options =>
        {
            var section = config.GetSection("Kafka");
            if (section.Exists())
            {
                section.Bind(options);
            }

            if (string.IsNullOrWhiteSpace(options.BootstrapServers) || !section.GetSection("BootstrapServers").Exists())
            {
                options.BootstrapServers = KafkaConfig.BootstrapServers;
            }
        });

    
        services.AddDbContextFactory<CdcDbContext>(options =>
            options.UseInMemoryDatabase("kafka-patterns-cdc"));

        // Redis, or a stand-in for it. IDistributedCache is the same API either way, so the
        // pipeline code never learns which one it got — the connection string is the only switch.
        var redisConnection = config["Redis:ConnectionString"]
                              ?? Environment.GetEnvironmentVariable("REDIS_CONNECTION");

        if (string.IsNullOrWhiteSpace(redisConnection))
        {
            // In-process, no container required. Note the one real difference from Redis: this
            // cache is per-process, so two instances of a service do NOT share a duplicate filter.
            // Harmless here because the durable ledger is the actual guarantee.
            services.AddDistributedMemoryCache();
        }
        else
        {
            services.AddStackExchangeRedisCache(options =>
            {
                options.Configuration = redisConnection;
                options.InstanceName = "kafka-patterns:";
            });
        }

        services.AddSingleton<IIdempotencyCache>(sp => new DistributedIdempotencyCache(
            sp.GetRequiredService<Microsoft.Extensions.Caching.Distributed.IDistributedCache>(),
            sp.GetRequiredService<Microsoft.Extensions.Logging.ILogger<DistributedIdempotencyCache>>()));

        services.AddSingleton<ISerializer, Serializer>();
        services.AddSingleton<IKafkaMessagePub, KafkaMessagePub>();
        services.AddSingleton<DeadLetterProducer>();

        return services;
    }
}
