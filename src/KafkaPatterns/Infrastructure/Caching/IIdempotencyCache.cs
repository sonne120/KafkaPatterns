using KafkaPatterns.Infrastructure.Messaging;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Logging;

namespace KafkaPatterns.Infrastructure.Caching;


public interface IIdempotencyCache
{
  
    /// Success(true)  -> definitely seen; safe to skip.
    /// Success(false) -> not in the cache; fall through to the durable ledger.
    /// Transient      -> the cache is unreachable; the caller should ALSO fall through, not fail.
  
    Task<Result<bool>> WasProcessedAsync(string key, CancellationToken cancellationToken);

    Task<Result> MarkProcessedAsync(string key, CancellationToken cancellationToken);
}

public sealed class DistributedIdempotencyCache : IIdempotencyCache
{
    private static readonly byte[] Marker = [1];

    private readonly IDistributedCache _cache;
    private readonly ILogger<DistributedIdempotencyCache> _logger;
    private readonly DistributedCacheEntryOptions _entryOptions;

    public DistributedIdempotencyCache(
        IDistributedCache cache,
        ILogger<DistributedIdempotencyCache> logger,
        TimeSpan? retention = null)
    {
        _cache = cache;
        _logger = logger;
        _entryOptions = new DistributedCacheEntryOptions
        {
            AbsoluteExpirationRelativeToNow = retention ?? TimeSpan.FromHours(24)
        };
    }

    public async Task<Result<bool>> WasProcessedAsync(string key, CancellationToken cancellationToken)
    {
        try
        {
            var hit = await _cache.GetAsync(key, cancellationToken);
            return Result.Success(hit is not null);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(ex, "Idempotency cache unreachable while reading {Key}; falling through to the ledger", key);
            return Result.Transient<bool>($"idempotency cache unreachable: {ex.Message}");
        }
    }

    public async Task<Result> MarkProcessedAsync(string key, CancellationToken cancellationToken)
    {
        try
        {
            await _cache.SetAsync(key, Marker, _entryOptions, cancellationToken);
            return Result.Success();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(ex, "Could not record {Key} in the idempotency cache", key);
            return Result.Transient($"idempotency cache unreachable: {ex.Message}");
        }
    }
}
