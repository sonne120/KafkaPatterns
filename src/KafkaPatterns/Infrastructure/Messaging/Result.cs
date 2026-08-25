using System.Text.Json.Serialization;

namespace KafkaPatterns.Infrastructure.Messaging;

public class Result
{
    public bool IsSuccess { get; }

    [JsonIgnore]
    public bool IsFailure => !IsSuccess;

    [JsonIgnore]
    public bool IsTransient { get; }

    public string Error { get; }

    protected Result(bool isSuccess, string error, bool isTransient = false)
    {
        IsSuccess = isSuccess;
        Error = error;
        IsTransient = isTransient;
    }

    public static Result Success() => new(true, string.Empty);
    public static Result Failure(string error) => new(false, error);
    public static Result Transient(string error) => new(false, error, isTransient: true);

    public static Result<T> Success<T>(T value) => new(value, true, string.Empty);
    public static Result<T> Failure<T>(string error) => new(default!, false, error);
    public static Result<T> Transient<T>(string error) => new(default!, false, error, isTransient: true);

    public override string ToString() =>
        IsSuccess ? "Success" : $"{(IsTransient ? "Transient" : "Failure")}: {Error}";
}

public class Result<T> : Result
{
    private readonly T _value;

    public T Value => IsSuccess
        ? _value
        : throw new InvalidOperationException($"Cannot read Value of a failed Result: {Error}");

    protected internal Result(T value, bool isSuccess, string error, bool isTransient = false)
        : base(isSuccess, error, isTransient)
    {
        _value = value;
    }
}
