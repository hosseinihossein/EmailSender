using System.Text.Json;
using Confluent.Kafka;

namespace NetApp.Helpers;

public class KafkaEmailConsumerService : BackgroundService
{
    private readonly ILogger<KafkaEmailConsumerService> _logger;
    private readonly ConsumerConfig _config;
    private readonly string _topicName;
    private readonly IEmailSender _emailSender;

    public KafkaEmailConsumerService(
        ILogger<KafkaEmailConsumerService> logger,
        IConfiguration configuration,
        IEmailSender emailSender
    )
    {
        _logger = logger;

        // Define Kafka consumer configurations
        _config = new ConsumerConfig
        {
            BootstrapServers = configuration["Kafka:BootstrapServer"] ?? "kafka:9092",
            GroupId = configuration["Kafka:GroupId"] ?? "logger-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true // Or false if you intend to commit manually
        };

        _topicName = configuration["Kafka:EmailTopic"] ?? "email";

        _emailSender = emailSender;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Execute the consumer loop on a background thread to prevent blocking application startup
        return Task.Run(() => ConsumeLoopAsync(stoppingToken), stoppingToken);
    }

    private void ConsumeLoopAsync(CancellationToken stoppingToken)
    {
        // Build the Kafka consumer instance
        using var consumer = new ConsumerBuilder<Ignore, string>(_config)
            .SetErrorHandler((_, e) =>
            {
                _logger.LogError($"Kafka Email Consumer Error: {e.Reason}");
            })
            .Build();

        consumer.Subscribe(_topicName);
        _logger.LogInformation($"Subscribed to topic: {_topicName}");

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    // Polls for new messages. Pass stoppingToken to handle graceful shutdown.
                    var consumeResult = consumer.Consume(stoppingToken);

                    if (consumeResult != null)
                    {
                        //_logger.LogInformation($"Received message: '{consumeResult.Message.Value}' from partition: {consumeResult.Partition}");

                        // TODO: Implement your business logic / message handling here
                        string emailJson = consumeResult.Message.Value;
                        EmailModel? emailModel = JsonSerializer.Deserialize<EmailModel>(emailJson);
                        if (emailModel is null)
                        {
                            _logger.LogWarning($"emailModel can not be null! emailJson: {emailJson}");
                        }
                        else
                        {
                            _emailSender.SendEmail(emailModel.ReceiverName, emailModel.ReceiverEmail,
                            emailModel.Subject, emailModel.Message);
                        }
                    }
                }
                catch (ConsumeException e)
                {
                    // Errors occurring during consumption (e.g., deserialization issues)
                    _logger.LogError($"Email Consume Error occurred during consumption: {e.Error.Reason}");
                }
                catch (Exception e)
                {
                    // Errors occurring during consumption (e.g., deserialization issues)
                    _logger.LogError($"Error occurred during email consumption: {e}");
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Kafka email consumption loop canceled via termination token.");
        }
        finally
        {
            // Close down cleanly, committing remaining offsets and leaving the consumer group gracefully
            consumer.Close();
            _logger.LogWarning("Kafka email consumer connection closed cleanly.");
            consumer.Dispose();
        }
    }
}

public class EmailModel
{
    public string ReceiverName { get; set; } = "User";
    public string ReceiverEmail { get; set; } = null!;
    public string Subject { get; set; } = string.Empty;
    public string Message { get; set; } = string.Empty;
}
