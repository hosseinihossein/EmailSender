using NetApp.Helpers;

namespace NetApp;

public class Program
{
    public static void Main(string[] args)
    {
        var builder = Host.CreateApplicationBuilder(args);

        // **********    EmailSender **********
        builder.Services.AddSingleton<IEmailSender, EmailSender>();

        // ********** Kafka Producer **********
        KafkaProducerService kafkaService = new(builder.Configuration);
        builder.Services.AddSingleton<KafkaProducerService>(kafkaService);

        // ********** Logger **********
        builder.Logging.ClearProviders();
        builder.Logging.AddConsole(); // Keep Console for local visibility
        builder.Logging.AddKafka(kafkaService, builder.Configuration);

        // *********    KafkaEmailConsumer **********
        builder.Services.AddHostedService<KafkaEmailConsumerService>();

        var host = builder.Build();

        // ********** host.Run **********
        Console.WriteLine("host is Running!");
        host.Run();
    }
}
