using FinancialTrackAzureFunction.Services;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace MyNamespace
{
    public static class Function1
    {
        [FunctionName(nameof(Main))]
        public static async Task Main([OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            string payload = context.GetInput<string>();
            var transaction = Mapper.GetDeserializedTransaction(payload);
            if(transaction != null)
            {
                var validator = new Validator();
                var isValidtransaction = validator.Validate(transaction);
                if (isValidtransaction)
                    PublishJsonRabbiqMQ("queue.api.processing.transaction", payload);
                else
                    PublishJsonRabbiqMQ("queue.api.holding.transaction", payload);
            }
        }

        private static void PublishJsonRabbiqMQ(string queuename, string payload)
        {
            string connectionString = Environment.GetEnvironmentVariable("RabbitMQConnection");
            var factory = new ConnectionFactory
            {
                Uri = new Uri(connectionString)
            };
            using(var connection = factory.CreateConnection())
            {
                using(var channel = connection.CreateModel())
                {
                    channel.QueueDeclare(queuename, durable: true, exclusive: false, autoDelete: false, arguments: null);
                    var body = Encoding.UTF8.GetBytes(payload);
                    channel.BasicPublish(exchange: "", routingKey: queuename, basicProperties: null, body: body);
                }
            }
        }

        [FunctionName("RabbitMQTrigger")]
        public static async Task RabbitMQTriggerAsync(
            [RabbitMQTrigger("queue.message.bus", ConnectionStringSetting = "RabbitMQConnection")] BasicDeliverEventArgs args,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger logger
        )
        {
            string message = System.Text.Encoding.UTF8.GetString(args.Body.ToArray());
            string instanceId = GenerateShortInstanceId(message);
            await starter.StartNewAsync(nameof(Main), instanceId, message);
            logger.LogInformation($"Started orchestration with ID = '{instanceId}'.");
        }

        // Compute the SHA256 hash of the message
        private static string GenerateShortInstanceId(string message)
        {
            using(SHA256 sha256 = SHA256.Create())
            {
                byte[] hashBytes = sha256.ComputeHash(Encoding.UTF8.GetBytes(message));
                return BitConverter.ToString(hashBytes).Replace("-", "").Substring(0, 10);
            }
        }
    }
}
