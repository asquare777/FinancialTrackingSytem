using FinancialTrackAzureFunction.Models;
using FinancialTrackAzureFunction.Models.Tenant;
using FinancialTrackAzureFunction.Services;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FinancialTrackAzureFunction
{
    public static class Function1
    {
        [FunctionName(nameof(FinancialTransaction))]
        public static async Task FinancialTransaction(
            [OrchestrationTrigger] IDurableOrchestrationContext context,
            [DurableClient] IDurableEntityClient  entityClient
            )
        {
            string payload = context.GetInput<string>();
            var transaction = Mapper.GetDeserializedTransaction(payload);
            if(transaction != null)
            {
                var validator = new Validator();
                var isValidtransaction = await validator.Validate(transaction, entityClient);
                // call the final procedure activity
                var serializedTransaction = JsonConvert.SerializeObject(transaction);
                if (isValidtransaction)
                    PublishJsonRabbiqMQ("queue.api.processing.transaction", serializedTransaction);
                else
                    PublishJsonRabbiqMQ("queue.api.holding.transaction", serializedTransaction);
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

        [FunctionName("VelocityLimitEntity")]
        public static void VelocityLimitEntity([EntityTrigger] IDurableEntityContext entityContext)
        {
            switch(entityContext.OperationName.ToLowerInvariant())
            {
                case "set":
                    entityContext.SetState(entityContext.GetInput<decimal>());
                    break;
                case "get":
                    entityContext.Return(entityContext.GetState<decimal>());
                    break;
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
            await starter.StartNewAsync(nameof(FinancialTransaction), instanceId, message);
            logger.LogInformation($"Started orchestration with ID = '{instanceId}'.");
        }

        [FunctionName("ClearAllRunningInstances")]
        public static async Task<IActionResult> ClearAllRunningInstances(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequest req,
            [DurableClient] IDurableClient durableClient,
            ILogger log)
        {
            // Get all running instances
            var queryCondition = new OrchestrationStatusQueryCondition()
            {
                RuntimeStatus = new[] { OrchestrationRuntimeStatus.Running }
            };
            var runningInstances = await durableClient.ListInstancesAsync(queryCondition, CancellationToken.None);

            // Terminate each running instance
            List<Task> terminationTasks = new List<Task>();
            foreach (var instance in runningInstances.DurableOrchestrationState)
            {
                terminationTasks.Add(durableClient.TerminateAsync(instance.InstanceId, "Clearing all running instances"));
            }

            // Wait for all termination tasks to complete
            await Task.WhenAll(terminationTasks);

            // Return response
            return new OkObjectResult($"Cleared {runningInstances.DurableOrchestrationState.Count()} running instances.");
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
