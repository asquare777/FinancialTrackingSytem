using FinancialTrackingSystemMain.Service;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace FinancialTrackingSystemMain
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                ConnectionFactory factory = new ConnectionFactory();
                factory.Uri = new Uri("amqp://guest:guest@localhost:5672");
                factory.ClientProvidedName = "Financial Transaction Tracking App";

                IConnection connection = factory.CreateConnection();
                IModel channel = connection.CreateModel();

                string exchangeName = "amq.topic";
                string routingkey = "rabbitmq.financial.transaction.routingkey";
                string queueName = "queue.api.financial.app";

                channel.ExchangeDeclare(exchangeName, ExchangeType.Topic, true);
                channel.QueueDeclare(queueName, true, false, false, null);
                channel.QueueBind(queueName, exchangeName, routingkey, null);

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (sender, args) =>
                {
                    var body = args.Body.ToArray();
                    string message = Encoding.UTF8.GetString(body);
                    // deserialize the message
                    var transaction = Mapper.GetDeserializedTransaction(message);
                    if (transaction != null)
                    {
                        var validator = new Validator();
                        var isValidtransaction = validator.Validate(transaction);
                        if (isValidtransaction)
                            Service.RabbitMQ.PublishToConsumer("queue.api.processing.transaction", "rabbitmq.financial.sender.processing.routingkey", message);
                        else
                            Service.RabbitMQ.PublishToConsumer("queue.api.holding.transaction", "rabbitmq.financial.sender.holding.routingkey", message);
                        channel.BasicAck(args.DeliveryTag, false);
                    }
                };

                string consumerTag = channel.BasicConsume(queueName, false, consumer);
                Console.ReadLine();

                channel.BasicCancel(consumerTag);
                channel.Close();
                connection.Close();
            }
            catch(Exception ex)
            {
                Service.RabbitMQ.PublishException(ex.Message.ToString());
            }
        }
    }
}