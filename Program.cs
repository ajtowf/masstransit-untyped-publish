using MassTransit;
using MassTransit.Context;
using MassTransit.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System.Net.Mime;
using System.Text;
using System.Text.Json;

namespace MassTransit.Untyped.Publish;

public record MessageEnvelope(string Topic, string Payload);
public record PersonCreated(Guid Id, string Name);
public record OrderPlaced(Guid Id, decimal Amount);
public class PersonCreatedConsumer : IConsumer<PersonCreated>
{
    private readonly CountdownEvent _countdown;

    public PersonCreatedConsumer(CountdownEvent countdown) => _countdown = countdown;

    public Task Consume(ConsumeContext<PersonCreated> context)
    {
        Console.WriteLine($"[x] Received PersonCreated: {context.Message}");
        _countdown.Signal();
        return Task.CompletedTask;
    }
}
public class OrderPlacedConsumer : IConsumer<OrderPlaced>
{
    private readonly CountdownEvent _countdown;

    public OrderPlacedConsumer(CountdownEvent countdown) => _countdown = countdown;

    public Task Consume(ConsumeContext<OrderPlaced> context)
    {
        Console.WriteLine($"[x] Received OrderPlaced: {context.Message}");
        _countdown.Signal();
        return Task.CompletedTask;
    }
}

public class EnvelopeMessageDeserializer : IMessageDeserializer
{
    public ContentType ContentType => new("application/vnd.masstransit+json");

    public ConsumeContext Deserialize(ReceiveContext receiveContext)
    {
        var bodyBytes = receiveContext.GetBody().ToArray();
        var json = Encoding.UTF8.GetString(bodyBytes);
        var envelope = JsonSerializer.Deserialize<MessageEnvelope>(json);

        if (envelope == null || string.IsNullOrWhiteSpace(envelope.Topic))
            throw new InvalidOperationException("Invalid envelope or missing topic.");

        var messageType = Type.GetType(envelope.Topic);
        if (messageType == null)
            throw new InvalidOperationException($"Unknown type: {envelope.Topic}");

        var message = JsonSerializer.Deserialize(envelope.Payload, messageType);
        if (message == null)
            throw new InvalidOperationException("Payload deserialization failed.");

        var method = typeof(MessageConsumeContext<>)
            .MakeGenericType(messageType)
            .GetConstructor(new[] { typeof(ConsumeContext), messageType });

        var consumeContext = (ConsumeContext)method!.Invoke([receiveContext, message]);
        return consumeContext;
    }

    public SerializerContext Deserialize(MessageBody body, Headers headers, Uri? destinationAddress = null) =>
        throw new NotImplementedException("This deserializer only supports ConsumeContext.");

    public MessageBody GetMessageBody(string text) => new StringMessageBody(text);

    public void Probe(ProbeContext context) => context.CreateScope("envelopeDeserializer");

}

public class CustomDeserializerFactory : ISerializerFactory
{
    public ContentType ContentType => new("application/vnd.masstransit+json");

    public IMessageDeserializer CreateDeserializer() => new EnvelopeMessageDeserializer();

    public IMessageSerializer CreateSerializer() => new SystemTextJsonMessageSerializer(); // optional
}

class Program
{
    static async Task Main()
    {
        var countdown = new CountdownEvent(2);

        var host = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddSingleton(countdown);

                services.AddMassTransit(x =>
                {
                    x.SetKebabCaseEndpointNameFormatter();

                    x.AddConsumer<PersonCreatedConsumer>().Endpoint(e => e.Name = typeof(PersonCreatedConsumer).FullName!);
                    x.AddConsumer<OrderPlacedConsumer>().Endpoint(e => e.Name = typeof(OrderPlacedConsumer).FullName!);

                    x.UsingRabbitMq((context, cfg) =>
                    {
                        cfg.Host("localhost", "/", h => { });

                        // Custom deserializer
                        cfg.ClearSerialization();
                    
                        cfg.AddSerializer(new CustomDeserializerFactory());
                        cfg.AddDeserializer(new CustomDeserializerFactory(), isDefault: true);

                        cfg.ReceiveEndpoint("dynamic-message-router", e =>
                        {
                            e.ConfigureConsumer<PersonCreatedConsumer>(context);
                            e.ConfigureConsumer<OrderPlacedConsumer>(context);
                        });
                    });
                });
            })
            .Build();

        await host.StartAsync();

        await Task.Delay(500);

        // Send the message after the bus has started
        var bus = host.Services.GetRequiredService<IBus>();

        MessageEnvelope Wrap<T>(T payload) => new(typeof(T).FullName!, JsonSerializer.Serialize(payload));

        var createdPayload = Wrap(new PersonCreated(Guid.NewGuid(), "Ajden T."));
        var orderPayload = Wrap(new OrderPlaced(Guid.NewGuid(), 2500m));

        var sendendpointProvider = host.Services.GetRequiredService<ISendEndpointProvider>();
        var endpoint = await sendendpointProvider.GetSendEndpoint(new Uri($"exchange:{createdPayload.Topic}"));
        await endpoint.Send(createdPayload);

        endpoint = await sendendpointProvider.GetSendEndpoint(new Uri($"exchange:{orderPayload.Topic}"));
        await endpoint.Send(orderPayload);

        // var personId = Guid.NewGuid();
        // var createdPayload = Wrap(new PersonCreated(personId, "Ajden T."));

        // await bus.Publish((object)createdPayload, ctx => ctx.Headers.Set(MessageHeaders.MessageType, new[] { $"urn:message:{createdPayload.Topic}" }));
        // var sendendpointProvider = host.Services.GetRequiredService<ISendEndpointProvider>();
        // var endpoint = await sendendpointProvider.GetSendEndpoint(new Uri($"exchange:{createdPayload.Topic}"));
        // await endpoint.Send((object)createdPayload.Payload, ctx =>
        // {
        //     ctx.Headers.Set(MessageHeaders.MessageType, new[] { $"urn:message:{createdPayload.Topic}" });
        // });

        Console.WriteLine("[>] Published PersonCreated");
        countdown.Wait(TimeSpan.FromSeconds(5));

        Console.WriteLine("[✓] Message handled. Exiting.");
        await host.StopAsync();
    }
}


