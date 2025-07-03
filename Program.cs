using MassTransit;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System.Text.Json;
using System.Threading;

namespace MassTransit.Untyped.Publish;

public record PersonCreated(Guid Id, string Name);
public record PersonChanged(Guid Id, string Name);
public record PersonDeleted(Guid Id);
public record MessageEnvelope(string Topic, string Payload);

public abstract class AbstractConsumer<T> : IConsumer<MessageEnvelope>
{
    public async Task Consume(ConsumeContext<MessageEnvelope> context)
    {
        var envelope = context.Message;

        if (envelope.Topic != typeof(T).FullName)
            return;

        var obj = JsonSerializer.Deserialize<T>(envelope.Payload);
        if (obj != null)
            await Handle(context, obj);
    }

    protected abstract Task Handle(ConsumeContext<MessageEnvelope> context, T message);
}

public class PersonCreatedConsumer : AbstractConsumer<PersonCreated>
{
    private readonly CountdownEvent _countdown;

    public PersonCreatedConsumer(CountdownEvent countdown) => _countdown = countdown;

    protected override Task Handle(ConsumeContext<MessageEnvelope> context, PersonCreated message)
    {
        Console.WriteLine($"[x] Received PersonCreated: {message.Id} - {message.Name}");
        _countdown.Signal();
        return Task.CompletedTask;
    }
}

public class PersonChangedConsumer : AbstractConsumer<PersonChanged>
{
    private readonly CountdownEvent _countdown;

    public PersonChangedConsumer(CountdownEvent countdown) => _countdown = countdown;

    protected override Task Handle(ConsumeContext<MessageEnvelope> context, PersonChanged message)
    {
        Console.WriteLine($"[x] Received PersonChanged: {message.Id} - {message.Name}");
        _countdown.Signal();
        return Task.CompletedTask;
    }
}

public class PersonDeletedConsumer : AbstractConsumer<PersonDeleted>
{
    private readonly CountdownEvent _countdown;

    public PersonDeletedConsumer(CountdownEvent countdown) => _countdown = countdown;

    protected override Task Handle(ConsumeContext<MessageEnvelope> context, PersonDeleted message)
    {
        Console.WriteLine($"[x] Received PersonDeleted: {message.Id}");
        _countdown.Signal();
        return Task.CompletedTask;
    }
}


class Program
{
    static async Task Main()
    {
        var countdown = new CountdownEvent(3);

        var host = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddSingleton(countdown);

                services.AddMassTransit(x =>
                {
                    x.SetKebabCaseEndpointNameFormatter();

                    x.AddConsumer<PersonCreatedConsumer>().Endpoint(e => e.Name = typeof(PersonCreatedConsumer).FullName!);
                    x.AddConsumer<PersonChangedConsumer>().Endpoint(e => e.Name = typeof(PersonChangedConsumer).FullName!);
                    x.AddConsumer<PersonDeletedConsumer>().Endpoint(e => e.Name = typeof(PersonDeletedConsumer).FullName!);

                    x.UsingRabbitMq((context, cfg) =>
                    {
                        cfg.Host("localhost", "/", h => { });

                        cfg.ConfigureEndpoints(context);
                    });
                });
            })
            .Build();

        await host.StartAsync();

        await Task.Delay(500);

        // Send the message after the bus has started
        var bus = host.Services.GetRequiredService<IBus>();        

        MessageEnvelope Wrap<T>(T payload) => new(typeof(T).FullName!, JsonSerializer.Serialize(payload));

        var personId = Guid.NewGuid();
        var createdPayload = Wrap(new PersonCreated(personId, "Ajden T."));
        var changedPayload = Wrap(new PersonChanged(personId, "Ajden T."));
        var deletedPayload = Wrap(new PersonDeleted(personId));

        // This is the "untyped" part, where the publisher doesn't need to know the type
        // It just passes along the serialized json along with the type name embedded in the envelope.
        await bus.Publish(createdPayload);
        await bus.Publish(changedPayload);
        await bus.Publish(deletedPayload);

        Console.WriteLine("[>] Published PersonCreated, PersonChanged, PersonDeleted");
        countdown.Wait();

        Console.WriteLine("[✓] Message handled. Exiting.");
        await host.StopAsync();
    }
}


