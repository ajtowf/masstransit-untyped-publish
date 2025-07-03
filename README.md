# Setup RabbitMQ
 - docker run -d --hostname my-rabbit --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management

# Run sample
 - dotnet run

## Output
```
Bus started: rabbitmq://localhost/
[>] Published PersonCreated, PersonChanged, PersonDeleted
[x] Received PersonDeleted: 80f016d5-97b8-47e8-b11f-bc5d5f2b5e39
[x] Received PersonCreated: 80f016d5-97b8-47e8-b11f-bc5d5f2b5e39 - Ajden T.
[x] Received PersonChanged: 80f016d5-97b8-47e8-b11f-bc5d5f2b5e39 - Ajden T.
[✓] Message handled. Exiting.
Bus stopped: rabbitmq://localhost/
```
### Created exchanges
![alt text](image.png)

### Created queues
![alt text](image-1.png)

### MessageEnvelope bindings
![alt text](image-2.png)

### Consumer exhange binding
![alt text](image-3.png)