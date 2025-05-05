package reader;

import com.rabbitmq.client.*;
import common.messaging.Message;
import common.messaging.MessageType;
import common.messaging.MessageUtils;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;
import common.constants.Constants;

public class ClientReader {

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection conn = factory.newConnection();
                Channel channel = conn.createChannel()) {

            channel.exchangeDeclare(Constants.EXCHANGE_READ, BuiltinExchangeType.FANOUT);

            String replyQueue = channel.queueDeclare("", false, true, true, null).getQueue();

            // Génère un identifiant de corrélation unique
            String corrId = UUID.randomUUID().toString();

            Message request = new Message(MessageType.READ_LAST, "");
            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                    .replyTo(replyQueue)
                    .correlationId(corrId)
                    .build();

            // Envoi de la requête
            channel.basicPublish(
                    Constants.EXCHANGE_READ,
                    "",
                    props,
                    MessageUtils.toBytes(request));
            System.out.println("ClientReader: requête READ_LAST envoyée, corrId=" + corrId);

            BlockingQueue<String> responseQueue = new ArrayBlockingQueue<>(1);

            String consumerTag = channel.basicConsume(replyQueue, true, (ctag, delivery) -> {
                if (delivery.getProperties().getCorrelationId().equals(corrId)) {
                    Message resp = MessageUtils.fromBytes(delivery.getBody());
                    if (resp.getType() == MessageType.READ_RESPONSE) {
                        String payload = resp.getPayload();
                        responseQueue.offer(payload);
                    }
                }
            }, ctag -> {
            });

            String result = responseQueue.take();
            System.out.println("ClientReader: réponse reçue -> \"" + result + "\"");
            channel.basicCancel(consumerTag);
        }
    }
}
