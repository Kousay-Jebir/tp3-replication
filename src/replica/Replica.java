package replica;

import com.rabbitmq.client.*;
import common.messaging.Message;
import common.messaging.MessageType;
import common.messaging.MessageUtils;
import common.storage.DataStore;
import common.storage.FileDataStore;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;
import common.constants.Constants;

public class Replica {

    public static void main(String[] args) throws IOException, TimeoutException {
        if (args.length < 2) {
            System.err.println("Usage: java replica.Replica <replicaId> <dataDirectory>");
            System.exit(1);
        }
        String replicaId = args[0], dataDir = args[1];
        DataStore store = new FileDataStore(dataDir, replicaId);

        String writeQueueName = String.format(Constants.QUEUE_WRITE_FMT, replicaId);
        String readQueueName = String.format(Constants.QUEUE_READ_FMT, replicaId);
        System.err.println(writeQueueName);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection conn = factory.newConnection()) {

            setupConsumer(
                    conn, Constants.EXCHANGE_WRITE, BuiltinExchangeType.FANOUT,
                    writeQueueName,
                    (msg, props, ch) -> {
                        store.writeLine(msg.getPayload());
                        System.out.printf("Replica %s: écrit \"%s\"%n", replicaId, msg.getPayload());
                    });

            setupConsumer(
                    conn, Constants.EXCHANGE_READ, BuiltinExchangeType.FANOUT,
                    readQueueName,
                    (msg, props, ch) -> {
                        String corrId = props.getCorrelationId();
                        String replyTo = props.getReplyTo();
                        if (corrId == null || replyTo == null)
                            return;

                        if (msg.getType() == MessageType.READ_LAST) {
                            String last = store.readLastLine();
                            sendResponse(ch, replyTo, corrId, last);
                            System.out.printf("Replica %s: répondu ReadLast -> \"%s\"%n", replicaId, last);

                        } else if (msg.getType() == MessageType.READ_ALL) {
                            List<String> all = store.readAllLines();
                            for (String line : all) {
                                sendResponse(ch, replyTo, corrId, line);
                            }
                            System.out.printf("Replica %s: répondu ReadAll (%d lignes)%n", replicaId, all.size());
                        }
                    });

            System.out.printf("Replica %s prêt.%n", replicaId);
            // Bloquer indéfiniment pour laisser tourner les consumers
            Thread.currentThread().join();

        } catch (Exception e) {
            System.err.println("Erreur lors du traitement du message : " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void setupConsumer(
            Connection conn,
            String exchange,
            BuiltinExchangeType type,
            String queueName,
            DeliveryHandler handler) throws IOException {
        Channel ch = conn.createChannel();
        ch.exchangeDeclare(exchange, type);
        ch.queueDeclare(queueName, false, false, false, null);
        ch.queueBind(queueName, exchange, "");
        System.out.printf("   [DEBUG] queue « %s » bound to exchange « %s »%n", queueName, exchange);
        ch.basicConsume(queueName, true,
                (consumerTag, delivery) -> {
                    try {
                        Message msg = MessageUtils.fromBytes(delivery.getBody());
                        handler.handle(msg, delivery.getProperties(), ch);
                    } catch (IOException e) {
                        System.err.println("Erreur lors du traitement du message :");
                        e.printStackTrace();
                    }
                },

                consumerTag -> {
                });
    }

    private static void sendResponse(Channel ch, String replyTo, String corrId, String payload) {
        try {
            Message resp = new Message(MessageType.READ_RESPONSE, payload);
            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                    .correlationId(corrId)
                    .build();
            ch.basicPublish("", replyTo, props, MessageUtils.toBytes(resp));
        } catch (IOException e) {
            System.err.println("Erreur lors de l’envoi de la réponse RPC :");
            e.printStackTrace();
        }
    }

    @FunctionalInterface
    private interface DeliveryHandler {
        void handle(Message msg, AMQP.BasicProperties props, Channel ch) throws IOException;
    }
}
