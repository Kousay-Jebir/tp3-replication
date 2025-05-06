package reader;

import com.rabbitmq.client.*;
import common.constants.Constants;
import common.messaging.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

public class ClientReaderV2 {

    private static final int REPLICA_COUNT = 3;
    private static final int LINES_PER_REPLICA = 7; // or however many each replica sends

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection conn = factory.newConnection();
             Channel channel = conn.createChannel()) {

            channel.exchangeDeclare(Constants.EXCHANGE_READ, BuiltinExchangeType.FANOUT);
            String replyQueue = channel.queueDeclare("", false, true, true, null).getQueue();
            String correlationId = UUID.randomUUID().toString();

            Message request = new Message(MessageType.READ_ALL, "");
            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                    .correlationId(correlationId)
                    .replyTo(replyQueue)
                    .build();

            channel.basicPublish(Constants.EXCHANGE_READ, "", props, MessageUtils.toBytes(request));
            System.out.println("ClientReaderV2: 'READ_ALL' request sent");

            BlockingQueue<String> lineQueue = new LinkedBlockingQueue<>();

            // Consume all lines
            String consumerTag = channel.basicConsume(replyQueue, true, (tag, delivery) -> {
                if (correlationId.equals(delivery.getProperties().getCorrelationId())) {
                    Message msg = MessageUtils.fromBytes(delivery.getBody());
                    if (msg.getType() == MessageType.READ_RESPONSE) {
                        String line = msg.getPayload().trim();
                        lineQueue.offer(line);
                    }
                }
            }, tag -> {});

            // Wait to receive REPLICA_COUNT * LINES_PER_REPLICA lines
            int expectedLines = REPLICA_COUNT * LINES_PER_REPLICA;
            List<String> allLines = new ArrayList<>();
            int waitMs = 5000;
            long start = System.currentTimeMillis();
            while (allLines.size() < expectedLines && System.currentTimeMillis() - start < waitMs) {
                String line = lineQueue.poll(500, TimeUnit.MILLISECONDS);
                if (line != null) allLines.add(line);
            }

            channel.basicCancel(consumerTag);

            if (allLines.isEmpty()) {
                System.out.println("No lines received.");
                return;
            }

            // Count frequency across all replicas
            Map<String, Integer> lineCount = new HashMap<>();
            for (String line : allLines) {
                lineCount.merge(line, 1, Integer::sum);
            }

            int majority = REPLICA_COUNT / 2 + 1;
            System.out.println("\nLines in majority (appearing in at least " + majority + " replicas):");
            boolean found = false;
            for (Map.Entry<String, Integer> entry : lineCount.entrySet()) {
                //System.out.println("[DEBUG] Line \"" + entry.getKey() + "\" seen " + entry.getValue() + " times");
                if (entry.getValue() >= majority) {
                    System.out.println(entry.getKey());
                    found = true;
                }
            }

            if (!found) {
                System.out.println("(No lines found in majority)");
            }
        }
    }
}
