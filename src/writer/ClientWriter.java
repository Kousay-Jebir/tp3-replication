package writer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.Scanner;
import common.constants.Constants;
import common.messaging.Message;
import common.messaging.MessageType;
import common.messaging.MessageUtils;

public class ClientWriter {

    public static void main(String[] args) throws Exception {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
                Channel channel = connection.createChannel();
                Scanner scanner = new Scanner(System.in)) {

            channel.exchangeDeclare(Constants.EXCHANGE_WRITE, "fanout");

            System.out.println("ClientWriter prêt. Tapez une ligne à écrire (ou 'exit' pour quitter) :");

            while (true) {
                System.out.print("> ");
                String input = scanner.nextLine().trim();
                if ("exit".equalsIgnoreCase(input)) {
                    break;
                }
                channel.basicPublish(Constants.EXCHANGE_WRITE, "", null,
                        MessageUtils.toBytes(new Message(MessageType.WRITE_LINE, input)));
                System.out.println("Envoyé : " + input);
            }

            System.out.println("ClientWriter terminé.");
        }
    }
}
