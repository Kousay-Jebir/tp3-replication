package common.constants;

public final class Constants {
    private Constants() {
    }

    public static final String EXCHANGE_WRITE = "write.exchange";
    public static final String EXCHANGE_READ = "read.exchange";

    public static final String QUEUE_WRITE_FMT = "replica.%s.write";
    public static final String QUEUE_READ_FMT = "replica.%s.read";

}
