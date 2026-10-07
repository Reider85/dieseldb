import java.nio.file.StandardOpenOption;
public class TestStandardOpenOption {
    public static void main(String[] args) {
        System.out.println("ATOMIC_MOVE: " + StandardOpenOption.ATOMIC_MOVE);
        System.out.println("REPLACE_EXISTING: " + StandardOpenOption.REPLACE_EXISTING);
    }
}