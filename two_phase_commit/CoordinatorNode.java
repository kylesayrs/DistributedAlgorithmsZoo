import java.util.HashMap;


public class CoordinatorNode {
    HashMap<String, String> nodeList = new HashMap<String, String>();

    Thread requestThread = new Thread(() -> {
        System.out.println("Hello thread");
    });

    public CoordinatorNode() {
        requestThread.start();
    }

    
    static void sendReady() {
        System.out.println("Sending ready");
    }

    public static void main(String[] args) {
        System.out.println("main");

        CoordinatorNode node = new CoordinatorNode();
    }
}
