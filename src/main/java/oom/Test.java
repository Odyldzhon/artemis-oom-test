package oom;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Test implements MqttCallback{
  
  private static long sentMessagesCount = 0;
  private static long receavedMessagesCount = 0;

  public static void main(String[] args) throws Exception {
    String topicName = "testTopic";

    MqttClient subscriber = createMqttClient("subscriber");
    subscriber.connect(createMqttConnectOptions());
    subscriber.setCallback(new Test());
    subscriber.subscribe(topicName);

    MqttClient publisher = createMqttClient("publisher");
    publisher.connect(createMqttConnectOptions());

    byte[] message = "testMessage".getBytes();
    int qos = 1;
    boolean isRetainedMessage = false;
    for (int i = 0; i < 1_000_000; i++) {
      Thread.sleep(5);
      publisher.publish(topicName, message, qos, isRetainedMessage);
      sentMessagesCount++;
      if (sentMessagesCount % 10000 == 0) {
        System.out.println("Number of sent messages : " + sentMessagesCount);
      }
    }
    System.exit(0);
  }

  private static MqttClient createMqttClient(String clientId) throws MqttException {
    String url = "tcp://0.0.0.0:61616";
    MqttClient mqttClient = new MqttClient(url, clientId);
    return mqttClient;
  }

  private static MqttConnectOptions createMqttConnectOptions() {
    MqttConnectOptions options = new MqttConnectOptions();
    options.setKeepAliveInterval(30);
    options.setCleanSession(false);
    options.setAutomaticReconnect(true);
    options.setMaxInflight(1000);
    options.setConnectionTimeout(10); // sec
    return options;
  }

  @Override
  public void connectionLost(Throwable cause) {
  }

  @Override
  public void messageArrived(String topic, MqttMessage message) throws Exception {
    receavedMessagesCount++;
    if (receavedMessagesCount % 10000 == 0) {
      System.out.println("Number of receaved messages : " + receavedMessagesCount);
    }
  }

  @Override
  public void deliveryComplete(IMqttDeliveryToken token) {
  }
}
