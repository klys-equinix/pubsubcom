package com.lucidworks.cloud.archiver.webcontrol;

import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.PubsubMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.gcp.pubsub.PubSubAdmin;
import org.springframework.cloud.gcp.pubsub.core.PubSubTemplate;
import org.springframework.cloud.gcp.pubsub.support.AcknowledgeablePubsubMessage;
import org.springframework.http.ResponseEntity;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.view.RedirectView;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@RestController
public class WebController {


    private final PubSubTemplate pubSubTemplate;

    private final PubSubAdmin pubSubAdmin;

    private final ArrayList<Subscriber> allSubscribers;

    @Autowired
    public WebController(PubSubTemplate pubSubTemplate, PubSubAdmin pubSubAdmin) {
        this.pubSubTemplate = pubSubTemplate;
        this.pubSubAdmin = pubSubAdmin;
        this.allSubscribers = new ArrayList<>();
    }

    @PostMapping("/createTopic")
    public ResponseEntity<String> createTopic(@RequestParam("topicName") String topicName) {
        this.pubSubAdmin.createTopic(topicName);

        return ResponseEntity.ok("Messages published asynchronously; status unknown.");
    }

    @PostMapping("/createSubscription")
    public ResponseEntity<String> createSubscription(@RequestParam("topicName") String topicName,
                                                     @RequestParam("subscriptionName") String subscriptionName) {
        this.pubSubAdmin.createSubscription(subscriptionName, topicName);

        return ResponseEntity.ok("Messages published asynchronously; status unknown.");
    }

    @PostMapping("/postMessage")
    public ResponseEntity publish(
            @RequestParam("topicName") String topicName,
            @RequestParam("message") String message,
            @RequestBody Map<String, String> headers
    ) {
        this.pubSubTemplate.publish(topicName, message, headers);

        return ResponseEntity.ok("Messages published asynchronously; status unknown.");
    }

    @GetMapping("/pull")
    public List<Object> pull(@RequestParam("subscription") String subscriptionName) {

        Collection<AcknowledgeablePubsubMessage> messages = this.pubSubTemplate.pull(subscriptionName, 10, true);

        RedirectView returnView;
        try {
            ListenableFuture<Void> ackFuture = this.pubSubTemplate.ack(messages);
            ackFuture.get();
        } catch (Exception ex) {
            return null;
        }

        return messages.stream().map(pm ->
                Map.of("documentId", pm.getPubsubMessage().getAttributesMap().get("documentId"), "body", pm.getPubsubMessage().getData().toStringUtf8())).collect(Collectors.toList());
    }

    @GetMapping("/multipull")
    public RedirectView multipull(
            @RequestParam("subscription1") String subscriptionName1,
            @RequestParam("subscription2") String subscriptionName2) {

        Set<AcknowledgeablePubsubMessage> mixedSubscriptionMessages = new HashSet<>();
        mixedSubscriptionMessages.addAll(this.pubSubTemplate.pull(subscriptionName1, 1000, true));
        mixedSubscriptionMessages.addAll(this.pubSubTemplate.pull(subscriptionName2, 1000, true));

        if (mixedSubscriptionMessages.isEmpty()) {
            return buildStatusView("No messages available for retrieval.");
        }

        RedirectView returnView;
        try {
            ListenableFuture<Void> ackFuture = this.pubSubTemplate.ack(mixedSubscriptionMessages);
            ackFuture.get();
            returnView = buildStatusView(
                    String.format("Pulled and acked %s message(s)", mixedSubscriptionMessages.size()));
        } catch (Exception ex) {
            returnView = buildStatusView("Acking failed");
        }

        return returnView;
    }

    @GetMapping("/subscribe")
    public RedirectView subscribe(@RequestParam("subscription") String subscriptionName) {
        Subscriber subscriber = this.pubSubTemplate.subscribe(subscriptionName, message -> {
            message.ack();
        });

        this.allSubscribers.add(subscriber);
        return buildStatusView("Subscribed.");
    }

    @PostMapping("/deleteTopic")
    public RedirectView deleteTopic(@RequestParam("topic") String topicName) {
        this.pubSubAdmin.deleteTopic(topicName);

        return buildStatusView("Topic deleted successfully.");
    }

    @PostMapping("/deleteSubscription")
    public RedirectView deleteSubscription(@RequestParam("subscription") String subscriptionName) {
        this.pubSubAdmin.deleteSubscription(subscriptionName);

        return buildStatusView("Subscription deleted successfully.");
    }

    private RedirectView buildStatusView(String statusMessage) {
        RedirectView view = new RedirectView("/");
        view.addStaticAttribute("statusMessage", statusMessage);
        return view;
    }
}