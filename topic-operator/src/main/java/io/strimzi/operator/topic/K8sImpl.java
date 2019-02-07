/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.DoneableKafkaTopic;
import io.strimzi.api.kafka.KafkaTopicList;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.operator.common.model.Labels;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class K8sImpl implements K8s {

    private final static Logger LOGGER = LogManager.getLogger(TopicOperator.class);

    private final LabelPredicate resourcePredicate;
    private final String namespace;

    private KubernetesClient client;

    private Vertx vertx;

    public K8sImpl(Vertx vertx, KubernetesClient client, LabelPredicate resourcePredicate, String namespace) {
        this.vertx = vertx;
        this.client = client;
        this.resourcePredicate = resourcePredicate;
        this.namespace = namespace;
    }

    @Override
    public void createResource(KafkaTopic topicResource, Handler<AsyncResult<Void>> handler) {
        vertx.executeBlocking(future -> {
            try {
                operation().inNamespace(namespace).create(topicResource);
                future.complete();
            } catch (Exception e) {
                future.fail(e);
            }
        }, handler);
    }

    @Override
    public void updateResource(KafkaTopic topicResource, Handler<AsyncResult<Void>> handler) {
        vertx.executeBlocking(future -> {
            try {
                operation().inNamespace(namespace).withName(topicResource.getMetadata().getName()).patch(topicResource);
                future.complete();
            } catch (Exception e) {
                future.fail(e);
            }
        }, handler);
    }

    @Override
    public void deleteResource(ResourceName resourceName, Handler<AsyncResult<Void>> handler) {
        vertx.executeBlocking(future -> {
            try {
                // Delete the resource by the topic name, because neither ZK nor Kafka know the resource name
                operation().inNamespace(namespace).withName(resourceName.toString()).delete();
                future.complete();
            } catch (Exception e) {
                future.fail(e);
            }
        }, handler);
    }

    private MixedOperation<KafkaTopic, KafkaTopicList, DoneableKafkaTopic, Resource<KafkaTopic, DoneableKafkaTopic>> operation() {
        return client.customResources(Crds.topic(), KafkaTopic.class, KafkaTopicList.class, DoneableKafkaTopic.class);
    }

    @Override
    public void listMaps(Handler<AsyncResult<List<KafkaTopic>>> handler) {
        vertx.executeBlocking(future -> {
            try {
                future.complete(operation().inNamespace(namespace).withLabels(resourcePredicate.labels()).list().getItems());
            } catch (Exception e) {
                future.fail(e);
            }
        }, handler);
    }

    @Override
    public void getFromName(String topicName, Handler<AsyncResult<KafkaTopic>> handler) {
        vertx.executeBlocking(future -> {
            try {
                List<KafkaTopic> list = operation().inNamespace(namespace).list().getItems();
                LOGGER.debug("Searching k8s topic with " + Labels.STRIMZI_TOPIC_LABEL + "==" + topicName);
                for (int i = 0; i < list.size(); i++) {
                    // this may be reduced after we will be sure there is always a label set (putting it at the place every reconc)
                    String kafkaTopicNameHash = list.get(i).getMetadata().getLabels().get(Labels.STRIMZI_TOPIC_LABEL);
                    if (kafkaTopicNameHash == null) {
                        LOGGER.debug("Label " + Labels.STRIMZI_TOPIC_LABEL + " not set. Searching by spec.topicName");
                        // we are reconciling fresh topic which does not have set label yet
                        kafkaTopicNameHash = Integer.toString(list.get(i).getSpec().getTopicName().hashCode());
                    }
                    if (kafkaTopicNameHash == null) {
                        LOGGER.debug("Label " + Labels.STRIMZI_TOPIC_LABEL + " nor spec.topicName set. Searching by resource name");
                        // use resource name whether the topicName is not set
                        kafkaTopicNameHash = Integer.toString(list.get(i).getMetadata().getName().hashCode());
                    } else {
                        if (new TopicName(list.get(i).getMetadata().getName()).asMapName().toString().equals(topicName)) {
                            LOGGER.debug("spec.topicName set but  Searching by resource name");
                            // topicName set, but might not be equal to topic name
                            kafkaTopicNameHash = Integer.toString(topicName.hashCode());
                        }
                    }
                    LOGGER.debug("Comparing: " + topicName.hashCode() + " - " + kafkaTopicNameHash + " (" + list.get(i).getMetadata().getName() + ")");
                    if (Integer.toString(topicName.hashCode()).equals(kafkaTopicNameHash)) {
                        LOGGER.debug("Found k8s topic " + list.get(i).getMetadata().getName() + " with {metadata.name hash | spec.topicName hash | " + Labels.STRIMZI_TOPIC_LABEL + "} equal to " + kafkaTopicNameHash);
                        future.complete(list.get(i));
                        return;
                    }
                }
                LOGGER.debug("K8s topic with " + Labels.STRIMZI_TOPIC_LABEL + "==" + topicName + " not found");
                future.complete(null);
            } catch (Exception e) {
                future.fail(e);
            }
        }, handler);

    }

    /**
     * Create the given k8s event
     */
    @Override
    public void createEvent(Event event, Handler<AsyncResult<Void>> handler) {
        vertx.executeBlocking(future -> {
            try {
                try {
                    LOGGER.debug("Creating event {}", event);
                    client.events().inNamespace(namespace).create(event);
                } catch (KubernetesClientException e) {
                    LOGGER.error("Error creating event {}", event, e);
                }
                future.complete();
            } catch (Exception e) {
                future.fail(e);
            }
        }, handler);
    }
}
