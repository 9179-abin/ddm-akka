package de.ddm.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.actors.profiling.DependencyMiner;
import de.ddm.actors.profiling.DependencyWorker;
import de.ddm.configuration.SystemConfiguration;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.SystemConfigurationSingleton;
import de.ddm.structures.ColumnIndex;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.*;

public class DataStore extends AbstractBehavior<DataStore.Message> {


    public interface Message extends AkkaSerializable {
    }

    public interface LargeMessage extends LargeMessageProxy.LargeMessage, Message {
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    public static class PutDataMessage implements LargeMessage {
        private static final long serialVersionUID = -6807622716750691919L;
        private ColumnIndex index;
        private Set<String> data;
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    public static class GetDataMessage implements Message {
        private static final long serialVersionUID = -1435914603789668380L;
        private ColumnIndex index1;
        private ColumnIndex index2;
        private ActorRef<DependencyWorker.Message> worker;
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    public static class RemoteGetDataMessage implements Message {
        private static final long serialVersionUID = -2861970036951873254L;
        private ColumnIndex index;
        private ActorRef<LargeMessageProxy.Message> largeMessageProxy;
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ReceptionistListingMessage implements Message {
        private static final long serialVersionUID = -646310169830645755L;
        private Receptionist.Listing listing;
    }

    @Getter
    @AllArgsConstructor
    public static class ShutdownMessage implements Message {
        private static final long serialVersionUID = 5465251501735075468L;
    }

    public static final String DEFAULT_NAME = "dataStore";

    public static final ServiceKey<DataStore.Message> dataStoreService = ServiceKey.create(DataStore.Message.class, DEFAULT_NAME + "Service");

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(ReceptionistListingMessage.class, this::handle)
                .onMessage(GetDataMessage.class, this::handle)
                .onMessage(RemoteGetDataMessage.class, this::handle)
                .onMessage(PutDataMessage.class, this::handle)
                .onMessage(ShutdownMessage.class, this::handle)
                .build();
    }

    public static Behavior<Message> create() {
        return Behaviors.setup(DataStore::new);
    }

    private DataStore(ActorContext<Message> context) {
        super(context);
        if (SystemConfigurationSingleton.get().getRole().equals(SystemConfiguration.MASTER_ROLE)) {
            context.getSystem().receptionist().tell(Receptionist.register(dataStoreService, context.getSelf()));
        } else {
            final ActorRef<Receptionist.Listing> listingResponseAdapter = context.messageAdapter(Receptionist.Listing.class, ReceptionistListingMessage::new);
            context.getSystem().receptionist().tell(Receptionist.subscribe(dataStoreService, listingResponseAdapter));
        }
        final ActorRef<Receptionist.Listing> listingResponseAdapter = context.messageAdapter(Receptionist.Listing.class, ReceptionistListingMessage::new);
        context.getSystem().receptionist().tell(Receptionist.subscribe(DependencyMiner.dependencyMinerService, listingResponseAdapter));
        this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);
    }


    private final Map<ColumnIndex, Set<String>> data = new HashMap<>();

    private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

    private ActorRef<Message> masterStore;

    private ActorRef<DependencyMiner.Message> dependencyMiner;

    private final Map<ColumnIndex, Queue<GetDataMessage>> requests = new HashMap<>();

    private Behavior<Message> handle(ReceptionistListingMessage message) {
        if (dependencyMiner == null && message.getListing().isForKey(DependencyMiner.dependencyMinerService)) {
            Set<ActorRef<DependencyMiner.Message>> dependencyMiners = message.getListing()
                    .getServiceInstances(DependencyMiner.dependencyMinerService);
            if (!dependencyMiners.isEmpty()) {
                dependencyMiner = dependencyMiners.iterator().next();
            }
        }
        if (masterStore == null && message.getListing().isForKey(dataStoreService)) {
            Set<ActorRef<DataStore.Message>> dataStores = message.getListing().getServiceInstances(dataStoreService);
            if (!dataStores.isEmpty()) {
                masterStore = dataStores.iterator().next();
            }
        }
        return this;
    }

    private Behavior<Message> handle(PutDataMessage message) {
        this.data.put(message.getIndex(), message.getData());
        Queue<GetDataMessage> requestsWithData = this.requests.get(message.getIndex());
        if (requestsWithData == null) {
            return this;
        }
        while (!requestsWithData.isEmpty()) {
            GetDataMessage request = requestsWithData.remove();
            if (data.containsKey(request.getIndex1()) && data.containsKey(request.getIndex2())) {
                getContext().getSelf().tell(request);
            }
        }
        return this;
    }

    private Behavior<Message> handle(RemoteGetDataMessage message) {
        this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(
                new PutDataMessage(message.getIndex(), this.data.get(message.getIndex())),
                message.getLargeMessageProxy()));
        return this;
    }

    private Behavior<Message> handle(GetDataMessage message) {
        Set<String> column1 = this.data.get(message.getIndex1());
        Set<String> column2 = this.data.get(message.getIndex2());
        if (column1 != null && column2 != null) {
            if (dependencyMiner == null) {
                getContext().getSelf().tell(message);
                return this;
            }
            message.getWorker().tell(new DependencyWorker.DependencyMessage(dependencyMiner, column1, column2,
                    message.getIndex1(), message.getIndex2()));
            return this;
        }
        if (masterStore == null) {
            getContext().getSelf().tell(message);
            return this;
        }
        if (column1 == null) {
            requestData(message, message.getIndex1());
        }
        if (column2 == null) {
            requestData(message, message.getIndex2());
        }
        return this;
    }

    private Behavior<Message> handle(ShutdownMessage message) {
        return Behaviors.stopped();
    }

    private void requestData(GetDataMessage message, ColumnIndex index) {
        this.requests.compute(index, (k, v) -> {
            Queue<GetDataMessage> queue = v;
            if (queue == null) {
                queue = new ArrayDeque<>();
            }
            queue.add(message);
            return queue;
        });
        masterStore.tell(new RemoteGetDataMessage(index, this.largeMessageProxy));
    }
}
