package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.actors.DataStore;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.structures.ColumnIndex;
import de.ddm.structures.Dependency;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public class DependencyWorker extends AbstractBehavior<DependencyWorker.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable {
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class DependencyMessage implements Message {
		private static final long serialVersionUID = -5120321521679530290L;
		private ActorRef<DependencyMiner.Message> dependencyMiner;
		private Set<String> column1, column2;
		private ColumnIndex left, right;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ReceptionistListingMessage implements Message {
		private static final long serialVersionUID = -3239136855758589337L;
		private Receptionist.Listing listing;
	}

	@Getter
	@AllArgsConstructor
	public static class ShutdownMessage implements Message {
		private static final long serialVersionUID = -3785894828471650658L;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyWorker";

	public static Behavior<Message> create(ActorRef<DataStore.Message> localDataStore) {
		return Behaviors.setup(context -> new DependencyWorker(context, localDataStore));
	}

	private DependencyWorker(ActorContext<Message> context, ActorRef<DataStore.Message> localDataStore) {
		super(context);
		this.localDataStore = localDataStore;
		final ActorRef<Receptionist.Listing> listingResponseAdapter = context.messageAdapter(Receptionist.Listing.class, ReceptionistListingMessage::new);
		context.getSystem().receptionist().tell(Receptionist.subscribe(DependencyMiner.dependencyMinerService, listingResponseAdapter));
	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef<DataStore.Message> localDataStore;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ReceptionistListingMessage.class, this::handle)
				.onMessage(DependencyMessage.class, this::handle)
				.onMessage(ShutdownMessage.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(ReceptionistListingMessage message) {
		Set<ActorRef<DependencyMiner.Message>> dependencyMiners = message.getListing().getServiceInstances(DependencyMiner.dependencyMinerService);
		for (ActorRef<DependencyMiner.Message> dependencyMiner : dependencyMiners)
			dependencyMiner.tell(new DependencyMiner.RegistrationMessage(this.getContext().getSelf()));
		return this;
	}

	private Behavior<Message> handle(DependencyMessage message) {
		if (message.getColumn1() == null || message.getColumn2() == null) {
			localDataStore.tell(new DataStore.GetDataMessage(message.getLeft(), message.getRight(), getContext().getSelf()));
			return this;
		}
		Dependency result = message.column1.size() < message.column2.size() ?
				message.column2.containsAll(message.column1) ?
						Dependency.LEFT
						: Dependency.NONE
				: message.column1.containsAll(message.column2) ?
				message.column1.size() == message.column2.size() ?
						Dependency.BOTH
						: Dependency.RIGHT
				: Dependency.NONE;
		message.getDependencyMiner().tell(new DependencyMiner.DependencyMessage(this.getContext().getSelf(),
				message.getLeft(), message.getRight(), result));
		return this;
	}

	private Behavior<Message> handle(ShutdownMessage message) {
		return Behaviors.stopped();
	}
}
