package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.ddm.actors.DataStore;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.singletons.SystemConfigurationSingleton;
import de.ddm.structures.ColumnIndex;
import de.ddm.structures.Dependency;
import de.ddm.structures.InclusionDependency;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.util.*;

public class DependencyMiner extends AbstractBehavior<DependencyMiner.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
	}

	@NoArgsConstructor
	public static class StartMessage implements Message {
		private static final long serialVersionUID = -1963913294517850454L;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class HeaderMessage implements Message {
		private static final long serialVersionUID = -5322425954432915838L;
		private int id;
		private String[] header;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class FileMessage implements Message {
		private static final long serialVersionUID = 4591192372652568030L;
		private int id;
		private List<Set<String>> fileContent;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class RegistrationMessage implements Message {
		private static final long serialVersionUID = -4025238529984914107L;
		private ActorRef<DependencyWorker.Message> dependencyWorker;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class DependencyMessage implements Message {
		private static final long serialVersionUID = 7696173597050572194L;
		private ActorRef<DependencyWorker.Message> dependencyWorker;
		private ColumnIndex left, right;
		private Dependency dependency;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ReceptionistListingMessage implements Message {
		private static final long serialVersionUID = -6043905415647393701L;
		private Receptionist.Listing listing;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyMiner";

	public static final ServiceKey<DependencyMiner.Message> dependencyMinerService = ServiceKey.create(DependencyMiner.Message.class, DEFAULT_NAME + "Service");

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyMiner::new);
	}

	private DependencyMiner(ActorContext<Message> context) {
		super(context);
		this.discoverNaryDependencies = SystemConfigurationSingleton.get().isHardMode();
		this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
		this.headerLines = new String[this.inputFiles.length][];

		this.inputReaders = new ArrayList<>(inputFiles.length);
		for (int id = 0; id < this.inputFiles.length; id++)
			this.inputReaders.add(context.spawn(InputReader.create(id, this.inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));
		this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
		//this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);

		this.dependencyWorkers = new ArrayList<>();
		context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));
		final ActorRef<Receptionist.Listing> listingResponseAdapter = context.messageAdapter(Receptionist.Listing.class, ReceptionistListingMessage::new);
		context.getSystem().receptionist().tell(Receptionist.subscribe(DataStore.dataStoreService, listingResponseAdapter));
	}

	/////////////////
	// Actor State //
	/////////////////

	private long startTime;

	private final boolean discoverNaryDependencies;
	private final File[] inputFiles;
	private final String[][] headerLines;
	private final Map<Integer, List<Set<String>>> files = new HashMap<>();

	private final List<ActorRef<InputReader.Message>> inputReaders;
	private final ActorRef<ResultCollector.Message> resultCollector;
	// private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;
	private ActorRef<DataStore.Message> masterStore;

	private final List<ActorRef<DependencyWorker.Message>> dependencyWorkers;

	private final Queue<ActorRef<DependencyWorker.Message>> workerQueue = new ArrayDeque<>();
	private final Queue<DependencyWorker.Message> workQueue = new ArrayDeque<>();


	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ReceptionistListingMessage.class, this::handle)
				.onMessage(StartMessage.class, this::handle)
				.onMessage(FileMessage.class, this::handle)
				.onMessage(HeaderMessage.class, this::handle)
				.onMessage(RegistrationMessage.class, this::handle)
				.onMessage(DependencyMessage.class, this::handle)
				.onSignal(Terminated.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(ReceptionistListingMessage message) {
		if (masterStore == null && message.getListing().isForKey(DataStore.dataStoreService)) {
			Set<ActorRef<DataStore.Message>> dataStores = message.getListing().getServiceInstances(DataStore.dataStoreService);
			if (!dataStores.isEmpty()) {
				masterStore = dataStores.iterator().next();
			}
		}
		return this;
	}

	private Behavior<Message> handle(StartMessage message) {
		if (masterStore == null) {
			getContext().getSelf().tell(message);
			return this;
		}
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadHeaderMessage(this.getContext().getSelf()));
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadFileMessage(this.getContext().getSelf()));
		this.startTime = System.currentTimeMillis();
		return this;
	}

	private Behavior<Message> handle(HeaderMessage message) {
		this.headerLines[message.getId()] = message.getHeader();
		return this;
	}

	private Behavior<Message> handle(FileMessage message) {
		// Ignoring batch content for now ... but I could do so much with it.
		for (int i = 0; i < message.getFileContent().size(); i++) {
			masterStore.tell(new DataStore.PutDataMessage(new ColumnIndex(message.getId(), i), message.getFileContent().get(i)));
		}

		for (int h = 0; h < message.getFileContent().size(); h++) {
			for (Map.Entry<Integer, List<Set<String>>> columns: files.entrySet()) {
				for (int j = 0; j < columns.getValue().size(); j++) {
					addToWorkQueue(new DependencyWorker.DependencyMessage(getContext().getSelf(),
							null, null, new ColumnIndex(message.getId(), h),
							new ColumnIndex(columns.getKey(), j)));
				}
			}
		}
		for (int i = 0; i < message.getFileContent().size(); i++) {
			for (int j = 0; j < i; j++) {
				addToWorkQueue(new DependencyWorker.DependencyMessage(getContext().getSelf(),
						null, null,
						new ColumnIndex(message.getId(), i), new ColumnIndex(message.getId(), j)));
			}
		}
		files.put(message.getId(), message.getFileContent());
		// Dependency Detect between new and all old
		return this;
	}

	private Behavior<Message> handle(DependencyMessage message) {
		switch (message.getDependency()) {
			case RIGHT:
				putDependency(message.getRight(), message.getLeft());
				break;
			case LEFT:
				putDependency(message.getLeft(), message.getRight());
				break;
			case BOTH:
				putDependency(message.getLeft(), message.getRight());
				putDependency(message.getRight(), message.getLeft());
				break;
		}
		giveWorkFromQueue(message.getDependencyWorker());
		return this;
	}

	private Behavior<Message> handle(RegistrationMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		if (!this.dependencyWorkers.contains(dependencyWorker)) {
			this.dependencyWorkers.add(dependencyWorker);
			this.getContext().watch(dependencyWorker);
			// The worker should get some work ... let me send her something before I figure out what I actually want from her.
			// I probably need to idle the worker for a while, if I do not have work for it right now ... (see master/worker pattern)

			this.giveWorkFromQueue(message.getDependencyWorker());
		}
		return this;
	}

	private void putDependency(ColumnIndex left, ColumnIndex right) {
		InclusionDependency ind = new InclusionDependency(inputFiles[left.getFileId()],
				new String[] {headerLines[left.getFileId()][left.getColumnIndex()]},
				inputFiles[right.getFileId()], new String[] {headerLines[right.getFileId()][right.getColumnIndex()]});
		this.resultCollector.tell(new ResultCollector.ResultMessage(List.of(ind)));
	}

	private void addToWorkQueue(DependencyWorker.Message message) {
		if (workerQueue.isEmpty()) {
			workQueue.add(message);
		} else {
			workerQueue.remove().tell(message);
		}
	}

	private void giveWorkFromQueue(ActorRef<DependencyWorker.Message> dependencyWorker) {
		if (workQueue.isEmpty() && files.size() == inputFiles.length && workerQueue.size() == this.dependencyWorkers.size() - 1) { // last returns so is not in queue
			end();
			System.exit(0); // apparently someone set some reasonable shutdown hook, probably akka
		}
		if (this.workQueue.isEmpty()) {
			this.workerQueue.add(dependencyWorker);
		} else {
			dependencyWorker.tell(this.workQueue.remove());
		}
	}

	private void end() {
		this.resultCollector.tell(new ResultCollector.FinalizeMessage());
		long discoveryTime = System.currentTimeMillis() - this.startTime;
		this.getContext().getLog().info("Finished mining within {} ms!", discoveryTime);
	}

	private Behavior<Message> handle(Terminated signal) {
		ActorRef<DependencyWorker.Message> dependencyWorker = signal.getRef().unsafeUpcast();
		this.dependencyWorkers.remove(dependencyWorker);
		this.workerQueue.remove(dependencyWorker);
		return this;
	}
}