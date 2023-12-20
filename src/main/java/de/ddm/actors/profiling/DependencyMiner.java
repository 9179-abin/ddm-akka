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
		int id;
		String[] header;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class FileMessage implements Message {
		private static final long serialVersionUID = 4591192372652568030L;
		int id;
		List<Set<String>> fileContent;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class RegistrationMessage implements Message {
		private static final long serialVersionUID = -4025238529984914107L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
		ActorRef<LargeMessageProxy.Message> largeMessageProxy;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class DependencyMessage implements Message {
		private static final long serialVersionUID = 7696173597050572194L;
		ActorRef<LargeMessageProxy.Message> dependencyWorkerLargeMessageProxy;
		ColumnIndex left, right;
		Dependency dependency;
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
		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);

		this.dependencyWorkers = new ArrayList<>();
		context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));
	}

	/////////////////
	// Actor State //
	/////////////////

	private long startTime;

	private final boolean discoverNaryDependencies;
	private final File[] inputFiles;
	private final String[][] headerLines;
	private final Map<Integer, List<Set<String>>> files = new HashMap<>();
	private final Map<ColumnIndex, Map<ColumnIndex, Boolean>> dependencies = new HashMap<>();

	private final List<ActorRef<InputReader.Message>> inputReaders;
	private final ActorRef<ResultCollector.Message> resultCollector;
	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	private final List<ActorRef<DependencyWorker.Message>> dependencyWorkers;

	private final Queue<ActorRef<LargeMessageProxy.Message>> workerQueue = new ArrayDeque<>();
	private final Queue<DependencyWorker.LargeMessage> workQueue = new ArrayDeque<>();



	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(StartMessage.class, this::handle)
				.onMessage(FileMessage.class, this::handle)
				.onMessage(HeaderMessage.class, this::handle)
				.onMessage(RegistrationMessage.class, this::handle)
				.onMessage(DependencyMessage.class, this::handle)
				.onSignal(Terminated.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(StartMessage message) {
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

		for (int h = 0; h < message.getFileContent().size(); h++) {
			for (Map.Entry<Integer, List<Set<String>>> columns: files.entrySet()) {
				for (int j = 0; j < columns.getValue().size(); j++) {
					addToWorkQueue(new DependencyWorker.DependencyMessage(this.largeMessageProxy,
							message.getFileContent().get(h), columns.getValue().get(j), new ColumnIndex(message.getId(), h),
							new ColumnIndex(columns.getKey(), j)));
				}
			}
		}
		for (int i = 0; i < message.getFileContent().size(); i++) {
			for (int j = 0; j < i; j++) {
				addToWorkQueue(new DependencyWorker.DependencyMessage(this.largeMessageProxy,
						message.getFileContent().get(i), message.getFileContent().get(j),
						new ColumnIndex(message.getId(), i), new ColumnIndex(message.getId(), j)));
			}
		}
		files.put(message.getId(), message.getFileContent());
		// Dependency Detect between new and all old
		return this;
	}

	private Behavior<Message> handle(DependencyMessage message) {
		switch (message.getDependency()) {
			case NONE:
				putDependency(message.getLeft(), message.getRight(), false);
				putDependency(message.getRight(), message.getLeft(), false);
				break;
			case RIGHT:
				putDependency(message.getLeft(), message.getRight(), false);
				putDependency(message.getRight(), message.getLeft(), true);
				break;
			case LEFT:
				putDependency(message.getLeft(), message.getRight(), true);
				putDependency(message.getRight(), message.getLeft(), false);
				break;
			case BOTH:
				putDependency(message.getLeft(), message.getRight(), true);
				putDependency(message.getRight(), message.getLeft(), true);
				break;
		}
		giveWorkFromQueue(message.getDependencyWorkerLargeMessageProxy());
		return this;
	}

	private Behavior<Message> handle(RegistrationMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		if (!this.dependencyWorkers.contains(dependencyWorker)) {
			this.dependencyWorkers.add(dependencyWorker);
			this.getContext().watch(dependencyWorker);
			// The worker should get some work ... let me send her something before I figure out what I actually want from her.
			// I probably need to idle the worker for a while, if I do not have work for it right now ... (see master/worker pattern)

			this.giveWorkFromQueue(message.getLargeMessageProxy());
		}
		return this;
	}

	private void putDependency(ColumnIndex left, ColumnIndex right, boolean dependency) {
		dependencies.compute(left, (k, v) -> {
			if (v == null)
				return new HashMap<>(Map.of(right, dependency));
			v.put(right, dependency);
			return v;
		});
		if (!dependency) return;
		InclusionDependency ind = new InclusionDependency(inputFiles[left.getFileId()],
				new String[] {headerLines[left.getFileId()][left.getColumnIndex()]},
				inputFiles[right.getFileId()], new String[] {headerLines[right.getFileId()][right.getColumnIndex()]});
		this.resultCollector.tell(new ResultCollector.ResultMessage(List.of(ind)));
	}

	private void addToWorkQueue(DependencyWorker.LargeMessage message) {
		if (workerQueue.isEmpty()) {
			workQueue.add(message);
		} else {
			largeMessageProxy.tell(new LargeMessageProxy.SendMessage(message, workerQueue.remove()));
		}
	}

	private void giveWorkFromQueue(ActorRef<LargeMessageProxy.Message> largeMessageProxy) {
		if (workQueue.isEmpty() && files.size() == inputFiles.length && workerQueue.size() == this.dependencyWorkers.size() - 1) {
			end();
			System.exit(0);
		}
		if (workQueue.isEmpty()) {
			workerQueue.add(largeMessageProxy);
		} else {
			largeMessageProxy.tell(new LargeMessageProxy.SendMessage(workQueue.remove(), largeMessageProxy));
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
		return this;
	}
}