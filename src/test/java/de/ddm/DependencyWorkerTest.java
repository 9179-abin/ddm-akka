package de.ddm;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import de.ddm.actors.DataStore;
import de.ddm.actors.profiling.DependencyMiner;
import de.ddm.actors.profiling.DependencyWorker;
import de.ddm.singletons.SystemConfigurationSingleton;
import de.ddm.structures.ColumnIndex;
import de.ddm.structures.Dependency;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Set;

public class DependencyWorkerTest {

    @ClassRule
    public static final TestKitJunitResource testKit = new TestKitJunitResource(SystemConfigurationSingleton.get().toAkkaTestConfig());


    @Test
    public void testRequestData() {
        TestProbe<DataStore.Message> probe = testKit.createTestProbe();

        ActorRef<DependencyWorker.Message> dependencyWorker = testKit.spawn(DependencyWorker.create(probe.getRef()), "dependencyWorker");

        dependencyWorker.tell(new DependencyWorker.DependencyMessage(null, null, null, new ColumnIndex(0, 1), new ColumnIndex(2, 0)));

        DataStore.Message message = probe.receiveMessage();
        Assert.assertEquals(DataStore.GetDataMessage.class, message.getClass());

        DataStore.GetDataMessage getData = (DataStore.GetDataMessage) message;

        Assert.assertEquals(new ColumnIndex(0, 1), getData.getIndex1());
        Assert.assertEquals(new ColumnIndex(2, 0), getData.getIndex2());
        Assert.assertEquals(dependencyWorker, getData.getWorker());
        probe.expectNoMessage();
    }

    @Test
    public void testCalcDependency() {
        TestProbe<DependencyMiner.Message> probe = testKit.createTestProbe();

        ActorRef<DependencyWorker.Message> dependencyWorker = testKit.spawn(DependencyWorker.create(testKit.createTestProbe().getRef().unsafeUpcast()), "dependencyWorker2");

        checkDependencyCase(probe, dependencyWorker, Set.of("1", "2"), Set.of("1", "2"), Dependency.BOTH);
        checkDependencyCase(probe, dependencyWorker, Set.of("1", "3"), Set.of("1", "2"), Dependency.NONE);
        checkDependencyCase(probe, dependencyWorker, Set.of("1"), Set.of("1", "2"), Dependency.LEFT);
        checkDependencyCase(probe, dependencyWorker, Set.of("1", "2"), Set.of("1"), Dependency.RIGHT);
        probe.expectNoMessage();
    }

    private void checkDependencyCase(TestProbe<DependencyMiner.Message> probe, ActorRef<DependencyWorker.Message> dependencyWorker, Set<String> c1, Set<String> c2, Dependency result) {
        dependencyWorker.tell(new DependencyWorker.DependencyMessage(probe.getRef(), c1, c2, new ColumnIndex(0, 1), new ColumnIndex(2, 0)));

        DependencyMiner.Message message = probe.receiveMessage();
        Assert.assertEquals(DependencyMiner.DependencyMessage.class, message.getClass());

        DependencyMiner.DependencyMessage dependencyMessage = (DependencyMiner.DependencyMessage) message;

        Assert.assertEquals(new ColumnIndex(0, 1), dependencyMessage.getLeft());
        Assert.assertEquals(new ColumnIndex(2, 0), dependencyMessage.getRight());
        Assert.assertEquals(dependencyWorker, dependencyMessage.getDependencyWorker());
        Assert.assertEquals(result, dependencyMessage.getDependency());
    }
}
