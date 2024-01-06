package de.ddm;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.actors.profiling.DataStore;
import de.ddm.actors.profiling.DependencyMiner;
import de.ddm.actors.profiling.DependencyWorker;
import de.ddm.configuration.SystemConfiguration;
import de.ddm.singletons.SystemConfigurationSingleton;
import de.ddm.structures.ColumnIndex;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Set;

public class DataStoreTest {

    @ClassRule
    public static final TestKitJunitResource testKit = new TestKitJunitResource(SystemConfigurationSingleton.get().toAkkaTestConfig());

    @Test
    public void testDataStore() {
        SystemConfigurationSingleton.get().setRole(SystemConfiguration.WORKER_ROLE);
        TestProbe<DependencyMiner.Message> miner = testKit.createTestProbe();
        TestProbe<DataStore.Message> masterStore = testKit.createTestProbe();
        TestProbe<DependencyWorker.Message> worker = testKit.createTestProbe();

        testKit.system().receptionist().tell(Receptionist.register(DependencyMiner.dependencyMinerService, miner.getRef()));
        testKit.system().receptionist().tell(Receptionist.register(DataStore.dataStoreService, masterStore.getRef()));

        ActorRef<DataStore.Message> dataStore = testKit.spawn(DataStore.create());

        dataStore.tell(new DataStore.GetDataMessage(new ColumnIndex(0, 1), new ColumnIndex(2, 0), worker.getRef()));

        DataStore.Message message = masterStore.receiveMessage();
        Assert.assertEquals(DataStore.RemoteGetDataMessage.class, message.getClass());

        message = masterStore.receiveMessage();
        Assert.assertEquals(DataStore.RemoteGetDataMessage.class, message.getClass());

        dataStore.tell(new DataStore.PutDataMessage(new ColumnIndex(0, 1), Set.of("1")));

        dataStore.tell(new DataStore.GetDataMessage(new ColumnIndex(0, 1), new ColumnIndex(2, 0), worker.getRef()));

        message = masterStore.receiveMessage();
        Assert.assertEquals(DataStore.RemoteGetDataMessage.class, message.getClass());

        masterStore.expectNoMessage();

        dataStore.tell(new DataStore.PutDataMessage(new ColumnIndex(2, 0), Set.of("2")));

        dataStore.tell(new DataStore.GetDataMessage(new ColumnIndex(0, 1), new ColumnIndex(2, 0), worker.getRef()));

        masterStore.expectNoMessage();

        DependencyWorker.Message workerMessage = worker.receiveMessage();
        Assert.assertEquals(DependencyWorker.DependencyMessage.class, workerMessage.getClass());

        DependencyWorker.DependencyMessage dependencyMessage = (DependencyWorker.DependencyMessage) workerMessage;

        Assert.assertEquals(miner.getRef(), dependencyMessage.getDependencyMiner());
        Assert.assertEquals(new ColumnIndex(0, 1), dependencyMessage.getLeft());
        Assert.assertEquals(new ColumnIndex(2, 0), dependencyMessage.getRight());
        Assert.assertEquals(Set.of("1"), dependencyMessage.getColumn1());
        Assert.assertEquals(Set.of("2"), dependencyMessage.getColumn2());
    }
}
