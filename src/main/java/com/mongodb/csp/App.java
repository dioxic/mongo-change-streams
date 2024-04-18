package com.mongodb.csp;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.csp.converters.ConnectionStringConverter;
import com.mongodb.csp.converters.NamespaceConverter;
import com.mongodb.csp.converters.ProcessorConverter;
import com.mongodb.csp.processors.Processor;
import com.mongodb.csp.workers.AbstractWorker;
import com.mongodb.csp.workers.SimpleWorker;
import org.bson.Document;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.mongodb.client.model.changestream.ChangeStreamDocument.createCodec;
import static org.bson.codecs.configuration.CodecRegistries.fromCodecs;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@Command(name = "csp", mixinStandardHelpOptions = true, description = "Change stream processor")
class App implements Callable<Integer> {

    @Option(names = {"--uri"},
            description = "Source mongodb uri",
            defaultValue = "mongodb://localhost:27017",
            converter = ConnectionStringConverter.class)
    private ConnectionString srcConnectionString;

    @Option(names = {"--target-uri"},
            description = "Target mongodb uri",
            converter = ConnectionStringConverter.class)
    private ConnectionString targetConnectionString;

    @Option(names = {"--source-ns"},
            description = "Source namespace",
            required = true,
            converter = NamespaceConverter.class)
    private MongoNamespace srcNamespace;

    @Option(names = {"--target-ns"},
            description = "Target namespace",
            required = true,
            converter = NamespaceConverter.class)
    private MongoNamespace targetNamespace;

    @Option(names = {"--token-ns"},
            description = "Token namespace",
            converter = NamespaceConverter.class)
    private MongoNamespace tokenNamespace;

    @Option(names = {"--error-ns"},
            description = "Error namespace",
            converter = NamespaceConverter.class)
    private MongoNamespace errorNamespace;

    @Option(names = {"--p", "--processor"},
            description = "Change stream processor",
            converter = ProcessorConverter.class,
            defaultValue = "ExampleProcessor"
    )
    private Processor processor;

    @Option(names = {"--log-dups"},
            description = "Log duplicate key exceptions",
            defaultValue = "true"
    )
    private Boolean logDupEx;

    @Option(names = {"--total-workers"},
            description = "total number of workers",
            defaultValue = "1")
    private Integer totalWorkers;

    @Option(names = {"--instance-workers"},
            split = ",",
            description = "worker ids for this instance",
            defaultValue = "0")
    private int[] instanceWorkers;

    @Override
    public Integer call() throws Exception {

        if (targetConnectionString == null) {
            targetConnectionString = srcConnectionString;
        }

        if (tokenNamespace == null) {
            tokenNamespace = new MongoNamespace(targetNamespace.getDatabaseName(), targetNamespace.getCollectionName() + "_token");
        }

        if (errorNamespace == null) {
            errorNamespace = new MongoNamespace(targetNamespace.getDatabaseName(), targetNamespace.getCollectionName() + "_error");
        }

        MongoClient srcClient = MongoClients.create(srcConnectionString);
        MongoClient targetClient = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(targetConnectionString)
                .codecRegistry(fromRegistries(
                        MongoClientSettings.getDefaultCodecRegistry(),
                        fromCodecs(createCodec(Document.class, MongoClientSettings.getDefaultCodecRegistry()))
                ))
                .build());

        ExecutorService executorService = Executors.newFixedThreadPool(instanceWorkers.length);
        List<AbstractWorker> workers = new ArrayList<>();

        for (int workerId : instanceWorkers) {
            workers.add(new SimpleWorker(workerId,
                    totalWorkers,
                    logDupEx,
                    processor,
                    getCollection(srcClient, srcNamespace),
                    getCollection(targetClient, targetNamespace),
                    getCollection(targetClient, tokenNamespace),
                    getCollection(targetClient, errorNamespace)
            ));
        }

        executorService.invokeAll(workers);

        return 0;
    }

    private MongoCollection<Document> getCollection(MongoClient client, MongoNamespace namespace) {
        return client.getDatabase(namespace.getDatabaseName()).getCollection(namespace.getCollectionName());
    }

    public static void main(String... args) {
        int exitCode = new CommandLine(new App()).execute(args);
        System.exit(exitCode);
    }
}
