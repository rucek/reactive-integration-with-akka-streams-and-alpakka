package org.kunicki.reactive_integration;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.http.javadsl.Http;
import akka.http.javadsl.model.HttpRequest;
import akka.stream.ActorMaterializer;
import akka.stream.Graph;
import akka.stream.SinkShape;
import akka.stream.alpakka.csv.javadsl.CsvParsing;
import akka.stream.alpakka.cassandra.javadsl.CassandraSink;
import akka.stream.alpakka.file.javadsl.FileTailSource;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.Partition;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.function.BiFunction;

public class CsvImporter {

    class Model {

        final int id;
        final String value;

        Model(List<ByteString> fields) {
            this.id = Integer.valueOf(fields.get(0).utf8String());
            this.value = fields.get(1).utf8String();
        }
    }

    private static final Path DATA_PATH = Paths.get("src/main/resources/data.csv");
    private static final String INSERT_QUERY = "insert into alpakka.test (id, value) values (now(), ?)";

    private final ActorSystem actorSystem = ActorSystem.create();
    private final ActorMaterializer materializer = ActorMaterializer.create(actorSystem);

    private final Source<ByteString, NotUsed> fileBytes = FileTailSource.create(DATA_PATH, 100, 0, Duration.ofSeconds(1));

    private final Flow<ByteString, Model, NotUsed> toModel =
        CsvParsing.lineScanner()
            .map(List::copyOf)
            .map(Model::new);

    private Sink<Model, NotUsed> httpSink() {
        var httpConnectionFlow = Http.get(actorSystem).outgoingConnection("localhost:9999");

        return Flow.of(Model.class)
            .map(m -> HttpRequest.POST("/echo").withEntity(m.value))
            .via(httpConnectionFlow)
            .to(Sink.ignore());
    }

    private Sink<Model, NotUsed> cassandraSink() {
        var session = Cluster.builder().addContactPoint("localhost").withPort(9042).build().connect();
        var preparedStatement = session.prepare(INSERT_QUERY);
        BiFunction<String, PreparedStatement, BoundStatement> statementBinder = (String value, PreparedStatement statement) -> statement.bind(value);

        var cassandraSink = CassandraSink.create(1, preparedStatement, statementBinder, session);

        return Flow.of(Model.class)
            .map(m -> m.value)
            .to(cassandraSink);
    }

    private final Graph<SinkShape<Model>, NotUsed> partitioningSink =
        GraphDSL.create(builder -> {
            var partition = builder.add(Partition.create(Model.class, 2, m -> m.id % 2));
            var cassandra = builder.add(cassandraSink());
            var http = builder.add(httpSink());

            builder.from(partition.out(0)).to(cassandra);
            builder.from(partition.out(1)).to(http);

            return SinkShape.of(partition.in());
        });
}
