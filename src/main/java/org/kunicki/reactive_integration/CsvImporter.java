package org.kunicki.reactive_integration;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.alpakka.csv.javadsl.CsvParsing;
import akka.stream.alpakka.file.javadsl.FileTailSource;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Source;
import akka.util.ByteString;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;

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
}
