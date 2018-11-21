package org.kunicki.reactive_integration;

import akka.http.javadsl.server.HttpApp;
import akka.http.javadsl.server.Route;
import akka.http.javadsl.unmarshalling.Unmarshaller;
import akka.stream.ActorMaterializer;

import java.util.concurrent.ExecutionException;

public class HttpServer extends HttpApp {

    @Override
    protected Route routes() {
        final ActorMaterializer materializer = ActorMaterializer.create(systemReference.get());

        return route(
            post(() ->
                path("echo", () ->
                    extractRequestEntity(requestEntity ->
                        completeOKWithFutureString(
                            Unmarshaller.entityToString()
                                .unmarshal(requestEntity, materializer)
                                .thenApplyAsync(s -> {
                                    System.out.println(s);
                                    return s;
                                })
                        )
                    )
                )
            )
        );
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        new HttpServer().startServer("localhost", 9999);
    }
}
