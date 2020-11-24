package com.thehecklers.rsaccli;

import io.rsocket.SocketAcceptor;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.RSocketStrategies;
import org.springframework.messaging.rsocket.annotation.support.RSocketMessageHandler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.UUID;

@SpringBootApplication
public class RsAcCliApplication {

    public static void main(String[] args) {
        SpringApplication.run(RsAcCliApplication.class, args);
    }

}

@Configuration
class AircraftClientConfig {
/*
    @Bean
    RSocketRequester requester(RSocketRequester.Builder builder) {
        return builder.tcp("localhost", 9091);
    }
*/

    @Bean
    RSocketRequester requester(RSocketRequester.Builder builder,
                               RSocketStrategies strategies) {

        String clientId = UUID.randomUUID().toString();

        SocketAcceptor responder = RSocketMessageHandler.responder(strategies,
                new ClientHandler());

        RSocketRequester requester = builder
                .setupRoute("clients")
                .setupData(clientId)
                .rsocketStrategies(strategies)
                .rsocketConnector(connector -> connector.acceptor(responder))
                .tcp("localhost", 9091);

        return requester;
    }
}

@Component
@AllArgsConstructor
class AircraftClient {
    private final RSocketRequester requester;
    private final WeatherGenerator generator;

    //	@PostConstruct
    void requestResponse() {
        requester.route("reqresp")
                .data(Instant.now())
                .retrieveMono(Aircraft.class)
                .subscribe(ac -> System.out.println("🛩 " + ac));
    }

    //	@PostConstruct
    void requestStream() {
        requester.route("reqstream")
                .data(Instant.now())
                .retrieveFlux(Aircraft.class)
                .subscribe(ac -> System.out.println("🛩🛩 " + ac));
    }

    //    @PostConstruct
    void fireAndForget() {
        requester.route("fireforget")
                .data(generator.generateFlux().next())
                .send()
                .subscribe();
    }

    @PostConstruct
    void channel() {
        requester.route("channel")
                .data(generator.generateFlux())
                .retrieveFlux(Aircraft.class)
                .subscribe(ac -> System.out.println("✈️✈️✈️ " + ac));

    }
}

/*
@Component
@EnableScheduling
@AllArgsConstructor
class WeatherFeed {
    private final RSocketRequester requester;
    private final WeatherGenerator generator;

    @Scheduled(fixedRate = 1000)
    void sendWeather() {
        requester.route("fireforget")
                .data(generator.generateFlux().next())
                .send()
                .subscribe();
    }
}
*/

class ClientHandler {
    @MessageMapping("clientwx")
    Flux<Weather> sendWeather(Mono<String> stringMono) {
        return stringMono.log()
                .thenMany(Flux.interval(Duration.ofSeconds(3))
                .map(l -> new Weather(Instant.now(), "IFR")));
    }
}

@Component
class WeatherGenerator {
    private final List<String> obsList = List.of("OVC 5000, VIS 10SM",
            "SKC, VIS 12SM",
            "BKN 7000, VIS 7SM");
    private final Random rnd = new Random();

    Flux<Weather> generateFlux() {
        return Flux.interval(Duration.ofSeconds(5))
                .map(l -> new Weather(Instant.now(),
                        obsList.get(rnd.nextInt(obsList.size()))));
    }
}

@Data
@AllArgsConstructor
class Weather {
    private Instant when;
    private String observation;
}

@Data
class Aircraft {
    private String callsign, reg, flightno, type;
    private int altitude, heading, speed;
    private double lat, lon;
}
