package pl.icreatesoftware.entrypoint.rest;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
class KafkaMessageController {

    @GetMapping("/index")
    public String index() {
        return "Greetings from Spring Boot!";
    }
}
