package io.emeraldpay.dshackle.archive.runner

import com.fasterxml.jackson.databind.ObjectMapper
import io.emeraldpay.etherjar.domain.TransactionId
import spock.lang.Specification

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

class ReplayProcessorSpec extends Specification {

    ObjectMapper objectMapper = new ObjectMapper()

    def "Parse transactions"() {
        setup:
        def original = ReplayProcessorSpec.class.getResourceAsStream("/blockTraces-1.json").readAllBytes()
        def processor = new ReplayProcessor()
        when:
        def items = processor.splitReplayList(ByteBuffer.wrap(original))
        then:
        items.size() == 3
        with(items[0]) {
            it.txid == TransactionId.from("0x55569bfa63108516a9559941689a92d288c25354e6337088356768775f9d08ff")
            with(new String(StandardCharsets.UTF_8.decode(it.json).toString())) { json ->
                json.startsWith("{")
                json.endsWith("}")
                with(objectMapper.readValue(json, Map.class)) {
                    it["stateDiff"] != null
                    it["stateDiff"] instanceof Map
                    it["stateDiff"]["0x11b815efb8f581194ae79006d24e0d814b7697f6"]["balance"] == "="
                }
            }
        }
        with(items[1]) {
            it.txid == TransactionId.from("0xe3b762864612b9787b45e44c3823290cd8bd3447b9c3955e5b065fe775085fd6")
            with(new String(StandardCharsets.UTF_8.decode(it.json).toString())) { json ->
                json.startsWith("{")
                json.endsWith("}")
                with(objectMapper.readValue(json, Map.class)) {
                    it["stateDiff"] != null
                    it["stateDiff"] instanceof Map
                    it["stateDiff"]["0x2dce0dda1c2f98e0f171de8333c3c6fe1bbf4877"]["balance"] == "="
                }
            }
        }
        with(items[2]) {
            it.txid == TransactionId.from("0xdf39ead88d7caaa0cbe72a7a742c1bbc4a7876420db696276ab2f5800f03e617")
            with(new String(StandardCharsets.UTF_8.decode(it.json).toString())) { json ->
                json.startsWith("{")
                json.endsWith("}")
                with(objectMapper.readValue(json, Map.class)) {
                    it["stateDiff"] != null
                    it["stateDiff"] instanceof Map
                    it["stateDiff"]["0x829bd824b016326a401d083b33d092293333a830"]["balance"]["*"]["from"] == "0x11129ca7587a13c7f03"
                }
            }
        }
    }

}
