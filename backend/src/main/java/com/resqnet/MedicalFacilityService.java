package com.resqnet;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

record MedicalFacilityDto(String id, String name, String kind, double latitude, double longitude,
                          String operator, String openingHours, String source) {}

@Service
class MedicalFacilityService {
  private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
  private final ObjectMapper json;
  private final StringRedisTemplate redis;
  @Value("${resqnet.osm.overpass-url:https://overpass.kumi.systems/api/interpreter}") private String overpassUrl;

  MedicalFacilityService(ObjectMapper json, StringRedisTemplate redis) {
    this.json = json;
    this.redis = redis;
  }

  List<MedicalFacilityDto> byCity(String city, boolean refresh) {
    double[] center = ZoneSeeder.CENTERS.get(city);
    if (center == null) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown zone");
    return query(city, refresh, "around:12000," + center[0] + "," + center[1],
      "resqnet:osm:facilities:" + city);
  }

  List<MedicalFacilityDto> byBounds(String city, boolean refresh, double south, double west, double north, double east) {
    if (!ZoneSeeder.CENTERS.containsKey(city)) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown zone");
    if (south < -90 || north > 90 || west < -180 || east > 180 || south >= north || west >= east
        || (north - south) * (east - west) > 0.06) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid or overly large map bounds");
    }
    String key = String.format(Locale.ROOT, "resqnet:osm:viewport:%s:%.3f:%.3f:%.3f:%.3f", city, south, west, north, east);
    return query(city, refresh, String.format(Locale.ROOT, "%.6f,%.6f,%.6f,%.6f", south, west, north, east), key);
  }

  private List<MedicalFacilityDto> query(String city, boolean refresh, String area, String cacheKey) {
    String cached = redis.opsForValue().get(cacheKey);
    if (!refresh && cached != null) return deserialize(cached);

    String query = String.format(Locale.ROOT,
      "[out:json][timeout:15];(nwr(%s)[amenity~\"^(hospital|pharmacy)$\"];nwr(%s)[healthcare~\"^(hospital|pharmacy)$\"];nwr(%s)[shop=medical_supply];);out center tags;",
      area, area, area);
    String body = "data=" + URLEncoder.encode(query, StandardCharsets.UTF_8);
    Exception lastError = null;
    for (String endpoint : new java.util.LinkedHashSet<>(List.of(overpassUrl, "https://overpass.private.coffee/api/interpreter", "https://lz4.overpass-api.de/api/interpreter"))) {
      try {
        HttpRequest request = HttpRequest.newBuilder(URI.create(endpoint))
          .timeout(Duration.ofSeconds(12))
          .header("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
          .header("Accept", "application/json")
          .header("User-Agent", "RESQNET-Emergency-Response/1.0 (OpenStreetMap facility lookup)")
          .POST(HttpRequest.BodyPublishers.ofString(body))
          .build();
        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) { lastError = new IllegalStateException(endpoint + " returned HTTP " + response.statusCode()); continue; }
        List<MedicalFacilityDto> result = parse(response.body());
        String encoded = json.writeValueAsString(result);
        redis.opsForValue().set(cacheKey, encoded, 15, TimeUnit.MINUTES);
        redis.opsForValue().set(cacheKey + ":stale", encoded, 7, TimeUnit.DAYS);
        return result;
      } catch (Exception e) { lastError = e; }
    }
    String stale = redis.opsForValue().get(cacheKey + ":stale");
    if (stale != null) return deserialize(stale);
    System.err.println("OpenStreetMap facility lookup failed: " + (lastError == null ? "unknown upstream error" : lastError.getMessage()));
    throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,
      "OpenStreetMap facility data is temporarily unavailable; retry shortly");
  }

  private List<MedicalFacilityDto> parse(String response) throws Exception {
    JsonNode elements = json.readTree(response).path("elements");
    List<MedicalFacilityDto> facilities = new ArrayList<>();
    for (JsonNode element : elements) {
      JsonNode tags = element.path("tags");
      String amenity = tags.path("amenity").asText("");
      String healthcare = tags.path("healthcare").asText("");
      String shop = tags.path("shop").asText("");
      String kind = amenity.equals("hospital") || healthcare.equals("hospital") ? "HOSPITAL"
        : amenity.equals("pharmacy") || healthcare.equals("pharmacy") ? "PHARMACY"
        : shop.equals("medical_supply") ? "MEDICAL_STORE" : "HEALTHCARE";
      JsonNode center = element.has("center") ? element.path("center") : element;
      if (!center.hasNonNull("lat") || !center.hasNonNull("lon")) continue;
      String id = "osm:" + element.path("type").asText("node") + ":" + element.path("id").asText();
      String name = tags.path("name").asText("").trim();
      if (name.isBlank()) name = kind.equals("PHARMACY") ? "Unnamed pharmacy" : kind.equals("MEDICAL_STORE") ? "Unnamed medical store" : "Unnamed medical facility";
      facilities.add(new MedicalFacilityDto(id, name, kind, center.path("lat").asDouble(), center.path("lon").asDouble(),
        tags.path("operator").asText(""), tags.path("opening_hours").asText(""), "OpenStreetMap"));
    }
    facilities.sort((a,b) -> {
      int kindOrder = Integer.compare(kindRank(a.kind()), kindRank(b.kind()));
      return kindOrder != 0 ? kindOrder : a.name().compareToIgnoreCase(b.name());
    });
    return List.copyOf(facilities);
  }

  private int kindRank(String kind) { return kind.equals("HOSPITAL") ? 0 : kind.equals("PHARMACY") ? 1 : 2; }
  private List<MedicalFacilityDto> deserialize(String jsonString) {
    try { return json.readerForListOf(MedicalFacilityDto.class).readValue(jsonString); }
    catch (Exception e) { return List.of(); }
  }
}
