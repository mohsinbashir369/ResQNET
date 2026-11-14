package com.resqnet;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.IntStream;

record RoadStepDto(String instruction, String road, double distanceMeters, double durationSeconds, List<Double> location) {}
record RoadRouteDto(List<List<Double>> coordinates, double distanceMeters, double durationSeconds, List<RoadStepDto> steps, String source) {}

@Service
class RoadRoutingService {
  private final HttpClient http=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
  private final ObjectMapper json;
  @Value("${resqnet.routing.base-url:https://router.project-osrm.org}") private String baseUrl;
  RoadRoutingService(ObjectMapper json){this.json=json;}

  RoadRouteDto route(double fromLat,double fromLon,double toLat,double toLon){
    String coords=String.format(Locale.ROOT,"%.6f,%.6f;%.6f,%.6f",fromLon,fromLat,toLon,toLat);
    URI uri=URI.create(baseUrl.replaceAll("/$","")+"/route/v1/driving/"+coords+"?alternatives=true&overview=full&geometries=geojson&steps=true");
    HttpRequest request=HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(12)).header("Accept","application/json").GET().build();
    try{
      HttpResponse<String> response=http.send(request,HttpResponse.BodyHandlers.ofString());
      if(response.statusCode()!=200)throw new ResponseStatusException(HttpStatus.BAD_GATEWAY,"Road routing service returned HTTP "+response.statusCode());
      JsonNode root=json.readTree(response.body());
      if(!"Ok".equals(root.path("code").asText()))throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY,"No driving route found for these locations");
      JsonNode route=null;double fastest=Double.POSITIVE_INFINITY;for(JsonNode candidate:root.path("routes")){double seconds=candidate.path("duration").asDouble(Double.POSITIVE_INFINITY);if(seconds<fastest){fastest=seconds;route=candidate;}}if(route==null)throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY,"No driving route found for these locations");JsonNode coordsNode=route.path("geometry").path("coordinates");
      List<List<Double>> points=new ArrayList<>();for(JsonNode c:coordsNode)if(c.size()>=2)points.add(List.of(c.get(1).asDouble(),c.get(0).asDouble()));
      if(points.size()<2)throw new ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY,"Routing service returned no road geometry");
      List<RoadStepDto> steps=new ArrayList<>();for(JsonNode leg:route.path("legs"))for(JsonNode s:leg.path("steps")){JsonNode maneuver=s.path("maneuver");String type=maneuver.path("type").asText("Continue"),modifier=maneuver.path("modifier").asText("");String instruction=(type+" "+modifier).trim();JsonNode loc=maneuver.path("location");List<Double> location=loc.size()>=2?List.of(loc.get(1).asDouble(),loc.get(0).asDouble()):List.of();steps.add(new RoadStepDto(instruction,s.path("name").asText("Unnamed road"),s.path("distance").asDouble(),s.path("duration").asDouble(),location));}
      return new RoadRouteDto(points,route.path("distance").asDouble(),route.path("duration").asDouble(),steps,"OpenStreetMap road graph · OSRM estimate");
    }catch(ResponseStatusException e){throw e;}catch(java.net.http.HttpTimeoutException e){throw new ResponseStatusException(HttpStatus.GATEWAY_TIMEOUT,"Road routing timed out; retry in a moment");}catch(Exception e){throw new ResponseStatusException(HttpStatus.BAD_GATEWAY,"Road routing service is unavailable");}
  }

  List<Double> ambulanceTravelTimes(List<Object[]> candidates,double targetLat,double targetLon){
    if(candidates.isEmpty())return List.of();
    StringBuilder coordinates=new StringBuilder();
    for(Object[] candidate:candidates){if(coordinates.length()>0)coordinates.append(';');coordinates.append(String.format(Locale.ROOT,"%.6f,%.6f",(double)candidate[2],(double)candidate[1]));}
    if(coordinates.length()>0)coordinates.append(';');coordinates.append(String.format(Locale.ROOT,"%.6f,%.6f",targetLon,targetLat));
    String sources=IntStream.range(0,candidates.size()).mapToObj(String::valueOf).collect(java.util.stream.Collectors.joining(";"));
    URI uri=URI.create(baseUrl.replaceAll("/$","")+"/table/v1/driving/"+coordinates+"?sources="+sources+"&destinations="+candidates.size()+"&annotations=duration");
    HttpRequest request=HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(10)).header("Accept","application/json").GET().build();
    try{HttpResponse<String> response=http.send(request,HttpResponse.BodyHandlers.ofString());if(response.statusCode()!=200)return List.of();JsonNode rows=json.readTree(response.body()).path("durations");List<Double> times=new ArrayList<>();for(JsonNode row:rows){JsonNode value=row.isArray()&&row.size()>0?row.get(0):null;times.add(value==null||value.isNull()?Double.POSITIVE_INFINITY:value.asDouble(Double.POSITIVE_INFINITY));}return times.size()==candidates.size()?times:List.of();}
    catch(Exception ignored){return List.of();}
  }

  List<Double> travelTimesToCandidates(double fromLat,double fromLon,List<Object[]> candidates){
    if(candidates.isEmpty())return List.of();
    StringBuilder coordinates=new StringBuilder(String.format(Locale.ROOT,"%.6f,%.6f",fromLon,fromLat));
    for(Object[] candidate:candidates)coordinates.append(';').append(String.format(Locale.ROOT,"%.6f,%.6f",(double)candidate[2],(double)candidate[1]));
    URI uri=URI.create(baseUrl.replaceAll("/$","")+"/table/v1/driving/"+coordinates+"?sources=0&destinations="+IntStream.rangeClosed(1,candidates.size()).mapToObj(String::valueOf).collect(java.util.stream.Collectors.joining(";"))+"&annotations=duration");
    HttpRequest request=HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(10)).header("Accept","application/json").GET().build();
    try{
      HttpResponse<String> response=http.send(request,HttpResponse.BodyHandlers.ofString());
      if(response.statusCode()!=200)return List.of();
      JsonNode durations=json.readTree(response.body()).path("durations");
      JsonNode row=durations.isArray()&&durations.size()>0?durations.get(0):null;
      if(row==null||!row.isArray()||row.size()!=candidates.size())return List.of();
      List<Double> times=new ArrayList<>();for(JsonNode value:row)times.add(value.isNull()?Double.POSITIVE_INFINITY:value.asDouble(Double.POSITIVE_INFINITY));
      return times;
    }catch(Exception ignored){return List.of();}
  }
}
