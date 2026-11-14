package com.resqnet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Service
class DemoSimulationService {
  private static final Logger log = LoggerFactory.getLogger(DemoSimulationService.class);
  private static final double SPEED_METERS_PER_SECOND = 9.7;
  private static final long TICK_MILLIS = 2000;
  private final JdbcTemplate db;
  private final StringRedisTemplate redis;
  private final RoadRoutingService routing;
  private final SimpMessagingTemplate sockets;
  private final KafkaTemplate<String, Object> kafka;
  private final ConcurrentHashMap<String, Movement> movements = new ConcurrentHashMap<>();
  @Value("${resqnet.demo.auto-start:false}")
  private boolean autoStart;

  @EventListener(ApplicationReadyEvent.class)
  void initializeDemoFleet() {
    if (autoStart) ZoneSeeder.CENTERS.keySet().forEach(this::start);
  }

  DemoSimulationService(JdbcTemplate db, StringRedisTemplate redis, RoadRoutingService routing,
                        SimpMessagingTemplate sockets, KafkaTemplate<String, Object> kafka) {
    this.db = db; this.redis = redis; this.routing = routing; this.sockets = sockets; this.kafka = kafka;
  }

  void start(String city) {
    requireCity(city);
    redis.opsForValue().set(key(city), "true", 12, TimeUnit.HOURS);
    db.update("update ambulance set gps_source='DEMO',location_updated_at=now(),updated_at=now() " +
      "where city=? and gps_source<>'LIVE'", city);
    log.info("Demo GPS started for zone {}", city);
  }

  void stop(String city) {
    requireCity(city);
    redis.delete(key(city));
    db.update("update ambulance set gps_source='SEED',location_updated_at='epoch',updated_at=now() " +
      "where city=? and gps_source='DEMO'", city);
    movements.entrySet().removeIf(entry -> city.equals(entry.getValue().city));
    log.info("Demo GPS stopped for zone {}", city);
  }

  boolean enabled(String city) { return Boolean.TRUE.equals(redis.hasKey(key(city))); }

  private static String key(String city) { return "resqnet:demo-gps:" + city; }
  private static void requireCity(String city) {
    if (!ZoneSeeder.CENTERS.containsKey(city)) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown zone");
  }

  @Scheduled(fixedDelayString = "${resqnet.demo.tick-ms:2000}")
  void tick() {
    var active = db.query("select i.id,i.city,i.status,i.latitude,i.longitude,i.ambulance_unit,i.hospital_code," +
        "a.latitude,a.longitude,a.heading,h.latitude,h.longitude " +
        "from incident i join ambulance a on a.city=i.city and a.unit_number=i.ambulance_unit " +
        "join hospital h on h.city=i.city and h.code=i.hospital_code " +
        "where i.status in ('DISPATCHED','TRANSPORTING') and a.gps_source='DEMO'",
      (r, n) -> new Dispatch(r.getObject(1, UUID.class), r.getString(2), r.getString(3),
        r.getDouble(4), r.getDouble(5), r.getInt(6), r.getString(7), r.getDouble(8),
        r.getDouble(9), r.getInt(10), r.getDouble(11), r.getDouble(12)));

    for (Dispatch dispatch : active) {
      if (!enabled(dispatch.city)) continue;
      try { move(dispatch); }
      catch (Exception e) { log.warn("Could not advance demo ambulance {} in {}: {}", dispatch.unit, dispatch.city, e.getMessage()); }
    }
    moveReturningUnits();
    refreshIdleDemoGps();
  }

  private void move(Dispatch d) {
    String phase = d.status.equals("DISPATCHED") ? "SCENE" : "HOSPITAL";
    String cacheKey = d.id.toString();
    Movement movement = movements.get(cacheKey);
    if (movement == null || !movement.phase.equals(phase)) {
      double toLat = phase.equals("SCENE") ? d.incidentLat : d.hospitalLat;
      double toLon = phase.equals("SCENE") ? d.incidentLon : d.hospitalLon;
      RoadRouteDto route = routing.route(d.ambulanceLat, d.ambulanceLon, toLat, toLon);
      movement = new Movement(d.city, phase, route.coordinates(), route.distanceMeters());
      movements.put(cacheKey, movement);
    }

    movement.progressMeters = Math.min(movement.distanceMeters,
      movement.progressMeters + SPEED_METERS_PER_SECOND * TICK_MILLIS / 1000.0);
    Point point = interpolate(movement.points, movement.distanceMeters <= 0 ? 1 : movement.progressMeters / movement.distanceMeters);
    int heading = heading(d.ambulanceLat, d.ambulanceLon, point.lat, point.lon);
    int changed = db.update("update ambulance set latitude=?,longitude=?,heading=?,gps_source='DEMO',location_updated_at=now(),updated_at=now() " +
      "where city=? and unit_number=? and gps_source='DEMO'", point.lat, point.lon, heading, d.city, d.unit);
    if (changed == 0) { movements.remove(cacheKey); return; }

    boolean arrived = movement.progressMeters >= movement.distanceMeters;
    if (arrived && phase.equals("HOSPITAL")) {
      db.update("update incident set status='COMPLETED',updated_at=now() where id=? and status='TRANSPORTING'", d.id);
      db.update("update ambulance set status='RETURNING',updated_at=now() where city=? and unit_number=? and status='DISPATCHED'", d.city, d.unit);
      movements.remove(cacheKey);
    }

    Map<String, Object> event = Map.ofEntries(Map.entry("type", "AMBULANCE_LOCATION"), Map.entry("city", d.city),
      Map.entry("incidentId", d.id.toString()), Map.entry("unitNumber", d.unit), Map.entry("latitude", point.lat),
      Map.entry("longitude", point.lon), Map.entry("heading", heading),
      Map.entry("status", arrived && phase.equals("HOSPITAL") ? "RETURNING" : "DISPATCHED"),
      Map.entry("phase", phase), Map.entry("gpsSource", "DEMO"), Map.entry("updatedAt", Instant.now().toString()));
    sockets.convertAndSend("/topic/zones/" + d.city + "/ambulances", event);
    try { kafka.send("resqnet.events", d.city, event); }
    catch (Exception e) { log.debug("Kafka demo telemetry publish failed: {}", e.getMessage()); }
  }

  private void moveReturningUnits() {
    var returning = db.query("select city,unit_number,latitude,longitude,heading,home_latitude,home_longitude " +
        "from ambulance where gps_source='DEMO' and status='RETURNING'",
      (r,n) -> new ReturningUnit(r.getString(1),r.getInt(2),r.getDouble(3),r.getDouble(4),
        r.getInt(5),r.getDouble(6),r.getDouble(7)));
    for (ReturningUnit unit : returning) {
      if (!enabled(unit.city)) continue;
      try {
        String cacheKey="return:"+unit.city+":"+unit.unit;
        Movement movement=movements.get(cacheKey);
        if(movement==null){RoadRouteDto route=routing.route(unit.latitude,unit.longitude,unit.homeLat,unit.homeLon);movement=new Movement(unit.city,"RETURNING",route.coordinates(),route.distanceMeters());movements.put(cacheKey,movement);}
        movement.progressMeters=Math.min(movement.distanceMeters,movement.progressMeters+SPEED_METERS_PER_SECOND*TICK_MILLIS/1000.0);
        Point point=interpolate(movement.points,movement.distanceMeters<=0?1:movement.progressMeters/movement.distanceMeters);
        int heading=heading(unit.latitude,unit.longitude,point.lat,point.lon);boolean arrived=movement.progressMeters>=movement.distanceMeters;
        int changed = db.update("update ambulance set latitude=?,longitude=?,heading=?,status=?,location_updated_at=now(),updated_at=now() where city=? and unit_number=? and status='RETURNING' and gps_source='DEMO'",point.lat,point.lon,heading,arrived?"AVAILABLE":"RETURNING",unit.city,unit.unit);
        if (changed == 0 || arrived) movements.remove(cacheKey);
        if (changed == 0) continue;
        Map<String,Object> event=Map.of("type","AMBULANCE_LOCATION","city",unit.city,"unitNumber",unit.unit,"latitude",point.lat,"longitude",point.lon,"heading",heading,"status",arrived?"AVAILABLE":"RETURNING","phase",arrived?"IDLE":"RETURNING","gpsSource","DEMO","updatedAt",Instant.now().toString());
        sockets.convertAndSend("/topic/zones/"+unit.city+"/ambulances",event);try{kafka.send("resqnet.events",unit.city,event);}catch(Exception ignored){}
      } catch(Exception e) { log.warn("Could not return demo ambulance {} to its hub: {}",unit.unit,e.getMessage()); }
    }
  }

  private void refreshIdleDemoGps() {
    var idle = db.query("select city,unit_number,latitude,longitude,heading,status from ambulance " +
        "where gps_source='DEMO' and status='AVAILABLE'",
      (r, n) -> Map.<String, Object>of("city", r.getString(1), "unitNumber", r.getInt(2),
        "latitude", r.getDouble(3), "longitude", r.getDouble(4), "heading", r.getInt(5), "status", r.getString(6)));
    for (Map<String, Object> unit : idle) {
      String city = (String) unit.get("city");
      if (!enabled(city)) continue;
      int number = (int) unit.get("unitNumber");
      db.update("update ambulance set location_updated_at=now() where city=? and unit_number=? and gps_source='DEMO'", city, number);
      Map<String, Object> event = Map.of("type", "AMBULANCE_LOCATION", "city", city,
        "unitNumber", number, "latitude", unit.get("latitude"), "longitude", unit.get("longitude"),
        "heading", unit.get("heading"), "status", "AVAILABLE", "phase", "IDLE", "gpsSource", "DEMO",
        "updatedAt", Instant.now().toString());
      sockets.convertAndSend("/topic/zones/" + city + "/ambulances", event);
    }
  }

  private static Point interpolate(List<List<Double>> points, double fraction) {
    if (points.isEmpty()) throw new IllegalStateException("Road route contains no coordinates");
    if (points.size() == 1) return new Point(points.get(0).get(0), points.get(0).get(1));
    List<Double> lengths = new ArrayList<>(); double total = 0;
    for (int i = 1; i < points.size(); i++) {
      double length = distance(points.get(i - 1), points.get(i)); lengths.add(length); total += length;
    }
    double target = Math.max(0, Math.min(1, fraction)) * total;
    for (int i = 0; i < lengths.size(); i++) {
      double length = lengths.get(i);
      if (target <= length || i == lengths.size() - 1) {
        double t = length == 0 ? 0 : target / length;
        return new Point(points.get(i).get(0) + (points.get(i + 1).get(0) - points.get(i).get(0)) * t,
          points.get(i).get(1) + (points.get(i + 1).get(1) - points.get(i).get(1)) * t);
      }
      target -= length;
    }
    List<Double> last = points.get(points.size() - 1); return new Point(last.get(0), last.get(1));
  }

  private static double distance(List<Double> a, List<Double> b) {
    double lat = Math.toRadians(b.get(0) - a.get(0)), lon = Math.toRadians(b.get(1) - a.get(1));
    double x = Math.sin(lat / 2) * Math.sin(lat / 2) + Math.cos(Math.toRadians(a.get(0))) *
      Math.cos(Math.toRadians(b.get(0))) * Math.sin(lon / 2) * Math.sin(lon / 2);
    return 6371000 * 2 * Math.atan2(Math.sqrt(x), Math.sqrt(1 - x));
  }
  private static int heading(double lat1, double lon1, double lat2, double lon2) {
    double dLon = Math.toRadians(lon2 - lon1);
    double y = Math.sin(dLon) * Math.cos(Math.toRadians(lat2));
    double x = Math.cos(Math.toRadians(lat1)) * Math.sin(Math.toRadians(lat2)) -
      Math.sin(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.cos(dLon);
    return (int) Math.round((Math.toDegrees(Math.atan2(y, x)) + 360) % 360);
  }

  private record Point(double lat, double lon) {}
  private static final class Movement {
    final String city, phase; final List<List<Double>> points; final double distanceMeters; double progressMeters;
    Movement(String city, String phase, List<List<Double>> points, double distanceMeters) {
      this.city = city; this.phase = phase; this.points = points; this.distanceMeters = distanceMeters;
    }
  }
  private record Dispatch(UUID id, String city, String status, double incidentLat, double incidentLon,
                          int unit, String hospital, double ambulanceLat, double ambulanceLon, int heading,
                          double hospitalLat, double hospitalLon) {}
  private record ReturningUnit(String city,int unit,double latitude,double longitude,int heading,double homeLat,double homeLon) {}
}
