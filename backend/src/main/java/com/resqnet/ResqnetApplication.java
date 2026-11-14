package com.resqnet;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.*;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.*;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.*;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.cors.*;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.security.SecureRandom;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
@org.springframework.scheduling.annotation.EnableScheduling
public class ResqnetApplication {
  public static void main(String[] args) { SpringApplication.run(ResqnetApplication.class, args); }
  @Bean PasswordEncoder passwordEncoder() { return new BCryptPasswordEncoder(12); }
  @Bean SecurityFilterChain security(HttpSecurity http, TokenFilter tokens) throws Exception {
    return http.csrf(csrf -> csrf.disable()).cors(cors -> {}).sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
      .authorizeHttpRequests(a -> a.requestMatchers("/api/auth/login", "/api/zones/*/bootstrap", "/api/zones/*/facilities", "/api/zones/*/ambulance-recommendation", "/api/tracking/*/location", "/ws/**", "/actuator/health/**").permitAll().anyRequest().authenticated())
      .addFilterBefore(tokens, UsernamePasswordAuthenticationFilter.class).build();
  }
  @Bean CorsConfigurationSource corsConfigurationSource(@Value("${resqnet.web.allowed-origins:http://localhost:5173,http://127.0.0.1:5173}") String origins) {
    var c = new CorsConfiguration(); c.setAllowedOrigins(Arrays.stream(origins.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList());
    c.setAllowedMethods(List.of("GET", "POST", "PATCH", "OPTIONS")); c.setAllowedHeaders(List.of("Authorization", "Content-Type", "X-Tracker-Token"));
    var s = new UrlBasedCorsConfigurationSource(); s.registerCorsConfiguration("/api/**", c); return s;
  }
  @Bean org.apache.kafka.clients.admin.NewTopic resqnetEventsTopic() { return TopicBuilder.name("resqnet.events").partitions(3).replicas(1).build(); }
  @Bean ApplicationRunner seedUsers(JdbcTemplate db, PasswordEncoder encoder,
      @Value("${resqnet.auth.hospital-admin-username}") String admin,
      @Value("${resqnet.auth.hospital-admin-password}") String adminPass,
      @Value("${resqnet.auth.paramedic-username}") String medic,
      @Value("${resqnet.auth.paramedic-password}") String medicPass) {
    return args -> {
      upsertUser(db, encoder, admin, adminPass, "HOSPITAL_ADMIN");
      upsertUser(db, encoder, medic, medicPass, "PARAMEDIC");
    };
  }
  private static void upsertUser(JdbcTemplate db, PasswordEncoder encoder, String name, String pass, String role) {
    if (name == null || name.isBlank() || pass == null || pass.isBlank())
      throw new IllegalStateException("Set non-empty RESQNET login credentials before starting the API.");
    var current = db.query("select password_hash from app_user where username=? and role=?", (r,n) -> r.getString(1), name, role);
    if (current.isEmpty()) db.update("insert into app_user(username,password_hash,role) values(?,?,?)", name, encoder.encode(pass), role);
    else if (!encoder.matches(pass, current.get(0))) db.update("update app_user set password_hash=?,enabled=true where username=? and role=?", encoder.encode(pass), name, role);
  }
}

record LoginRequest(@NotBlank @Size(max=80) String username, @NotBlank @Size(max=72) String password, @NotBlank String role) {}
record LoginResponse(String accessToken, String tokenType, Instant expiresAt, String username, String role) {}
record HospitalDto(String id, String name, double latitude, double longitude, int totalBeds, int icuBeds, boolean trauma, boolean cardiac, boolean stroke, boolean burns, boolean obstetric, int lockTtlSeconds) {}
record AmbulanceDto(int id, String name, double latitude, double longitude, String status, int battery, int heading, Instant locationUpdatedAt, String gpsSource) {}
record IncidentDto(String id, String incidentNumber, String type, String triage, double latitude, double longitude, String notes, String status, int ambulanceId, String hospitalId, Instant createdAt) {}
record BootstrapDto(String city, double latitude, double longitude, List<HospitalDto> hospitals, List<AmbulanceDto> ambulances, List<IncidentDto> incidents) {}
record CreateIncidentRequest(@NotBlank @Size(max=100) String type, @NotBlank String triage, @DecimalMin("-90") @DecimalMax("90") double latitude, @DecimalMin("-180") @DecimalMax("180") double longitude, @Size(max=2000) String notes) {}
record IncidentStatusRequest(@NotBlank String status) {}
record BedCapacityRequest(@Min(0) @Max(500) int icuBeds) {}
record SpecialistAvailabilityRequest(@NotBlank String specialty, boolean available) {}
record TrackerLocationRequest(@DecimalMin("-90") @DecimalMax("90") double latitude, @DecimalMin("-180") @DecimalMax("180") double longitude, @Min(0) @Max(359) Integer heading, @Min(0) @Max(100) Integer battery) {}
record TrackerKeyDto(String trackerId, String token, String endpoint) {}
record EventDto(String time, String type, String message) {}
record AmbulanceRecommendationDto(int unitNumber, Double etaSeconds, String method) {}

@RestControllerAdvice
class ApiErrors {
  @ExceptionHandler(ResponseStatusException.class) ResponseEntity<Map<String,String>> response(ResponseStatusException e) { return ResponseEntity.status(e.getStatusCode()).body(Map.of("error", e.getReason() == null ? "Request failed" : e.getReason())); }
  @ExceptionHandler(BadCredentialsException.class) ResponseEntity<Map<String,String>> badCredentials() { return ResponseEntity.status(401).body(Map.of("error","Invalid username or password")); }
  @ExceptionHandler(org.springframework.web.bind.MethodArgumentNotValidException.class) ResponseEntity<Map<String,String>> invalid(org.springframework.web.bind.MethodArgumentNotValidException e) { String message=e.getBindingResult().getFieldErrors().stream().findFirst().map(x->x.getDefaultMessage()).orElse("Invalid request");return ResponseEntity.badRequest().body(Map.of("error",message)); }
  @ExceptionHandler(Exception.class) ResponseEntity<Map<String,String>> unknown(Exception e) { e.printStackTrace(); return ResponseEntity.status(500).body(Map.of("error","Internal server error")); }
}

@Component
class TokenFilter extends OncePerRequestFilter {
  private final StringRedisTemplate redis;
  TokenFilter(StringRedisTemplate redis) { this.redis=redis; }
  @Override protected void doFilterInternal(jakarta.servlet.http.HttpServletRequest req, jakarta.servlet.http.HttpServletResponse res, jakarta.servlet.FilterChain chain) throws IOException, jakarta.servlet.ServletException {
    String header=req.getHeader("Authorization");
    if(header!=null && header.startsWith("Bearer ")) {
      String key="resqnet:session:"+header.substring(7);
      String value=redis.opsForValue().get(key);
      if(value!=null){String[] parts=value.split("\\|",2);var auth=new org.springframework.security.authentication.UsernamePasswordAuthenticationToken(parts[0],null,List.of(new SimpleGrantedAuthority("ROLE_"+parts[1])));org.springframework.security.core.context.SecurityContextHolder.getContext().setAuthentication(auth);redis.expire(key,8,TimeUnit.HOURS);}
    }
    chain.doFilter(req,res);
  }
}

@RestController @RequestMapping("/api/auth")
class AuthController {
  private final JdbcTemplate db;private final PasswordEncoder encoder;private final StringRedisTemplate redis;private final SecureRandom random=new SecureRandom();
  @Value("${resqnet.auth.token-ttl:PT8H}") private Duration ttl;
  AuthController(JdbcTemplate db,PasswordEncoder encoder,StringRedisTemplate redis){this.db=db;this.encoder=encoder;this.redis=redis;}
  @PostMapping("/login") LoginResponse login(@Valid @RequestBody LoginRequest body){
    if (!Set.of("HOSPITAL_ADMIN", "PARAMEDIC").contains(body.role())) throw new BadCredentialsException("Invalid login");
    var rows=db.query("select username,password_hash,role,enabled from app_user where username=? and role=?",(r,n)->new Object[]{r.getString(1),r.getString(2),r.getString(3),r.getBoolean(4)},body.username(),body.role());
    if(rows.isEmpty()||!(Boolean)rows.get(0)[3]||!encoder.matches(body.password(),(String)rows.get(0)[1]))throw new BadCredentialsException("Invalid login");
    byte[] b=new byte[32];random.nextBytes(b);String token=Base64.getUrlEncoder().withoutPadding().encodeToString(b);Instant expires=Instant.now().plus(ttl);
    redis.opsForValue().set("resqnet:session:"+token,rows.get(0)[0]+"|"+rows.get(0)[2],ttl);
    return new LoginResponse(token,"Bearer",expires,(String)rows.get(0)[0],(String)rows.get(0)[2]);
  }
  @PostMapping("/logout") ResponseEntity<Void> logout(@RequestHeader(value="Authorization",required=false) String header){if(header!=null&&header.startsWith("Bearer "))redis.delete("resqnet:session:"+header.substring(7));return ResponseEntity.noContent().build();}
}

@Component
class ZoneSeeder implements ApplicationRunner {
  static final List<String> CITY_NAMES=List.of("Bengaluru","Delhi","Mumbai","Srinagar","Hyderabad","Chennai","Kolkata","Pune");
  static final Map<String,double[]> CENTERS=Map.of("Bengaluru",new double[]{12.9716,77.5946},"Delhi",new double[]{28.6139,77.2090},"Mumbai",new double[]{19.0760,72.8777},"Srinagar",new double[]{34.0837,74.7973},"Hyderabad",new double[]{17.3850,78.4867},"Chennai",new double[]{13.0827,80.2707},"Kolkata",new double[]{22.5726,88.3639},"Pune",new double[]{18.5204,73.8567});
  static final String[][] NAMES={
    {"Victoria Hospital","Manipal Hospital","Narayana Health City","Apollo Hospitals Bannerghatta","Fortis Hospital Bengaluru","St. John’s Medical College Hospital","Aster CMI Hospital","Sakra World Hospital","Fortis Hospital Rajajinagar","Manipal Hospitals Jayanagar","Gleneagles BGS Hospital","Apollo Speciality Hospitals Jayanagar"},
    {"AIIMS New Delhi","Safdarjung Hospital","Apollo Hospitals Delhi","Fortis Escorts Heart Institute","Max Super Speciality Hospital","Sir Ganga Ram Hospital","Lok Nayak Hospital","BLK-Max Super Speciality Hospital","Dr. Ram Manohar Lohia Hospital","Hindu Rao Hospital","Guru Teg Bahadur Hospital","Baba Saheb Ambedkar Hospital"},
    {"KEM Hospital","Lilavati Hospital","Kokilaben Dhirubhai Ambani Hospital","Breach Candy Hospital","Nanavati Max Hospital","Sir H. N. Reliance Foundation Hospital","Tata Memorial Hospital","Jaslok Hospital","P.D. Hinduja Hospital","L H Hiranandani Hospital","Prince Aly Khan Hospital","Global Hospital Mumbai"},
    {"SKIMS Soura","SMHS Hospital","JVC Hospital Bemina","GMC Srinagar","Lal Ded Hospital","SKIMS Medical College Hospital","Shri Maharaja Hari Singh Hospital","Firdousa Hospital","JLNM Hospital","Chest Diseases Hospital Srinagar","Children Hospital Bemina","Bone and Joint Hospital Barzulla"},
    {"Apollo Hospitals Jubilee Hills","Yashoda Hospitals Secunderabad","NIMS Hyderabad","KIMS Hospitals","CARE Hospitals Banjara Hills","Osmania General Hospital","Continental Hospitals","AIG Hospitals","Gandhi Hospital","Gleneagles Global Hospitals","Rainbow Children Hospital","LV Prasad Eye Institute"},
    {"Apollo Hospitals Greams Road","Government General Hospital","MIOT International","Fortis Malar Hospital","Sri Ramachandra Medical Centre","MGM Healthcare","Kauvery Hospital","Rajiv Gandhi Government Hospital","Sankara Nethralaya","Vijaya Hospital","Gleneagles HealthCity Chennai","Frontier Lifeline Hospital"},
    {"SSKM Hospital","Apollo Multispeciality Hospitals","AMRI Hospitals Dhakuria","Fortis Hospital Anandapur","Medica Superspecialty Hospital","Belle Vue Clinic","NRS Medical College Hospital","Ruby General Hospital","R. G. Kar Medical College Hospital","Peerless Hospital","Narayana Hospital RN Tagore","Manipal Hospitals Broadway"},
    {"Sassoon General Hospital","Ruby Hall Clinic","Jehangir Hospital","Deenanath Mangeshkar Hospital","Sahyadri Super Speciality Hospital","Manipal Hospital Baner","Aditya Birla Memorial Hospital","Jupiter Hospital","Noble Hospital","Inamdar Multispeciality Hospital","Dr. D. Y. Patil Hospital","Sahyadri Hospital Hadapsar"}
  };
  static final double[][] OFF={{.012,.008},{.003,.019},{-.012,.013},{-.017,-.002},{-.007,-.016},{.008,-.018},{.019,-.006},{-.003,.003},{.024,.014},{-.023,.012},{.016,-.025},{-.021,-.021}};
  static final double[][] AMB={{.002,.001},{-.008,.004},{.009,-.006},{.004,.012},{-.014,-.007},{.013,.009},{-.004,-.014},{.016,.002},{.021,.011},{-.019,.015},{.011,-.021},{-.021,-.012}};
  private final JdbcTemplate db;ZoneSeeder(JdbcTemplate db){this.db=db;}
  @Override public void run(org.springframework.boot.ApplicationArguments args){for(String city:CENTERS.keySet())seed(city);}
  @Transactional public void seed(String city){int ci=CITY_NAMES.indexOf(city);double[] c=CENTERS.get(city);int[] beds={22,15,18,11,26,9,14,17,12,16,10,20},totalBeds={1000,600,500,300,400,1300,500,350,250,400,450,320};for(int i=0;i<NAMES[ci].length;i++){String code=String.format("H-%02d",i+1);if(db.queryForObject("select count(*) from hospital where city=? and code=?",Integer.class,city,code)==0)db.update("insert into hospital(city,code,name,latitude,longitude,total_beds,icu_beds,trauma,cardiac,stroke,burns) values(?,?,?,?,?,?,?,?,?,?,?)",city,code,NAMES[ci][i],c[0]+OFF[i][0],c[1]+OFF[i][1],totalBeds[i],beds[i],i%8!=5,true,i%8!=3&&i%8!=7,i%8==1||i%8==2||i%8==6||i%8==7);}
    int[] batt={94,82,76,99,68,88,57,91,78,86,72,95},head={32,155,260,80,315,190,10,225,110,275,45,200};for(int i=0;i<AMB.length;i++){int unit=101+i;if(db.queryForObject("select count(*) from ambulance where city=? and unit_number=?",Integer.class,city,unit)==0)db.update("insert into ambulance(city,unit_number,latitude,longitude,status,battery,heading,home_latitude,home_longitude) values(?,?,?,?,?,?,?,?,?)",city,unit,c[0]+AMB[i][0],c[1]+AMB[i][1],"AVAILABLE",batt[i],head[i],c[0]+AMB[i][0],c[1]+AMB[i][1]);}}
}

@Service
class ResqnetService {
  private final JdbcTemplate db;private final StringRedisTemplate redis;private final KafkaTemplate<String,Object> kafka;private final ObjectMapper json;private final RoadRoutingService routing;
  ResqnetService(JdbcTemplate db,StringRedisTemplate redis,KafkaTemplate<String,Object> kafka,ObjectMapper json,RoadRoutingService routing){this.db=db;this.redis=redis;this.kafka=kafka;this.json=json;this.routing=routing;}
  @Transactional(readOnly=true) BootstrapDto bootstrap(String city,boolean includeIncidents){double[] c=ZoneSeeder.CENTERS.get(city);if(c==null)throw new ResponseStatusException(HttpStatus.NOT_FOUND,"Unknown zone");var hs=db.query("select code,name,latitude,longitude,total_beds,icu_beds,trauma,cardiac,stroke,burns,obstetric from hospital where city=? order by code",(r,n)->new HospitalDto(r.getString(1),r.getString(2),r.getDouble(3),r.getDouble(4),r.getInt(5),r.getInt(6),r.getBoolean(7),r.getBoolean(8),r.getBoolean(9),r.getBoolean(10),r.getBoolean(11),lockTtl(city,r.getString(1))),city);var as=db.query("select unit_number,latitude,longitude,status,battery,heading,location_updated_at,gps_source from ambulance where city=? order by unit_number",(r,n)->new AmbulanceDto(r.getInt(1),"Rescue Unit "+r.getInt(1),r.getDouble(2),r.getDouble(3),r.getString(4),r.getInt(5),r.getInt(6),r.getTimestamp(7).toInstant(),r.getString(8)),city);var ins=includeIncidents?db.query("select id,incident_number,type,triage,latitude,longitude,notes,status,ambulance_unit,hospital_code,created_at from incident where city=? order by created_at desc limit 100",this::incident,city):List.<IncidentDto>of();return new BootstrapDto(city,c[0],c[1],hs,as,ins);}
  int lockTtl(String city,String hospital){Long ttl=redis.getExpire("resqnet:lock:"+city+":"+hospital,TimeUnit.SECONDS);return ttl==null||ttl<0?0:Math.toIntExact(ttl);}
  private void releaseLock(String key,String token){String script="if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";redis.execute(new org.springframework.data.redis.core.script.DefaultRedisScript<>(script,Long.class),List.of(key),token);}
  @Transactional IncidentDto create(String city,CreateIncidentRequest req){
    if(!ZoneSeeder.CENTERS.containsKey(city))throw new ResponseStatusException(HttpStatus.NOT_FOUND,"Unknown zone");
    if(!Set.of("Critical","Urgent","Standard").contains(req.triage()))throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Invalid triage");
    var candidates=new ArrayList<>(db.query("select unit_number,latitude,longitude from ambulance where city=? and status='AVAILABLE' and location_updated_at>=now()-interval '20 seconds'",(r,n)->new Object[]{r.getInt(1),r.getDouble(2),r.getDouble(3)},city));
    List<Double> roadTimes=routing.ambulanceTravelTimes(candidates,req.latitude(),req.longitude());
    if(roadTimes.size()==candidates.size()){Map<Integer,Double> byUnit=new HashMap<>();for(int i=0;i<candidates.size();i++)byUnit.put((int)candidates.get(i)[0],roadTimes.get(i));candidates.sort(Comparator.comparingDouble(a->{double seconds=byUnit.getOrDefault((int)a[0],Double.POSITIVE_INFINITY);return Double.isFinite(seconds)?seconds:1_000_000_000d+distance(req.latitude(),req.longitude(),(double)a[1],(double)a[2]);}));}
    else candidates.sort(Comparator.comparingDouble(a->distance(req.latitude(),req.longitude(),(double)a[1],(double)a[2])));
    Object[] amb=null;
    for(Object[] candidate:candidates){
      int unit=(int)candidate[0];String lock="resqnet:dispatch-lock:"+city+":unit:"+unit;String lockToken=UUID.randomUUID().toString();
      Boolean acquired=redis.opsForValue().setIfAbsent(lock,lockToken,15,TimeUnit.SECONDS);
      if(!Boolean.TRUE.equals(acquired))continue;
      try{if(db.update("update ambulance set status='DISPATCHED',updated_at=now() where city=? and unit_number=? and status='AVAILABLE'",city,unit)==1){amb=candidate;break;}}
      finally{releaseLock(lock,lockToken);}
    }
     if(amb==null)throw new ResponseStatusException(HttpStatus.CONFLICT,"No available ambulance is currently sharing live GPS in this zone");
    String required=specialtyFor(req.type());
    String sql="select code,latitude,longitude from hospital where city=? and icu_beds>0"+specialtyPredicate(required)+" order by code";
    var hs=db.query(sql,(r,n)->new Object[]{r.getString(1),r.getDouble(2),r.getDouble(3)},city);
    if(hs.isEmpty())throw new ResponseStatusException(HttpStatus.CONFLICT,required==null?"No receiving hospital has an available ICU bed":"No hospital currently has both an available ICU bed and the required "+required+" team");
    List<Double> hospitalTimes=routing.travelTimesToCandidates(req.latitude(),req.longitude(),hs);
    Map<Integer,Double> hospitalTimeByIndex=new HashMap<>();
    if(hospitalTimes.size()==hs.size())for(int i=0;i<hs.size();i++)hospitalTimeByIndex.put(i,hospitalTimes.get(i));
    List<Integer> hospitalOrder=new ArrayList<>();for(int i=0;i<hs.size();i++)hospitalOrder.add(i);
    hospitalOrder.sort(Comparator.comparingDouble(i->{double seconds=hospitalTimeByIndex.getOrDefault(i,Double.POSITIVE_INFINITY);return Double.isFinite(seconds)?seconds:distance(req.latitude(),req.longitude(),(double)hs.get(i)[1],(double)hs.get(i)[2]);}));
    Object[] hosp=null;int selectedHospitalIndex=-1;
    for(int index:hospitalOrder){Object[] candidate=hs.get(index);if(db.update("update hospital set icu_beds=icu_beds-1,updated_at=now() where city=? and code=? and icu_beds>0"+specialtyPredicate(required),city,candidate[0])==1){hosp=candidate;selectedHospitalIndex=index;break;}}
    if(hosp==null)throw new ResponseStatusException(HttpStatus.CONFLICT,"Eligible hospitals ran out of available ICU beds; refresh and retry");
    String number="INC-"+UUID.randomUUID().toString().substring(0,8).toUpperCase(Locale.ROOT);UUID id=UUID.randomUUID();
    db.update("insert into incident(id,city,incident_number,type,triage,latitude,longitude,notes,status,ambulance_unit,hospital_code) values(?,?,?,?,?,?,?,?,?,?,?)",id,city,number,req.type(),req.triage(),req.latitude(),req.longitude(),req.notes(),"DISPATCHED",amb[0],hosp[0]);
    var dto=db.queryForObject("select id,incident_number,type,triage,latitude,longitude,notes,status,ambulance_unit,hospital_code,created_at from incident where id=?",this::incident,id);
    publish("INCIDENT_DISPATCHED",Map.of("city",city,"incident",dto,"hospital",hosp[0],"requiredSpecialty",required==null?"general":required,"hospitalRoadEtaSeconds",hospitalTimeByIndex.getOrDefault(selectedHospitalIndex,-1d)));return dto;
  }
  AmbulanceRecommendationDto recommend(String city,double latitude,double longitude){
    if(!ZoneSeeder.CENTERS.containsKey(city))throw new ResponseStatusException(HttpStatus.NOT_FOUND,"Unknown zone");
    var candidates=new ArrayList<>(db.query("select unit_number,latitude,longitude from ambulance where city=? and status='AVAILABLE' and location_updated_at>=now()-interval '20 seconds'",(r,n)->new Object[]{r.getInt(1),r.getDouble(2),r.getDouble(3)},city));
    if(candidates.isEmpty())throw new ResponseStatusException(HttpStatus.CONFLICT,"No available ambulance is currently sharing live GPS in this zone");
    List<Double> times=routing.ambulanceTravelTimes(candidates,latitude,longitude);
    if(times.size()==candidates.size()){
      Map<Integer,Double> byUnit=new HashMap<>();for(int i=0;i<candidates.size();i++)byUnit.put((int)candidates.get(i)[0],times.get(i));
      candidates.sort(Comparator.comparingDouble(a->byUnit.getOrDefault((int)a[0],Double.POSITIVE_INFINITY)));
      Object[] first=candidates.get(0);Double eta=byUnit.get((int)first[0]);
      if(eta!=null&&Double.isFinite(eta))return new AmbulanceRecommendationDto((int)first[0],eta,"OSRM road network");
    }
    candidates.sort(Comparator.comparingDouble(a->distance(latitude,longitude,(double)a[1],(double)a[2])));
    return new AmbulanceRecommendationDto((int)candidates.get(0)[0],null,"straight-line fallback");
  }
  @Transactional IncidentDto status(UUID id,String status){if(!Set.of("TRANSPORTING","COMPLETED").contains(status))throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Status must be TRANSPORTING or COMPLETED");var current=db.query("select city,ambulance_unit,status from incident where id=? for update",(r,n)->new Object[]{r.getString(1),r.getInt(2),r.getString(3)},id);if(current.isEmpty())throw new ResponseStatusException(HttpStatus.NOT_FOUND,"Incident not found");Object[] v=current.get(0);if("COMPLETED".equals(v[2]))throw new ResponseStatusException(HttpStatus.CONFLICT,"Incident is already completed");db.update("update incident set status=?,updated_at=now() where id=?",status,id);if("COMPLETED".equals(status))db.update("update ambulance set status=case when gps_source='DEMO' then 'RETURNING' else 'AVAILABLE' end,updated_at=now() where city=? and unit_number=?",v[0],v[1]);var dto=db.queryForObject("select id,incident_number,type,triage,latitude,longitude,notes,status,ambulance_unit,hospital_code,created_at from incident where id=?",this::incident,id);publish("INCIDENT_"+status,Map.of("incident",dto));return dto;}
  @Transactional void capacity(String city,String code,int beds){String lock="resqnet:lock:"+city+":"+code;String lockToken=UUID.randomUUID().toString();Boolean acquired=redis.opsForValue().setIfAbsent(lock,lockToken,15,TimeUnit.SECONDS);if(!Boolean.TRUE.equals(acquired))throw new ResponseStatusException(HttpStatus.CONFLICT,"Hospital record is being updated; retry shortly");try{int n=db.update("update hospital set icu_beds=?,updated_at=now() where city=? and code=?",beds,city,code);if(n==0)throw new ResponseStatusException(HttpStatus.NOT_FOUND,"Hospital not found");publish("HOSPITAL_CAPACITY_UPDATED",Map.of("city",city,"hospital",code,"icuBeds",beds));}finally{releaseLock(lock,lockToken);}}
  @Transactional void specialistAvailability(String city,String code,String specialty,boolean available){String column=switch(specialty.toLowerCase(Locale.ROOT)){case "trauma","cardiac","stroke","burns","obstetric"->specialty.toLowerCase(Locale.ROOT);default->throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Unsupported specialist service");};int n=db.update("update hospital set "+column+"=?,updated_at=now() where city=? and code=?",available,city,code);if(n==0)throw new ResponseStatusException(HttpStatus.NOT_FOUND,"Hospital not found");publish("HOSPITAL_SPECIALIST_AVAILABILITY_UPDATED",Map.of("city",city,"hospital",code,"specialty",column,"available",available));}
  private static String specialtyFor(String type){String value=type.toLowerCase(Locale.ROOT);if(value.contains("cardiac"))return "cardiac";if(value.contains("stroke"))return "stroke";if(value.contains("burn")||value.contains("fire"))return "burns";if(value.contains("trauma")||value.contains("collision")||value.contains("injury"))return "trauma";if(value.contains("obstetric"))return "obstetric";return null;}
  private static String specialtyPredicate(String specialty){return specialty==null?"":" and "+specialty+"=true";}
  List<EventDto> events(){List<String> values=redis.opsForList().range("resqnet:telemetry",0,99);if(values==null)return List.of();return values.stream().map(s->{try{return json.readValue(s,EventDto.class);}catch(Exception e){return new EventDto("", "INFO", s);}}).toList();}
  private void publish(String type,Object data){String message;try{message=json.writeValueAsString(data);}catch(Exception e){message="{}";}var event=new EventDto(Instant.now().toString(),type,message);try{redis.opsForList().leftPush("resqnet:telemetry",json.writeValueAsString(event));redis.opsForList().trim("resqnet:telemetry",0,99);kafka.send("resqnet.events",type,message).whenComplete((r,e)->{if(e!=null)System.err.println("Kafka publish failed: "+e.getMessage());});}catch(Exception e){System.err.println("Telemetry publish failed: "+e.getMessage());}}
  private IncidentDto incident(java.sql.ResultSet r,int n)throws java.sql.SQLException{return new IncidentDto(r.getString("id"),r.getString("incident_number"),r.getString("type"),r.getString("triage"),r.getDouble("latitude"),r.getDouble("longitude"),r.getString("notes"),r.getString("status"),r.getInt("ambulance_unit"),r.getString("hospital_code"),r.getTimestamp("created_at").toInstant());}
  private static double distance(double lat1,double lon1,double lat2,double lon2){double a=Math.toRadians(lat2-lat1),b=Math.toRadians(lon2-lon1);double x=Math.sin(a/2)*Math.sin(a/2)+Math.cos(Math.toRadians(lat1))*Math.cos(Math.toRadians(lat2))*Math.sin(b/2)*Math.sin(b/2);return 6371*2*Math.atan2(Math.sqrt(x),Math.sqrt(1-x));}
}

@Service
class AmbulanceTrackerService {
  private final JdbcTemplate db; private final SimpMessagingTemplate sockets; private final KafkaTemplate<String,Object> kafka;
  private final SecureRandom random = new SecureRandom();
  AmbulanceTrackerService(JdbcTemplate db, SimpMessagingTemplate sockets, KafkaTemplate<String,Object> kafka) { this.db = db; this.sockets=sockets; this.kafka=kafka; }

  @Transactional
  TrackerKeyDto provision(String city, int unit) {
    if (!ZoneSeeder.CENTERS.containsKey(city)) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown zone");
    if (db.queryForObject("select count(*) from ambulance where city=? and unit_number=?", Integer.class, city, unit) == 0)
      throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Ambulance unit not found in this zone");
    byte[] bytes = new byte[32]; random.nextBytes(bytes);
    String token = Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    String trackerId = UUID.randomUUID().toString();
    String hash = hash(token);
    db.update("insert into ambulance_tracker(tracker_id,city,unit_number,token_hash,enabled) values(?,?,?,?,true) on conflict(city,unit_number) do update set tracker_id=excluded.tracker_id,token_hash=excluded.token_hash,enabled=true,last_seen_at=null,created_at=now()",
      trackerId, city, unit, hash);
    return new TrackerKeyDto(trackerId, token, "/api/tracking/" + trackerId + "/location");
  }

  @Transactional
  void ingest(String trackerId, String token, TrackerLocationRequest position) {
    var trackers = db.query("select city,unit_number,token_hash from ambulance_tracker where tracker_id=? and enabled=true", (r,n) -> new Object[]{r.getString(1),r.getInt(2),r.getString(3)}, trackerId);
    if (trackers.isEmpty() || token == null || !MessageDigest.isEqual(hash(token).getBytes(StandardCharsets.US_ASCII), (trackers.isEmpty() ? "" : (String)trackers.get(0)[2]).getBytes(StandardCharsets.US_ASCII)))
      throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Invalid tracker credentials");
    Object[] tracker = trackers.get(0);
    int changed = db.update("update ambulance set latitude=?,longitude=?,heading=coalesce(?,heading),battery=coalesce(?,battery),gps_source='LIVE',location_updated_at=now(),updated_at=now() where city=? and unit_number=?",
      position.latitude(), position.longitude(), position.heading(), position.battery(), tracker[0], tracker[1]);
    if (changed == 0) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Ambulance unit not found");
    db.update("update ambulance_tracker set last_seen_at=now() where tracker_id=?", trackerId);
    String city=(String)tracker[0];int unit=(int)tracker[1];String status=db.queryForObject("select status from ambulance where city=? and unit_number=?",String.class,city,unit);
    Map<String,Object> event=Map.of("type","AMBULANCE_LOCATION","city",city,"unitNumber",unit,"latitude",position.latitude(),"longitude",position.longitude(),"heading",position.heading()==null?0:position.heading(),"status",status,"phase",status.equals("AVAILABLE")?"IDLE":"EN_ROUTE","gpsSource","LIVE","updatedAt",Instant.now().toString());
    sockets.convertAndSend("/topic/zones/"+city+"/ambulances",event);try{kafka.send("resqnet.events",city,event);}catch(Exception ignored){}
  }

  private static String hash(String value) {
    try { return Base64.getEncoder().encodeToString(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8))); }
    catch (NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
  }
}

@RestController
class ApiController {
  private final ResqnetService service;private final JdbcTemplate db;private final RoadRoutingService routing;private final MedicalFacilityService facilities;private final AmbulanceTrackerService trackers;private final DemoSimulationService demo;
  ApiController(ResqnetService service,JdbcTemplate db,RoadRoutingService routing,MedicalFacilityService facilities,AmbulanceTrackerService trackers,DemoSimulationService demo){this.service=service;this.db=db;this.routing=routing;this.facilities=facilities;this.trackers=trackers;this.demo=demo;}
  @GetMapping("/api/zones/{city}/bootstrap") BootstrapDto bootstrap(@PathVariable String city,Authentication auth){boolean signedIn=auth!=null&&!(auth instanceof org.springframework.security.authentication.AnonymousAuthenticationToken);return service.bootstrap(city,signedIn);}
  @GetMapping("/api/zones/{city}/facilities") List<MedicalFacilityDto> facilities(@PathVariable String city,@RequestParam(defaultValue="false") boolean refresh,@RequestParam(required=false) Double south,@RequestParam(required=false) Double west,@RequestParam(required=false) Double north,@RequestParam(required=false) Double east){boolean any=south!=null||west!=null||north!=null||east!=null;if(!any)return facilities.byCity(city,refresh);if(south==null||west==null||north==null||east==null)throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"All map bounds are required");return facilities.byBounds(city,refresh,south,west,north,east);}
  @GetMapping("/api/zones/{city}/ambulance-recommendation") AmbulanceRecommendationDto recommendation(@PathVariable String city,@RequestParam double latitude,@RequestParam double longitude){if(Math.abs(latitude)>90||Math.abs(longitude)>180)throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Coordinates are outside valid ranges");return service.recommend(city,latitude,longitude);}
  @PostMapping("/api/zones/{city}/demo/start") ResponseEntity<Void> startDemo(@PathVariable String city,Authentication auth){requireOperator(auth);demo.start(city);return ResponseEntity.noContent().build();}
  @PostMapping("/api/zones/{city}/demo/stop") ResponseEntity<Void> stopDemo(@PathVariable String city,Authentication auth){requireOperator(auth);demo.stop(city);return ResponseEntity.noContent().build();}
  @GetMapping("/api/zones/{city}/demo") Map<String,Boolean> demoStatus(@PathVariable String city){if(!ZoneSeeder.CENTERS.containsKey(city))throw new ResponseStatusException(HttpStatus.NOT_FOUND,"Unknown zone");return Map.of("enabled",demo.enabled(city));}
  private static void requireOperator(Authentication auth){if(auth==null||auth instanceof org.springframework.security.authentication.AnonymousAuthenticationToken||auth.getAuthorities().stream().noneMatch(a->Set.of("ROLE_HOSPITAL_ADMIN","ROLE_PARAMEDIC").contains(a.getAuthority())))throw new ResponseStatusException(HttpStatus.FORBIDDEN,"Sign in as a hospital administrator or paramedic");}
  @PostMapping("/api/zones/{city}/ambulances/{unit}/tracker-key") TrackerKeyDto trackerKey(@PathVariable String city,@PathVariable int unit,Authentication auth){if(!auth.getAuthorities().contains(new SimpleGrantedAuthority("ROLE_HOSPITAL_ADMIN")))throw new ResponseStatusException(HttpStatus.FORBIDDEN,"Hospital administrator role required");return trackers.provision(city,unit);}
  @PostMapping("/api/tracking/{trackerId}/location") ResponseEntity<Void> trackerLocation(@PathVariable String trackerId,@RequestHeader(value="X-Tracker-Token",required=false) String key,@Valid @RequestBody TrackerLocationRequest body){trackers.ingest(trackerId,key,body);return ResponseEntity.noContent().build();}
  @GetMapping("/api/routes") RoadRouteDto route(@RequestParam double fromLat,@RequestParam double fromLon,@RequestParam double toLat,@RequestParam double toLon){if(Math.abs(fromLat)>90||Math.abs(toLat)>90||Math.abs(fromLon)>180||Math.abs(toLon)>180)throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Coordinates are outside valid ranges");return routing.route(fromLat,fromLon,toLat,toLon);}
  @PostMapping("/api/zones/{city}/incidents") IncidentDto create(@PathVariable String city,@Valid @RequestBody CreateIncidentRequest req){return service.create(city,req);}
  @PatchMapping("/api/incidents/{id}/status") IncidentDto status(@PathVariable UUID id,@Valid @RequestBody IncidentStatusRequest req){return service.status(id,req.status());}
  @PatchMapping("/api/zones/{city}/hospitals/{code}/capacity") ResponseEntity<Void> capacity(@PathVariable String city,@PathVariable String code,@Valid @RequestBody BedCapacityRequest req,Authentication auth){if(!auth.getAuthorities().contains(new SimpleGrantedAuthority("ROLE_HOSPITAL_ADMIN")))throw new ResponseStatusException(HttpStatus.FORBIDDEN,"Hospital administrator role required");service.capacity(city,code,req.icuBeds());return ResponseEntity.noContent().build();}
  @PatchMapping("/api/zones/{city}/hospitals/{code}/readiness") ResponseEntity<Void> readiness(@PathVariable String city,@PathVariable String code,@Valid @RequestBody SpecialistAvailabilityRequest req,Authentication auth){if(!auth.getAuthorities().contains(new SimpleGrantedAuthority("ROLE_HOSPITAL_ADMIN")))throw new ResponseStatusException(HttpStatus.FORBIDDEN,"Hospital administrator role required");service.specialistAvailability(city,code,req.specialty(),req.available());return ResponseEntity.noContent().build();}
  @GetMapping("/api/telemetry") List<EventDto> telemetry(){return service.events();}
  @GetMapping("/api/me") Map<String,String> me(Authentication auth){return Map.of("username",auth.getName(),"role",auth.getAuthorities().iterator().next().getAuthority().replace("ROLE_",""));}
}
