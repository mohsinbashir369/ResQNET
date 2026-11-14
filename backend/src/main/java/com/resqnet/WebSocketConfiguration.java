package com.resqnet;

import org.springframework.context.annotation.Configuration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.simp.config.ChannelRegistration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompHeaderAccessor;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.MessageHeaderAccessor;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.web.socket.config.annotation.EnableWebSocketMessageBroker;
import org.springframework.web.socket.config.annotation.StompEndpointRegistry;
import org.springframework.web.socket.config.annotation.WebSocketMessageBrokerConfigurer;

import java.util.List;

@Configuration
@EnableWebSocketMessageBroker
class WebSocketConfiguration implements WebSocketMessageBrokerConfigurer {
  private final StringRedisTemplate redis;
  private final List<String> allowedOrigins;
  WebSocketConfiguration(StringRedisTemplate redis,
      @Value("${resqnet.web.allowed-origins:http://localhost:5173,http://127.0.0.1:5173}") String origins) {
    this.redis = redis;
    this.allowedOrigins = java.util.Arrays.stream(origins.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList();
  }

  @Override public void configureMessageBroker(MessageBrokerRegistry registry) {
    registry.enableSimpleBroker("/topic");
    registry.setApplicationDestinationPrefixes("/app");
  }

  @Override public void registerStompEndpoints(StompEndpointRegistry registry) {
    registry.addEndpoint("/ws").setAllowedOrigins(allowedOrigins.toArray(String[]::new));
  }

  @Override public void configureClientInboundChannel(ChannelRegistration registration) {
    registration.interceptors(new ChannelInterceptor() {
      @Override public Message<?> preSend(Message<?> message, MessageChannel channel) {
        StompHeaderAccessor accessor = MessageHeaderAccessor.getAccessor(message, StompHeaderAccessor.class);
        if (accessor == null) return message;
        if (StompCommand.CONNECT.equals(accessor.getCommand())) {
          String header = accessor.getFirstNativeHeader("authorization");
          if (header == null || !header.startsWith("Bearer ")) return null;
          String value = redis.opsForValue().get("resqnet:session:" + header.substring(7));
          if (value == null) return null;
          String[] account = value.split("\\|", 2);
          accessor.setUser(new UsernamePasswordAuthenticationToken(account[0], null,
            List.of(new SimpleGrantedAuthority("ROLE_" + account[1]))));
        } else if ((StompCommand.SUBSCRIBE.equals(accessor.getCommand()) || StompCommand.SEND.equals(accessor.getCommand()))
            && accessor.getUser() == null) {
          return null;
        }
        return message;
      }
    });
  }
}
