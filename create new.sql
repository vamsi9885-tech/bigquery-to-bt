I have a spring appplication ran it using gradle it is running showing tomcat server started 
then hit the api api/clients get  the data 
then there is another api asp/session/me it is throw me the erorr 
then i checked the controller code is 
@Slf4j
@RestController
@RequestMapping("/api/session")
public class SessionController {
    /**
     * OIDC properties. Marked as optional to avoid unsatisfied dependency in test context.
     */
    @Autowired(required = false)
    private OIDCProperties oidcProperties;

    @GetMapping("/me")
    public ResponseEntity<UserDto> getLoggedInUserDetails() {
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (authentication == null || !authentication.isAuthenticated() || "anonymousUser".equals(authentication.getPrincipal())) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }
        if (authentication.getPrincipal() instanceof org.springframework.security.oauth2.core.oidc.user.DefaultOidcUser) {
            DefaultOidcUser userDetails = (DefaultOidcUser)authentication.getPrincipal();
            log.info("Login User details: {}", userDetails);
            UserDto userDto = new UserDto();
            userDto.setUserId(userDetails.getPreferredUsername());
            userDto.setUserEmail(userDetails.getEmail());
            userDto.setUserName(userDetails.getClaim("name").toString());
            return ResponseEntity.ok(userDto);
        }

        return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
    }


then I checked the autehncation earlier it is variable bases id condition 
now changed it to the default true 
package com.optum.dap.api.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.client.registration.ClientRegistrationRepository;
import org.springframework.security.oauth2.client.web.DefaultOAuth2AuthorizationRequestResolver;
import org.springframework.security.oauth2.client.web.OAuth2AuthorizationRequestRedirectFilter;
import org.springframework.security.oauth2.client.web.OAuth2AuthorizationRequestResolver;
import org.springframework.security.oauth2.core.endpoint.OAuth2AuthorizationRequest;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.access.expression.WebExpressionAuthorizationManager;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;  
import java.util.List;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import com.optum.dap.api.filter.JwtAuthenticationFilter;

@Slf4j
@Configuration
public class SecurityConfig {

    @Value("${DISABLE_AUTH}")
    private boolean disableAuth;

    @Value("${HomePageURL}")
    private String homePageURL;

    @Autowired
    private JwtAuthenticationFilter jwtAuthenticationFilter;

    @Autowired
    private ClientRegistrationRepository clientRegistrationRepository;
    
    @Autowired
    private CustomOAuth2SuccessHandler customOAuth2SuccessHandler;

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        log.info("diable status" + disableAuth);
        if (true) {
            log.info("DISABLE_AUTH is true. Allowing all requests without authentication.");
            http.authorizeHttpRequests(authorize -> authorize.anyRequest().permitAll());
        } else {
            log.info("DISABLE_AUTH is false. Applying authentication rules.");

            http
            .csrf(
                csrf -> csrf
                    .ignoringRequestMatchers("/api/**"))
            .cors(cors -> cors.configurationSource(corsConfigurationSource()))
                .authorizeHttpRequests(authorize -> authorize
                    .requestMatchers("/api/oauth2/authorization/**", "/api/login/oauth2/code/**", 
                                    "/api/heartbeat", "/api/session/me", "/api/access-denied","/api/session/logout").permitAll()
                    .requestMatchers("/api/clients/auth/**").access(new WebExpressionAuthorizationManager(
                        "isAuthenticated() and hasAuthority('ROLE_CLIENT_WRITE')"
                    ))
                    .anyRequest().access(new WebExpressionAuthorizationManager(
                        "isAuthenticated() and !hasAuthority('ROLE_CLIENT_WRITE')"
                ))
                )
                .oauth2Login(oauth2 -> oauth2
                    .authorizationEndpoint(authorization -> authorization.baseUri("/api/oauth2/authorization")
                        .authorizationRequestResolver(customAuthorizationRequestResolver(clientRegistrationRepository))
                    )
                    .redirectionEndpoint(redir -> redir.baseUri("/api/login/oauth2/code/*"))
                    .successHandler(customOAuth2SuccessHandler)
                )
                .exceptionHandling(exceptions -> exceptions
                    .accessDeniedPage("/api/access-denied")
                );

                http.addFilterBefore(jwtAuthenticationFilter, UsernamePasswordAuthenticationFilter.class);

                http.sessionManagement((session) -> session
                .sessionCreationPolicy(SessionCreationPolicy.IF_REQUIRED)
            );
            
        }

        return http.build();
    }
    @Bean
    public CorsConfigurationSource corsConfigurationSource() {
        CorsConfiguration configuration = new CorsConfiguration();
        configuration.setAllowedOrigins(List.of(homePageURL));
        configuration.setAllowCredentials(true);
        configuration.setAllowedMethods(List.of("GET", "POST", "PUT", "DELETE", "OPTIONS"));
        configuration.setAllowedHeaders(List.of("Authorization", "Content-Type"));

        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/**", configuration);
        return source;
    }
    @Bean
    public OAuth2AuthorizationRequestResolver customAuthorizationRequestResolver(ClientRegistrationRepository clientRegistrationRepository) {
        DefaultOAuth2AuthorizationRequestResolver defaultResolver = new DefaultOAuth2AuthorizationRequestResolver(
            clientRegistrationRepository, "/api/oauth2/authorization"
        );

        return new OAuth2AuthorizationRequestResolver() {
            @Override
            public OAuth2AuthorizationRequest resolve(HttpServletRequest request) {
                OAuth2AuthorizationRequest authorizationRequest = defaultResolver.resolve(request);
                if (authorizationRequest != null) {
                    return OAuth2AuthorizationRequest.from(authorizationRequest)
                        .additionalParameters(params -> params.put("kc_idp_hint", "uhg-ms-id"))
                        .build();
                }
                return null;
            }

            @Override
            public OAuth2AuthorizationRequest resolve(HttpServletRequest request, String clientId) {
                OAuth2AuthorizationRequest authorizationRequest = defaultResolver.resolve(request, clientId);
                if (authorizationRequest != null) {
                    return OAuth2AuthorizationRequest.from(authorizationRequest)
                        .additionalParameters(params -> params.put("kc_idp_hint", "uhg-ms-id"))
                        .build();
                }
                return null;
            }
        };
    }

}

is there way to setit up in local I ma trying hard 
