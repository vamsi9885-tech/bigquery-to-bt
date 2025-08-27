# --- OAuth2 dummy config for local ---
spring.security.oauth2.client.registration.local.client-id=test-client
spring.security.oauth2.client.registration.local.client-secret=test-secret
spring.security.oauth2.client.registration.local.authorization-grant-type=authorization_code
spring.security.oauth2.client.registration.local.redirect-uri=http://localhost:8080/login/oauth2/code/local
spring.security.oauth2.client.registration.local.scope=openid,profile,email

spring.security.oauth2.client.provider.local.authorization-uri=http://localhost:8080/oauth2/authorize
spring.security.oauth2.client.provider.local.token-uri=http://localhost:8080/oauth2/token
spring.security.oauth2.client.provider.local.user-info-uri=http://localhost:8080/userinfo
spring.security.oauth2.client.provider.local.user-name-attribute=sub
