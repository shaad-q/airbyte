package io.airbyte.integrations.source.mongodb;

import static io.airbyte.integrations.source.mongodb.MongoSslUtils.SslMode.CCV;
import static io.airbyte.integrations.source.mongodb.MongoSslUtils.SslMode.DISABLED;
import static io.airbyte.integrations.source.mongodb.MongoSslUtils.SslMode.AWS;
import static io.airbyte.integrations.source.mongodb.MongoConstants.AWS_CA_CERTIFICATE;
import static io.airbyte.integrations.source.mongodb.MongoConstants.PARAM_SSL_MODE_CCV;
import static io.airbyte.integrations.source.mongodb.MongoConstants.CLIENT_CERTIFICATE;
import static io.airbyte.integrations.source.mongodb.MongoConstants.CLIENT_CA_CERTIFICATE;
import static io.airbyte.integrations.source.mongodb.MongoConstants.CLIENT_KEY;
import static io.airbyte.integrations.source.mongodb.MongoConstants.CLIENT_KEY_STORE;
import static io.airbyte.integrations.source.mongodb.MongoConstants.KEY_STORE_TYPE;
import static io.airbyte.integrations.source.mongodb.MongoConstants.TRUST_STORE;
import static io.airbyte.integrations.source.mongodb.MongoConstants.TRUST_PASSWORD;
import static io.airbyte.integrations.source.mongodb.MongoConstants.TRUST_TYPE;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoSslUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(MongoSslUtils.class);

  public static void setupCertificates(
    final String sslMode,
    final String caCertificate,
    final String clientCertificate,
    final String clientKey,
    final String clientKeyStorePassword,
    final String clientKeyPassword,
    final String trustStorePassword
  ) {
    try {
      if (getSslVerifyMode(sslMode) == CCV) {
        LOGGER.info("Preparing SSL certificates for '{}' mode", PARAM_SSL_MODE_CCV);
        initCertificateStores(
          caCertificate,
          clientCertificate,
          clientKey,
          clientKeyStorePassword,
          getOrGeneratePassword(clientKeyPassword)
        );
      } else if (getSslVerifyMode(sslMode) == AWS) {
        LOGGER.info("Preparing SSL certificates for '{}' mode", AWS_CA_CERTIFICATE);
        initAWSCertificateStores(
          caCertificate,
          getOrGeneratePassword(trustStorePassword)
        );
      }
    } catch (final IOException | InterruptedException e) {
      throw new RuntimeException("Failed to import certificate into Java Keystore");
    }
  }
  
  private static String getOrGeneratePassword(final String clientKeyPassword) {
    return clientKeyPassword != null && !clientKeyPassword.isEmpty() ? clientKeyPassword : RandomStringUtils.randomAlphanumeric(10);
  }

  private static void initAWSCertificateStores(
    final String caCertificate,
    final String trustStorePassword
  )
  throws IOException, InterruptedException {

    LOGGER.info("Try to Split '{}'", AWS_CA_CERTIFICATE);
    String[] certificates = caCertificate.split("(?<=-----END CERTIFICATE-----)");

    for (int i = 0; i < certificates.length; i++) {
      String certFileName = "rds-ca-" + i + ".pem";
      createCertificateFile(certFileName, certificates[i] + "\n");

      // Import the certificates in the truststore
      String alias = getCertificateAlias(certFileName).replace(' ', '-');
      LOGGER.info("Importing '{}'", alias);
      runProcess(String.format("keytool -import -file %s -alias \"%s\" -keystore %s -storepass %s -noprompt",
        certFileName,
        alias,
        TRUST_STORE,
        trustStorePassword
      ));
      runProcess(String.format("rm %s",
        certFileName
      ));
      LOGGER.info("Removed '{}'", certFileName);
    }
    LOGGER.info("'{}' Generated", TRUST_STORE);
    setTrustStore(trustStorePassword);
  }

  private static void initCertificateStores(
    final String caCertificate,
    final String clientCertificate,
    final String clientKey,
    final String clientKeyStorePassword,
    final String clientKeyPassword
  )
  throws IOException, InterruptedException {

    LOGGER.info("Try to generate '{}'", CLIENT_KEY_STORE);
    createCertificateFile(CLIENT_CERTIFICATE, clientCertificate);
    createCertificateFile(CLIENT_KEY, clientKey);

    runProcess(String.format("openssl pkcs12 -export -in %s -inkey %s -out %s -passout pass:%s -passin pass:%s",
        CLIENT_CERTIFICATE,
        CLIENT_KEY,
        CLIENT_KEY_STORE,
        clientKeyStorePassword,
        clientKeyPassword));
    LOGGER.info("'{}' Generated", CLIENT_KEY_STORE);

    // Import the SSL certificates in the truststore

    LOGGER.info("Try to generate '{}'", TRUST_STORE);
    createCertificateFile(CLIENT_CA_CERTIFICATE, caCertificate);
    
    runProcess(String.format("keytool -import -file %s -alias mongoClient -keystore %s -storepass %s -noprompt",
        CLIENT_CA_CERTIFICATE,
        TRUST_STORE,
        TRUST_PASSWORD));
    LOGGER.info("'{}' Generated", TRUST_STORE);

    setKeyStore(clientKeyPassword);
    setTrustStore(TRUST_PASSWORD);
  }

  private static String getCertificateAlias(String certFileName) throws IOException, InterruptedException {
    String alias = null;

    String cmd = "openssl x509 -noout -text -in " + certFileName;
    ProcessBuilder processBuilder = new ProcessBuilder(cmd.split("\\s+"));
    Process process = processBuilder.start();
    BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
    String line;
    StringBuilder keytoolOutput = new StringBuilder();

    while ((line = reader.readLine()) != null) {
      keytoolOutput.append(line).append("\n");
    }

    int exitCode = process.waitFor();
    if (exitCode == 0) {
      String[] lines = keytoolOutput.toString().split("\n");

      for (String outputLine : lines) {
          if (outputLine.trim().startsWith("Subject:")) {
            int index = outputLine.indexOf("CN=");
            int offset = 3;
            if (index == -1) {
                index = outputLine.indexOf("CN = ");
                offset = 5;
            }
            if (index != -1) {
              int startIndex = index + offset;
              int endIndex = outputLine.indexOf(",", startIndex);
              if (endIndex == -1) {
                  endIndex = outputLine.length();
              }
              alias = outputLine.substring(startIndex, endIndex).trim();
            }
          }
      }
    }

    return alias;
  }

  private static void runProcess(final String cmd) throws IOException, InterruptedException {
    ProcessBuilder processBuilder = new ProcessBuilder(cmd.split("\\s+"));
    Process process = processBuilder.start();
    if (!process.waitFor(30, TimeUnit.SECONDS)) {
        process.destroy();
        throw new RuntimeException("Timeout while executing: " + cmd);
    }
  }

  private static void createCertificateFile(final String fileName, final String fileValue) throws IOException {
    try (final PrintWriter out = new PrintWriter(fileName, StandardCharsets.UTF_8)) {
      out.print(fileValue);
      LOGGER.info("Certificate File '{}' created", fileName);
    }
  }

  private static void setKeyStore(final String clientKeyPassword) {
    System.setProperty("javax.net.ssl.keyStoreType", KEY_STORE_TYPE);
    System.setProperty("javax.net.ssl.keyStore", CLIENT_KEY_STORE);
    System.setProperty("javax.net.ssl.keyStorePassword", clientKeyPassword);
  }

  private static void setTrustStore(final String trustStorePassword) {
    System.setProperty("javax.net.ssl.trustStoreType", TRUST_TYPE);
    System.setProperty("javax.net.ssl.trustStore", TRUST_STORE);
    System.setProperty("javax.net.ssl.trustStorePassword", trustStorePassword);
  }

  public static boolean isValidSslMode(final String sslMode) {
    return sslMode != null && !sslMode.isEmpty() && !sslMode.equals(DISABLED.toString());
  }

  private static SslMode getSslVerifyMode(final String sslMode) {
    return SslMode.bySpec(sslMode).orElseThrow(() -> new IllegalArgumentException("unexpected ssl mode"));
  }

  public enum SslMode {

    DISABLED("disable"),
    CCV("CCV"),
    AWS("AWS");

    public final List<String> spec;

    SslMode(final String... spec) {
      this.spec = Arrays.asList(spec);
    }

    public static Optional<SslMode> bySpec(final String spec) {
      return Arrays.stream(SslMode.values())
          .filter(sslMode -> sslMode.spec.contains(spec))
          .findFirst();
    }

  }

}
