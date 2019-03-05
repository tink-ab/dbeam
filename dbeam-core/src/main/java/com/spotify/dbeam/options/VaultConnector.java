/*-
 * -\-\-
 * DBeam Core
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.dbeam.options;

import com.bettercloud.vault.SslConfig;
import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultConfig;
import com.bettercloud.vault.VaultException;
import com.bettercloud.vault.json.Json;
import com.bettercloud.vault.response.AuthResponse;
import com.bettercloud.vault.rest.Rest;
import com.bettercloud.vault.rest.RestResponse;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class VaultConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(VaultConnector.class);
  private static final Path TOKEN_PATH =
          Paths.get("/var/run/secrets/kubernetes.io/serviceaccount/token");

  private VaultConfig vaultConfig;
  private AuthResponse authResponse;

  public static VaultConnector getInstance(String address, String certFile, String role) {
    return new VaultConnector(address, certFile, role);
  }

  private VaultConnector(String address, String certFile, String role) {
    try {
      vaultConfig = getVaultConfig(address, certFile);
      authResponse = loginByKubernetes(role, getServiceAuthToken());
      vaultConfig = getVaultConfig(address, certFile, authResponse.getAuthClientToken());
    } catch (VaultException | IOException e) {
      LOGGER.error("Could not authenticate to vault.", e);
    }
  }

  private static String getServiceAuthToken() throws IOException {
    String token;
    try (BufferedReader reader = Files.newBufferedReader(TOKEN_PATH, Charset.forName("UTF-8"))) {
      token = reader.readLine();
    } catch (IOException e) {
      LOGGER.error("Issue reading Kubernetes service account token. " + e.getMessage());
      throw e;
    }
    return token;
  }

  private VaultConfig getVaultConfig(String address, String certFile) throws VaultException {
    return new VaultConfig()
        .address(address)
        .engineVersion(1)
        .sslConfig(new SslConfig().pemFile(new File(certFile)))
        .build();
  }

  private VaultConfig getVaultConfig(String address, String certFile, String token)
      throws VaultException {
    return getVaultConfig(address, certFile)
        .token(token)
        .build();
  }

  private Vault getVault() {
    return new Vault(vaultConfig).withRetries(3, 1000);
  }

  public Map<String, String> getValue(String path) throws VaultException {
    return getVault().logical().read(path).getData();
  }

  /**
   * Basic login operation to authenticate to an kubernetes backend. Example usage:
   *
   * <blockquote>
   *
   * <pre>{@code
   * final AuthResponse response =
   *     vault.auth().loginByKubernetes("dev", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...");
   *
   * final String token = response.getAuthClientToken();
   * }</pre>
   *
   * </blockquote>
   * <p>
   * Code is adaption from the loginByGCP in vault-java-driver.
   * </p>
   * @param role The kubernetes role used for authentication
   * @param jwt The JWT token for the role
   * @return The auth token, with additional response metadata
   * @throws VaultException If any error occurs, or unexpected response received from Vault
   */
  private AuthResponse loginByKubernetes(final String role, final String jwt)
      throws VaultException {
    int retryCount = 0;

    while (true) {
      try {
        // HTTP request to Vault
        final String requestJson = Json.object().add("role", role).add("jwt", jwt).toString();
        final RestResponse restResponse =
            new Rest()
                .url(vaultConfig.getAddress() + "/v1/auth/kubernetes/login")
                .body(requestJson.getBytes(StandardCharsets.UTF_8))
                .connectTimeoutSeconds(vaultConfig.getOpenTimeout())
                .readTimeoutSeconds(vaultConfig.getReadTimeout())
                .sslVerification(vaultConfig.getSslConfig().isVerify())
                .sslContext(vaultConfig.getSslConfig().getSslContext())
                .post();

        // Validate restResponse
        if (restResponse.getStatus() != 200) {
          throw new VaultException(
              "Vault responded with HTTP status code: " + restResponse.getStatus(),
              restResponse.getStatus());
        }
        final String mimeType =
            restResponse.getMimeType() == null ? "null" : restResponse.getMimeType();
        if (!mimeType.equals("application/json")) {
          throw new VaultException(
              "Vault responded with MIME type: " + mimeType, restResponse.getStatus());
        }
        return new AuthResponse(restResponse, retryCount);
      } catch (Exception e) {
        // If there are retries to perform, then pause for the configured interval and then execute
        // the loop again...
        if (retryCount < vaultConfig.getMaxRetries()) {
          retryCount++;
          try {
            final int retryIntervalMilliseconds = vaultConfig.getRetryIntervalMilliseconds();
            Thread.sleep(retryIntervalMilliseconds);
          } catch (InterruptedException e1) {
            e1.printStackTrace();
          }
        } else if (e instanceof VaultException) {
          // ... otherwise, give up.
          throw (VaultException) e;
        } else {
          throw new VaultException(e);
        }
      }
    }
  }
}
