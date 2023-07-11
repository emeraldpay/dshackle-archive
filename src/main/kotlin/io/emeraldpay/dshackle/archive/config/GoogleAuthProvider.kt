package io.emeraldpay.dshackle.archive.config

import com.google.api.gax.core.CredentialsProvider
import com.google.auth.Credentials
import com.google.auth.oauth2.GoogleCredentials
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.auth.oauth2.UserCredentials
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Repository
import java.io.FileInputStream
import java.io.FileNotFoundException
import javax.annotation.PostConstruct

@Repository
@Profile("with-gcp")
class GoogleAuthProvider(
    @Autowired private val runConfig: RunConfig,
) : CredentialsProvider {

    companion object {
        private val log = LoggerFactory.getLogger(GoogleAuthProvider::class.java)
    }

    private var credentialsInternal: GoogleCredentials? = null

    @PostConstruct
    fun setup() {
        val jsonPath = runConfig.auth.gcp?.credentials
        this.credentialsInternal = (
            if (StringUtils.isNotEmpty(jsonPath)) {
                log.info("Use GCP Credentials from: $jsonPath")
                try {
                    GoogleCredentials.fromStream(FileInputStream(jsonPath!!))
                } catch (t: FileNotFoundException) {
                    log.error("GCP Auth file doesn't exist: $jsonPath")
                    null
                }
            } else {
                log.warn("Using system default GCP Credentials")
                GoogleCredentials.getApplicationDefault()
            }
            )?.createScoped(listOf("https://www.googleapis.com/auth/cloud-platform"))
    }

    val username: String
        get() {
            return credentials?.let { credentials ->
                when (credentials) {
                    is ServiceAccountCredentials -> credentials.clientEmail
                    is UserCredentials -> credentials.clientId
                    else -> "UNKNOWN"
                }
            } ?: "NONE"
        }

    override fun getCredentials(): Credentials? {
        return credentialsInternal
    }

    class Delegated(runConfig: RunConfig, private val delegate: CredentialsProvider) : GoogleAuthProvider(runConfig) {

        override fun getCredentials(): Credentials? {
            return delegate.credentials
        }
    }
}
