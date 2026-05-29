plugins {
    id("io.github.gradle-nexus.publish-plugin") version "2.0.0"
}

allprojects {
    group = "com.github.danlafeir"
    version = "0.1.0"
}

nexusPublishing {
    repositories {
        sonatype {
            nexusUrl.set(uri("https://ossrh-staging-api.central.sonatype.com/service/local/"))
            snapshotRepositoryUrl.set(uri("https://central.sonatype.com/repository/maven-snapshots/"))
            username.set(providers.gradleProperty("sonatypeUsername")
                .orElse(providers.environmentVariable("SONATYPE_USERNAME")))
            password.set(providers.gradleProperty("sonatypePassword")
                .orElse(providers.environmentVariable("SONATYPE_PASSWORD")))
        }
    }
}
