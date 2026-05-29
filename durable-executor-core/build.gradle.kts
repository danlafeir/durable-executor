plugins {
    `java-library`
    `maven-publish`
    signing
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
    withSourcesJar()
    withJavadocJar()
}

repositories {
    mavenCentral()
}

dependencies {
    api("com.fasterxml.jackson.core:jackson-databind:2.16.2")
    api("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.16.2")
    api("org.slf4j:slf4j-api:2.0.12")
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            pom {
                name.set("durable-executor-core")
                description.set("Framework-agnostic core for durable execution: @Durable annotation, execution model, and file-backed store.")
                url.set("https://github.com/danlafeir/durable-executor")
                licenses {
                    license {
                        name.set("Apache License, Version 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0")
                    }
                }
                developers {
                    developer {
                        id.set("danlafeir")
                        email.set("danlafeir@gmail.com")
                    }
                }
                scm {
                    connection.set("scm:git:git://github.com/danlafeir/durable-executor.git")
                    developerConnection.set("scm:git:ssh://github.com/danlafeir/durable-executor.git")
                    url.set("https://github.com/danlafeir/durable-executor")
                }
            }
        }
    }
    repositories {
        mavenLocal()
    }
}

tasks.javadoc {
    (options as StandardJavadocDocletOptions).addStringOption("Xdoclint:none", "-quiet")
}

signing {
    val signingKey = findProperty("signingKey") as String? ?: System.getenv("SIGNING_KEY")
    val signingPassword = findProperty("signingPassword") as String? ?: System.getenv("SIGNING_PASSWORD")
    if (signingKey != null && signingPassword != null) {
        useInMemoryPgpKeys(signingKey, signingPassword)
        sign(publishing.publications["mavenJava"])
    }
}
