plugins {
    `java-library`
    `maven-publish`
    signing
}

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
    withSourcesJar()
    withJavadocJar()
}

repositories {
    mavenCentral()
}

val springVersion = "6.2.5"
val springBootVersion = "3.5.3"

dependencies {
    api(project(":durable-executor-core"))
    api("org.springframework:spring-context:$springVersion")
    api("org.springframework:spring-aop:$springVersion")
    api("org.springframework.boot:spring-boot-autoconfigure:$springBootVersion")
    api("org.aspectj:aspectjweaver:1.9.24")

    compileOnly("org.springframework.boot:spring-boot-configuration-processor:$springBootVersion")
    annotationProcessor("org.springframework.boot:spring-boot-configuration-processor:$springBootVersion")

    testImplementation("org.springframework.boot:spring-boot-starter-test:$springBootVersion")
    testImplementation("org.springframework.boot:spring-boot-starter:$springBootVersion")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            pom {
                name.set("durable-executor-spring")
                description.set("Spring Boot integration for durable execution: AOP aspect, autoconfiguration, and startup recovery.")
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

tasks.test {
    useJUnitPlatform()
}
