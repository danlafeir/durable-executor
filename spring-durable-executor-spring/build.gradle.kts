plugins {
    `java-library`
    `maven-publish`
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
    withSourcesJar()
}

repositories {
    mavenCentral()
}

val springVersion = "6.1.6"
val springBootVersion = "3.2.4"

dependencies {
    api(project(":spring-durable-executor-core"))
    api("org.springframework:spring-context:$springVersion")
    api("org.springframework:spring-aop:$springVersion")
    api("org.springframework.boot:spring-boot-autoconfigure:$springBootVersion")
    api("org.aspectj:aspectjweaver:1.9.21")

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
        }
    }
    repositories {
        mavenLocal()
    }
}

tasks.test {
    useJUnitPlatform()
}
