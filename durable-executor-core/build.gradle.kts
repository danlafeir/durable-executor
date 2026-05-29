plugins {
    `java-library`
}

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

repositories {
    mavenCentral()
}

dependencies {
    api("com.fasterxml.jackson.core:jackson-databind:2.18.4")
    api("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.18.4")
    api("org.slf4j:slf4j-api:2.0.17")
}
