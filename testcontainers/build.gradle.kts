plugins {
    kotlin("jvm") version "2.0.0"
    id("maven-publish")
}

group = "io.streamline"
version = "0.2.0"

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

repositories {
    mavenCentral()
}

dependencies {
    api("org.testcontainers:testcontainers:1.20.4")
    implementation("io.ktor:ktor-client-core:3.0.0")
    implementation("io.ktor:ktor-client-cio:3.0.0")
    testImplementation(kotlin("test"))
    testImplementation("org.testcontainers:junit-jupiter:1.20.4")
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(17)
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            pom {
                name.set("Streamline Testcontainers (Kotlin)")
                description.set("Testcontainers module for the Streamline streaming platform — Kotlin edition")
                url.set("https://github.com/streamlinelabs/streamline-kotlin-sdk")
                licenses {
                    license {
                        name.set("Apache-2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0")
                    }
                }
            }
        }
    }
}
