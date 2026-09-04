plugins {
    kotlin("jvm") version "2.0.0"
    kotlin("plugin.serialization") version "2.0.0"
    id("maven-publish")
    id("org.jetbrains.kotlinx.binary-compatibility-validator") version "0.17.0"
    id("org.jlleitschuh.gradle.ktlint") version "12.1.1"
    id("org.cyclonedx.bom") version "1.8.2"
}

group = "io.streamline"
version = "0.4.0"

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
    withSourcesJar()
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.0")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.7.0")
    implementation("io.ktor:ktor-client-core:3.0.0")
    implementation("io.ktor:ktor-client-cio:3.0.0")
    implementation("io.ktor:ktor-client-java:3.0.0")
    implementation("io.ktor:ktor-client-websockets:3.0.0")
    implementation("io.ktor:ktor-client-content-negotiation:3.0.0")
    implementation("io.ktor:ktor-serialization-kotlinx-json:3.0.0")
    testImplementation(kotlin("test"))
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.8.0")
    testImplementation("io.ktor:ktor-client-mock:3.0.0")
    // Real, in-process WebSocket server used to test reconnect/subscription
    // replay/generation-guard behavior against an actual session lifecycle
    // rather than a hand-mocked one (loopback only; no external network).
    testImplementation("io.ktor:ktor-server-core:3.0.0")
    testImplementation("io.ktor:ktor-server-cio:3.0.0")
    testImplementation("io.ktor:ktor-server-websockets:3.0.0")
}

tasks.test {
    useJUnitPlatform()
    exclude("**/IntegrationTest*")
}

kotlin {
    jvmToolchain(17)
}

ktlint {
    // Pre-existing style violations are grandfathered here; new code is held
    // to the full ktlint standard ruleset. Regenerate with
    // `./gradlew ktlintGenerateBaseline` only when intentionally accepting
    // additional legacy debt, not to silence new violations.
    baseline.set(file("config/ktlint/baseline.xml"))
}

val examples by sourceSets.creating {
    kotlin.srcDir("examples")
    compileClasspath += sourceSets.main.get().output
    runtimeClasspath += output + compileClasspath
}

configurations[examples.implementationConfigurationName].extendsFrom(configurations.implementation.get())
configurations[examples.runtimeOnlyConfigurationName].extendsFrom(configurations.runtimeOnly.get())

val integrationTest by tasks.registering(Test::class) {
    description = "Runs live integration tests against a Streamline server."
    group = "verification"
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform()
    include("**/IntegrationTest*")
    systemProperty("streamline.integration", "true")
    failFast = true
    shouldRunAfter(tasks.test)
}

val validateVersionMetadata by tasks.registering {
    description = "Validates the project version and matching changelog entry."
    group = "verification"
    val projectVersion = version.toString()
    inputs.property("projectVersion", projectVersion)
    inputs.file(layout.projectDirectory.file("CHANGELOG.md"))
    doLast {
        require(Regex("""\d+\.\d+\.\d+""").matches(projectVersion)) {
            "Project version '$projectVersion' is not a release SemVer"
        }
        val changelog = layout.projectDirectory.file("CHANGELOG.md").asFile.readText()
        require(changelog.contains("## [$projectVersion]")) {
            "CHANGELOG.md has no release entry for $projectVersion"
        }
    }
}

tasks.register("verifyReleaseTag") {
    description = "Checks that -PreleaseTag exactly matches v<project.version>."
    group = "verification"
    dependsOn(validateVersionMetadata)
    doLast {
        val releaseTag =
            providers.gradleProperty("releaseTag").orNull
                ?: throw GradleException("Missing -PreleaseTag=v${project.version}")
        require(releaseTag == "v${project.version}") {
            "Release tag '$releaseTag' does not match project version v${project.version}"
        }
    }
}

tasks.check {
    dependsOn(validateVersionMetadata)
    dependsOn("apiCheck")
    dependsOn(examples.classesTaskName)
}

tasks.cyclonedxBom {
    setIncludeConfigs(listOf("runtimeClasspath"))
    setProjectType("library")
    setSchemaVersion("1.5")
    setDestination(layout.buildDirectory.dir("reports").get().asFile)
    setOutputName("bom")
    setOutputFormat("all")
    setIncludeBomSerialNumber(true)
    setIncludeLicenseText(false)
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            pom {
                name.set("Streamline Kotlin SDK")
                description.set("Kotlin client SDK for the Streamline streaming platform")
                url.set("https://github.com/streamlinelabs/streamline-kotlin-sdk")
                developers {
                    developer {
                        id.set("streamlinelabs")
                        name.set("Streamline Labs")
                        url.set("https://github.com/streamlinelabs")
                    }
                }
                licenses {
                    license {
                        name.set("Apache-2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0")
                    }
                }
                scm {
                    connection.set("scm:git:https://github.com/streamlinelabs/streamline-kotlin-sdk.git")
                    developerConnection.set("scm:git:ssh://git@github.com/streamlinelabs/streamline-kotlin-sdk.git")
                    url.set("https://github.com/streamlinelabs/streamline-kotlin-sdk")
                    tag.set("v$version")
                }
            }
        }
    }
}
