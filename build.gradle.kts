plugins {
    id("java")
    application
    id("com.github.ben-manes.versions") version "0.51.0"
}

group = "com.mongodb"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.reflections:reflections:0.10.2")
    implementation("org.slf4j:slf4j-api:2.0.9")
    implementation("ch.qos.logback:logback-classic:1.5.6")
    implementation("org.mongodb:mongodb-driver-sync:4.11.1")
    annotationProcessor("info.picocli:picocli-codegen:4.7.5")
    implementation("info.picocli:picocli:4.7.5")
    testImplementation(platform("org.junit:junit-bom:5.10.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.add("-Aproject=${project.group}/${project.name}")
}

application {
    mainClass.set("com.mongodb.csp.App")
}

distributions {
    main {
        distributionBaseName.set("csp")
    }
}

tasks.test {
    useJUnitPlatform()
}