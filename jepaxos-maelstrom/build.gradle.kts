plugins {
    application
    id("com.github.johnrengelman.shadow") version "7.1.2"
}

application {
    mainClass.set("edu.yuvyurchenko.jepaxos.maelstrom.NodeRunner")
}

group = "edu.yuvyurchenko.jepaxos"
version = "1.0"

repositories {
    mavenCentral()
    maven(url = "https://clojars.org/repo/")
}

dependencies {
    implementation(project(":jepaxos-epaxos"))
    implementation("com.fasterxml.jackson.core:jackson-databind:2.13.2.2")
    implementation("ch.qos.logback:logback-classic:1.2.11")
    implementation("ch.qos.logback:logback-core:1.2.11")
    testImplementation("org.testng:testng:7.6.0")
}

tasks {
    test {
        useTestNG()    
    }
}