plugins {
    application
}

application {
    mainClass.set("edu.yuvyurchenko.jepaxos.maelstrom.NodeRunner")
}

dependencies {
    implementation(project(":jepaxos-epaxos"))
    implementation("com.fasterxml.jackson.core:jackson-databind:2.13.2.2")
}

group = "edu.yuvyurchenko.jepaxos"
version = "1.0"

repositories {
    mavenCentral()
}