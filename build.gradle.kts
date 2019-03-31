import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    id("org.jetbrains.kotlin.jvm").version("1.3.21")
    application
}

repositories {
    mavenCentral()
    maven("https://jitpack.io")
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

val compileKotlin by tasks.getting(KotlinCompile::class) {
    kotlinOptions.jvmTarget = "1.8"
}

val compileTestKotlin by tasks.getting(KotlinCompile::class) {
    kotlinOptions.jvmTarget = "1.8"
}

application {
    // Define the main class for the application.
    mainClassName = "io.inbot.eskotlindemo.AppKt"
}

dependencies {
    // Use the Kotlin JDK 8 standard library.
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")

    implementation("com.github.jillesvangurp:es-kotlin-wrapper-client:v0.9.11")

    implementation("io.inbot:inbot-utils:1.28")
    implementation("io.inbot:xmltools:2.1")
    implementation("com.jillesvangurp:iterables-support:1.8")
    implementation("org.apache.commons:commons-compress:1.18")

    implementation( "org.slf4j:slf4j-api:1.7.25")
    implementation( "org.slf4j:jcl-over-slf4j:1.7.25")
    implementation ("org.slf4j:log4j-over-slf4j:1.7.25")
    implementation ("org.slf4j:jul-to-slf4j:1.7.25")
    implementation ("org.apache.logging.log4j:log4j-to-slf4j:2.11.2") // es seems to insist on log4j2
    implementation ("ch.qos.logback:logback-classic:1.2.3")
}
