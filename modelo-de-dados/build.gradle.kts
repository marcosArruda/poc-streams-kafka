import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import org.gradle.jvm.tasks.Jar

plugins {
    `maven-publish`
    kotlin("jvm") version "1.3.61"

}

group = "br.com.marcosfariaarruda.empiricus"
version = "0.0.1-SNAPSHOT"
java.sourceCompatibility = JavaVersion.VERSION_11

repositories {
    mavenLocal()
	mavenCentral()
}

buildscript {
    repositories {
        mavenLocal()
        mavenCentral()
        jcenter()
    }
}

extra["springCloudVersion"] = "Hoxton.SR1"

dependencies {
    implementation(kotlin("stdlib"))
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.10.+")
    implementation("org.apache.kafka:kafka-clients:2.3.1")
    implementation("org.apache.kafka:kafka-streams:2.3.1")
    testImplementation("junit:junit:4.12")
}

tasks.withType<KotlinCompile> {
	kotlinOptions {
		freeCompilerArgs = listOf("-Xjsr305=strict")
		jvmTarget = "1.8"
	}
}

publishing {
    publications {
        create<MavenPublication>("default") {
            from(components["kotlin"])
        }
    }
    repositories {
        maven {
            url = uri("$buildDir/repository")
        }
    }
}
