import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
	id("maven-publish")
	id("org.springframework.boot") version "2.2.2.RELEASE"
	id("io.spring.dependency-management") version "1.0.8.RELEASE"
	kotlin("jvm") version "1.3.61"
	kotlin("plugin.spring") version "1.3.61"
}

group = "br.com.marcosfariaarruda.empiricus"
version = "0.0.1-SNAPSHOT"
java.sourceCompatibility = JavaVersion.VERSION_11

repositories {
	mavenLocal()
	mavenCentral()
}

extra["springCloudVersion"] = "Hoxton.SR1"
//extra["protobuf.version"] = "3.10.0"

dependencies {
	implementation("org.springframework.boot:spring-boot-starter-web")
	implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
	implementation("org.apache.kafka:kafka-streams")
	implementation("org.jetbrains.kotlin:kotlin-reflect")
	implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
	implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.10.+")
	//implementation("org.springframework.cloud:spring-cloud-starter-circuitbreaker-resilience4j")
	//implementation("org.springframework.cloud:spring-cloud-starter-netflix-turbine-stream")
	implementation("org.springframework.cloud:spring-cloud-stream")
	implementation("org.springframework.cloud:spring-cloud-stream-binder-kafka")
	implementation("com.github.daniel-shuy:kafka-protobuf-serde:2.2.0")
	implementation("com.google.code.gson:gson:2.8.6")
	implementation("br.com.marcosfariaarruda.empiricus:modelo-de-dados:0.0.1-SNAPSHOT")
	runtimeOnly("com.h2database:h2")
	testImplementation("org.springframework.boot:spring-boot-starter-test") {
		exclude(group = "org.junit.vintage", module = "junit-vintage-engine")
	}
	testImplementation("org.springframework.cloud:spring-cloud-stream-test-support")
}

dependencyManagement {
	imports {
		mavenBom("org.springframework.cloud:spring-cloud-dependencies:${property("springCloudVersion")}")
	}
}

tasks.withType<Test> {
	useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
	kotlinOptions {
		freeCompilerArgs = listOf("-Xjsr305=strict")
		jvmTarget = "1.8"
	}
}
