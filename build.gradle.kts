plugins {
    id("com.diffplug.spotless") version "8.8.0" apply false
}
subprojects {
    apply(plugin = "com.diffplug.spotless")

    extensions.configure<com.diffplug.gradle.spotless.SpotlessExtension> {
        java {
            googleJavaFormat()
        }
    }
}
