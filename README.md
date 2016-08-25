# Kassandra
A Swift Cassandra driver

[![Build Status](https://travis-ci.com/IBM-Swift/Kassandra.svg?token=NtWCrCZmgqfHWpaxg7qx&branch=data-types)](https://travis-ci.com/IBM-Swift/Kassandra)
![](https://img.shields.io/badge/Swift-3.0-orange.svg?style=flat)
![](https://img.shields.io/badge/Snapshot-8/23-blue.svg?style=flat)

**Cassandra** adapter for **Swift 3.0**.

> Requires swift-DEVELOPMENT-SNAPSHOT-2016-08-23-a

## Installation ##

1. Install OpenSSL:

    - macOS:
    ```
    $ brew install openssl
    ```
    - Linux:
    ```
    $ sudo apt-get install openssl
    ```

2. Add `Kassandra` to your `Package.swift`

    ```swift
    import PackageDescription

    let package = Package(
    	dependencies: [
		.Package(url: "https://github.com/IBM-Swift/Kassandra.git", majorVersion: 0, minor: 1)
    	]
    )
    ```

3. Create XCode project to build library (Optional)

    ```
    $ swift package generate-xcodeproj \
            -Xswiftc -I/usr/local/opt/openssl/include \
            -Xlinker -L/usr/local/opt/openssl/lib
    ```

4. In Sources/main.swift, import the Kassandra module.

    ``` Swift
    import Kassandra
    ```
5. Build Locally

	- macOS
	```
	$ swift build -Xcc -I/usr/local/opt/openssl/include -Xlinker -L/usr/local/opt/openssl/lib
	```
	- Linux
	```
	$ swift build -Xcc -I/usr/local/opt/openssl/include -Xlinker -L/usr/local/opt/openssl/lib
	```

## Getting an Cassandra Docker Image from DockerHub

1. Download [Docker Toolbox](https://www.docker.com/products/docker-toolbox)

2. Go pull [Cassandra from DockerHub](https://hub.docker.com/r/library/cassandra/) with:

  `docker pull cassandra`

3. Run the Cassandra container with:

  `docker run --name some-cassandra -d cassandra:tag`


## License

This library is licensed under Apache 2.0. Full license text is available in [LICENSE](LICENSE).
