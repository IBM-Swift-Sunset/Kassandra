# Kassandra
A Swift Cassandra driver

[![Build Status](https://travis-ci.com/IBM-Swift/Kassandra.svg?token=NtWCrCZmgqfHWpaxg7qx&branch=data-types)](https://travis-ci.com/IBM-Swift/Kassandra)
![](https://img.shields.io/badge/Swift-3.0-orange.svg?style=flat)
![](https://img.shields.io/badge/Snapshot-8/18-blue.svg?style=flat)

**Cassandra** adapter for **Swift 3.0**.

> Requires swift-DEVELOPMENT-SNAPSHOT-2016-08-18-a

## Installation ##

NOTE: Linux users have to install the master branch of libdispatch. Instructions can be found [here](https://github.com/apple/swift-corelibs-libdispatch/blob/master/INSTALL.md).

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

3. Setup XCode to build library (Optional)

    Navigate to your XCode project build settings then in both the `SSLService` and `Kassandra` targets add:

    - `/usr/local/opt/openssl/include` to its Header Search Paths
    - `/usr/local/opt/openssl/lib` to its Library Search Paths

    Note: If interested in the test cases, `KassandraTestCases` target will also need `/usr/local/opt/openssl/lib` added to its Library Search Paths

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
	$ swift build -Xcc -fblocks -Xcc -I/usr/local/opt/openssl/include -Xlinker -L/usr/local/opt/openssl/lib
	```

## Getting an Cassandra Docker Image from DockerHub

1. Download [Docker Toolbox](https://www.docker.com/products/docker-toolbox)

2. Go pull [Cassandra from DockerHub](https://hub.docker.com/r/library/cassandra/) with:

  `docker pull cassandra`

3. Run the Cassandra container with:

  `docker run --name some-cassandra -d cassandra:tag`


## License

This library is licensed under Apache 2.0. Full license text is available in [LICENSE](LICENSE).
