# Kassandra
A Swift Cassandra driver

[![Build Status](https://travis-ci.com/IBM-Swift/Kassandra.svg?token=NtWCrCZmgqfHWpaxg7qx&branch=travis)](https://travis-ci.com/IBM-Swift/Kassandra) [![Swift 3 8-04](https://img.shields.io/badge/Swift%203-8/04 SNAPSHOT-blue.svg)](https://swift.org/download/#snapshots)


## Quick start:

1. Download the [Swift DEVELOPMENT 08-04 snapshot](https://swift.org/download/#snapshots)

2. Download Cassandra, Python, and pip
  You can use `brew install cassandra python` and `easy_install pip`

3. Install Cqlsh
  You can use `pip install cqlsh`

4. Clone the Kassandra repository
  `git clone https://github.com/IBM-Swift/Kassandra`

5. Compile the library with `swift build -Xcc -I/usr/local/opt/openssl/include -Xlinker -L/usr/local/opt/opensll/lib` or create an XCode project with `swift package generate-xcodeproj`

6. Run the test cases with `swift test -Xcc -I/usr/local/opt/openssl/include -Xlinker -L/usr/local/opt/opensll/lib` or directly from XCode

## Getting an Docker Image from DockerHub

1. Download [Docker Toolbox](https://www.docker.com/products/docker-toolbox)

2. Go pull [Cassandra from DockerHub](https://hub.docker.com/r/library/cassandra/) or you can you this command `docker pull cassandra`


## License

This library is licensed under Apache 2.0. Full license text is available in [LICENSE](LICENSE).
