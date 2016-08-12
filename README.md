# Kassandra
A Swift Cassandra driver

[![Build Status](https://travis-ci.com/IBM-Swift/Kassandra.svg?token=NtWCrCZmgqfHWpaxg7qx&branch=master)](https://travis-ci.com/IBM-Swift/Kassandra) [![Swift 3 8-07](https://img.shields.io/badge/Swift%203-8/07 SNAPSHOT-blue.svg)](https://swift.org/download/#snapshots)


## Quick start:

1. Download the [Swift DEVELOPMENT 08-07 snapshot](https://swift.org/download/#snapshots)

4. Add Kassandra as a dependency in your Package.swift file:

  ```swift
  .Package(url: "https://github.com/IBM-Swift/Kassandra", majorVersion: 0, minor: 2)
  ```

5. Compile your application with: 

  - macOS (CLI): `swift build -Xcc -I/usr/local/opt/openssl/include -Xlinker -L/usr/local/opt/openssl/lib` 
  - macOS (XCode): `swift package generate-xcodeproj`
  - Linux: `swift build -Xcc -fblocks`


## Getting an Cassandra Docker Image from DockerHub

1. Download [Docker Toolbox](https://www.docker.com/products/docker-toolbox)

2. Go pull [Cassandra from DockerHub](https://hub.docker.com/r/library/cassandra/) with:

  `docker pull cassandra`

3. Run the Cassandra container with:

  `docker run --name some-cassandra -d cassandra:tag`
  
## License

This library is licensed under Apache 2.0. Full license text is available in [LICENSE](LICENSE).
