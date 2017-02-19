import PackageDescription

let package = Package(
    name: "PerfectZooKeeper",
    dependencies: [
      .Package(url: "https://github.com/PerfectlySoft/Perfect-LinuxBridge.git", majorVersion: 2),
      .Package(url: "https://github.com/PerfectlySoft/Perfect-libZooKeeper.git", majorVersion: 1)
    ]
)
