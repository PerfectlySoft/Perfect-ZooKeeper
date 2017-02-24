# Perfect-ZooKeeper [English](README.md)

<p align="center">
    <a href="http://perfect.org/get-involved.html" target="_blank">
        <img src="http://perfect.org/assets/github/perfect_github_2_0_0.jpg" alt="Get Involed with Perfect!" width="854" />
    </a>
</p>

<p align="center">
    <a href="https://github.com/PerfectlySoft/Perfect" target="_blank">
        <img src="http://www.perfect.org/github/Perfect_GH_button_1_Star.jpg" alt="Star Perfect On Github" />
    </a>  
    <a href="https://gitter.im/PerfectlySoft/Perfect" target="_blank">
        <img src="http://www.perfect.org/github/Perfect_GH_button_2_Git.jpg" alt="Chat on Gitter" />
    </a>  
    <a href="https://twitter.com/perfectlysoft" target="_blank">
        <img src="http://www.perfect.org/github/Perfect_GH_button_3_twit.jpg" alt="Follow Perfect on Twitter" />
    </a>  
    <a href="http://perfect.ly" target="_blank">
        <img src="http://www.perfect.org/github/Perfect_GH_button_4_slack.jpg" alt="Join the Perfect Slack" />
    </a>
</p>

<p align="center">
    <a href="https://developer.apple.com/swift/" target="_blank">
        <img src="https://img.shields.io/badge/Swift-3.0-orange.svg?style=flat" alt="Swift 3.0">
    </a>
    <a href="https://developer.apple.com/swift/" target="_blank">
        <img src="https://img.shields.io/badge/Platforms-OS%20X%20%7C%20Linux%20-lightgray.svg?style=flat" alt="Platforms OS X | Linux">
    </a>
    <a href="http://perfect.org/licensing.html" target="_blank">
        <img src="https://img.shields.io/badge/License-Apache-lightgrey.svg?style=flat" alt="License Apache">
    </a>
    <a href="http://twitter.com/PerfectlySoft" target="_blank">
        <img src="https://img.shields.io/badge/Twitter-@PerfectlySoft-blue.svg?style=flat" alt="PerfectlySoft Twitter">
    </a>
    <a href="https://gitter.im/PerfectlySoft/Perfect?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge" target="_blank">
        <img src="https://img.shields.io/badge/Gitter-Join%20Chat-brightgreen.svg" alt="Join the chat at https://gitter.im/PerfectlySoft/Perfect">
    </a>
    <a href="http://perfect.ly" target="_blank">
        <img src="http://perfect.ly/badge.svg" alt="Slack Status">
    </a>
</p>

本工程实现了一个对 ZooKeeper 函数库的简单封装。

本项目是[Perfect](https://github.com/PerfectlySoft/Perfect) 项目的组成部分。

## 编译选项

本项目只能在 ⚠️ Ubuntu 16.04上编译⚠️ 。苹果操作系统暂时不支持本函数库

## 快速上手

### SPM 软件包管理器

请修改您的工程中的 Package.swift 文件并增加依存关系：

``` swift
.Package(url: "https://github.com/PerfectlySoft/Perfect-ZooKeeper.git", majorVersion: 1)
```

### 导入函数库

请在您程序的开头部分增加函数库导入：

``` swift
import PerfectZooKeeper
```

### 程序调试开关

函数库提供程序开关选项，您随时可以调用静态命令`debug()`提高或者降低调试等级：

``` swift
// this will set debug level to the whole application
ZooKeeper.debug()
```

其中，`debug(_ level: LogLevel = .DEBUG)` 调试等级的参数意义在于：

- level: LogLevel 类型，即调试等级，可以是.ERROR（只在出错时输出信息）， .WARN（只要有警告即输出），.INFO（输出任何一般信息）或者 .DEBUG（默认设置，输出所有信息）

### 日志记录

您还可以通过调用静态函数`log()`来控制日志输出的目标，比如：

``` swift

// 以下调用会将调试信息输出到标准错误终端
ZooKeeper.log()

```

静态函数 `log(_ to: UnsafeMutablePointer<FILE> = stderr)` 的唯一参数是 `to`，即标准C语言的文件指针，默认为标准错误输出。

### 初始化 ZooKeeper 对象

在执行任何实际操作之前，请首先初始化一个 `ZooKeeper` 对象。

``` swift
let z = ZooKeeper()
```

或者还可以在初始化时设置连接超时限制，单位是毫秒：

``` swift
// 下列调用意味着如果8秒钟没有收到服务器响应则意味着连接中断
let z = ZooKeeper(8192)
```

### 连接到 ZooKeeper 集群

使用函数 `ZooKeeper.connect()` 可以连接到指定服务器集群。下列例子展示了如何连接到一个服务器，并且一旦连接成功则向客户端进行回调处理：

``` swift
try z.connect("servername:2181") { connect in
  switch(connect) {
  case .CONNECTED:
    // 连接成功
  case .EXPIRED:
    // 连接超时
  default:
    // 无法连接
  }
}
```

⚠️ 注意 ⚠️ 您还可以同时连接多个服务器构成的集群， 具体方法是将连接字符串替换为类似 `"server1:2181,server2:2181,server3:2181"` 的表达式，但是这可能受到您具体的集群版本的限制，详见 [ZooKeeper 编程指南（英文版）](https://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html)


### 判断节点是否存在

连接完成后，可以调用 `exists()` 查看目标节点路径是否存在：

``` swift
let a = try z.exists("/path/to")
print(a)
```

正常情况下，该函数返回一个`Stat()` 结构，如下所示：

``` swift
// 如果调用 print(try z.exists("/path/to")) 可能会输出下列结果：
Stat(czxid: 0, mzxid: 0, ctime: 0, mtime: 0, version: 0, cversion: -1, aversion: 0, ephemeralOwner: 0, dataLength: 0, numChildren: 1, pzxid: 0)
```

### 列出当前节点的直接子节点

调用函数 `children()` 将列出目标节点的所有直接子节点，并保存为一个字符串数组并返回：

``` swift
let kids = try z.children("/path/to")
// 如果调用成功，则 /path/to 路径下的所有子节点将会保存为一个字符串数组。
// 比如，如果存在 /path/to/a 和 /path/to/b，那么结果可能是 ["a", "b"]
print(kids)
```

### 将数据保存到目标节点

作为一个典型的树状字典，每个节点都可以作为一个条目来保存一个对应的字符串数据，通常不允许超过10k字节。您可以采用同步或者异步方法把数据写入节点，通常是您的服务器集中配置数据。

#### 同步保存数据

调用同步调用 `save()` 方法可以用来保存数据。如果数据保存成功，该调用将返回一个 `Stat()` 方法表示节点当前状态。

``` swift
let stat = try z.save("/path/to/key", data: "该条目对应的配置数据")
print(stat)
```

同步保存函数 `func save(_ path: String, data: String, version: Int = -1) throws -> Stat` 的参数说明如下：
- path: String，目标节点的绝对路径
- data: String, 计划保存的数据（字符串）
- version: Int, 版本信息。如果设置为 -1 （默认值）则忽略版本信息。

#### 异步保存数据

异步保存函数`save()`与同步函数保持同样的参数，只不过最后一个参数是一个状态信息回调 `StatusCallback`，并且没有返回值：

``` swift
try z.save("/path/to/key", data: "该条目对应的配置数据") { err, stat in
  guard err == .ZOK else {
    // 出错了！
  }
  guard let st = stat else {
    // 异步保存无法获取节点状态
  }
  // 打印保存之后的节点状态
  print(st)
}
```

### 从节点中读取数据

与 `save()`函数类似，ZooKeeper 的读取函数 `load()`也存在同步读取和异步读取两个版本：

#### 同步读取节点数据

调用 `load("/path/to")` 可获取节点键值对应的数据，结果是返回一个双元组 (Value: String, Stat)，第一个元素是取得的字符串值，第二个元素是节点状态

``` swift
let (value, stat) = try z.load("/path/to")
```

#### 异步读取节点数据

异步调用方法将比同步方法多一个回调参数，回调是将返回给用户(error: Exception, value: String, Stat)，即出错信息、字符串值和节点状态：

``` swift
try z.load(path) { err, value, stat in
  guard err == .ZOK else {
    // 出错了
  }//end guard
  guard let st = stat else {
    // 无法获取节点信息
  }//end guard
  print(st)
  // 这里是实际读取的节点数据
  print(value)
}//end load
```

### 创建节点

函数 `func make(_ path: String, value: String = "", type: NodeType = .PERSISTENT, acl: ACLTemplate = .OPEN) throws -> String` 可以用于创建不同类型的节点，同时写入节点数据，并设置一个简单的访问控制列表。详细参数参考如下：

- path: String, 节点的绝对路径
- value: String, 计划保存的数据
- type: NodeType, 节点类型，允许值为.PERSISTENT（永久节点）、.EPHEMERAL（临时节点）、.SEQUENTIAL（顺序编号节点）或者.LEADERSHIP（可选举节点——带有顺序编号的临时节点）。默认情况下为永久节点。
- acl: ACLTemplate, 基本的访问控制列表模板，可以是.OPEN（开放性节点）、.READ（只读节点）或.CREATOR（私有节点）。默认为开放性节点，即任何人都可以读取。

⚠️ 注意 ⚠️ 只有在创建顺序编号节点或者可选举节点时，该函数会返回一个字符串类型的顺序编号（通常为10个字节的编号），其他类型的节点返回值为空，内容可忽略。

#### 创建永久节点

以下代码示范了如何创建一个永久节点并保存一些配置数据：

``` swift
let _ = try z.make("/path/to/key", value: "该条目对应的配置数据")
```

#### 创建临时节点

创建临时节点意味着在会话结束后节点会被自动删除（一般是会话断开后数秒钟）。参考以下示范：

``` swift
let _ = try z.make("/path/to/tempKey", value: "临时条目下的数据", type: .EPHEMERAL)
```

#### 创建顺序编号节点

顺序编号节点的意思是当创建一个路径为 `/path/to/key` 的节点时，结果会生成一个类似于`/path/to/key0123456789`的结果，也就是一个10位的序列号会增加到目标节点路径上，该序列号是自动递增的，不会重复：

``` swift
let serialNum = try z.make("/path/to/myApplication", type: .SEQUENTIAL)
print(serialNum)
// 如果成功，序列号将会是如`0000000123`的字符串，而节点全路径会变成`/path/to/myApplication0000000123`
```

⚠️ 注意 ⚠️ 顺序编号节点是永久节点，这意味着如果不调用 `remove()`则节点不会被删除。

#### 创建可选举节点

可选举节点的设置目的在于如果有多个服务器实例准备构成一个集群，则所有实例可以通过设置该方式自我选择出一个主节点。可选举节点既带有自动递增的顺序编号，又是一个临时节点，意味着所有节点都可以通过比较编号的方法，判断自己是否已经赢得集群的领导权——按照先到先得原则，当前有效的最小序列号意味着对应节点被推选为主节点。

``` swift
let serialNum = try z.make("/path/to/myApplication", type: .LEADERSHIP)
print(serialNum)
// 如果成功，序列号将会是如`0000000123`的字符串，而节点全路径会变成`/path/to/myApplication0000000123`
// 之后可以调用`children()`来检查所有参与竞选的实例序号
// 如果当前实例的编号为有效编号中的最小值，则将成为集群主节点，负责接管集群的配置（比如配置写入）
// 否则所有节点都要去等待直到选举成功（意味着前一个主节点崩溃了）。
```

### 删除节点

调用方法 `func remove(_ path: String, version: Int32 = -1)` 可以删除一个存在的节点

``` swift
// 以下示范将忽略版本信息，直接删除节点
try z.remove("/path/to/uselessNode")
```

### 监控节点数据

Perfect-ZooKeeper 提供了一个非常有用的 `watch()` 函数用于监控特定节点是否有内容性或结构性变化。
完整的`watch()`函数是`func watch(_ path: String, eventType: EventType = .BOTH, renew: Bool = true, onChange: @escaping WatchCallback)` 参数参考如下：

- path: String, 目标待监控节点的绝对路径
- eventType: 待监控的事件类型，可以是.DATA（只监控当前节点的数据内容变化）或者 .CHILDREN（只检查直接子节点变化），或者 .BOTH（监控所有变化）
- renew: 布尔变量，如果设置为真，则持续监控；否则只设置一次性监控，即事件一旦发生则自动解除监控。默认为持续监控。
- onChange: WatchCallback, 处理监控事件的回调函数

比如：

``` swift
try z.watch("/path/to/myCheese") { event in
  switch(event) {
  case CONNECTED:
    // 这才连接到服务器？不太可能吧？？
  case DISCONNECTED:
    // 连接中断
  case EXPIRED:
    // 连接超时
  case CREATED:
    // 这个应该不会发生：在监控之后创建？？？
  case DELETED:
    // 谁动了我的奶酪：节点被删除了
  case DATA_CHANGED:
    // 谁动了我的奶酪：节点数据被改变
  case CHILD_CHANGED:
    // 谁动了我的奶酪：节点之下的子节点有变化
  default:
    // 出现异常
  }
}//end watch
```

## 问题报告、内容贡献和客户支持

我们目前正在过渡到使用JIRA来处理所有源代码资源合并申请、修复漏洞以及其它有关问题。因此，GitHub 的“issues”问题报告功能已经被禁用了。

如果您发现了问题，或者希望为改进本文提供意见和建议，[请在这里指出](http://jira.perfect.org:8080/servicedesk/customer/portal/1).

在您开始之前，请参阅[目前待解决的问题清单](http://jira.perfect.org:8080/projects/ISS/issues).

## 更多信息
关于本项目更多内容，请参考[perfect.org](http://perfect.org).
