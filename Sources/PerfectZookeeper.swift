//
//  PerfectZooKeeper.swift
//  Perfect-ZooKeeper
//
//  Created by Rockford Wei on 2017-02-22.
//  Copyright © 2017 PerfectlySoft. All rights reserved.
//
//===----------------------------------------------------------------------===//
//
// This source file is part of the Perfect.org open source project
//
// Copyright (c) 2017 - 2018 PerfectlySoft Inc. and the Perfect project authors
// Licensed under Apache License v2.0
//
// See http://perfect.org/licensing.html for license information
//
//===----------------------------------------------------------------------===//
//

#if os(Linux)
import SwiftGlibc
#else
import Darwin
#endif
import czookeeper

/// status call back for zoo_set asyn version
public typealias StatusCallback = (ZooKeeper.Exception, Stat?) -> Void

/// data call back for zoo_get async version
public typealias DataCallback =  (ZooKeeper.Exception, String, Stat?) -> Void

/// watch call backs
public typealias WatchCallback = (ZooKeeper.Event) -> Void

/// ZooKeeper: a light weight swift class wrapper for zoo keeper.
public class ZooKeeper {

  /// according to the zookeeper doc, buffers should be restricted in 10k
  private let ZK_BUFSIZE = 10240

  /// directly copy from zookeeper.h
  public enum Exception: Int32, Error {
    case ZOK = 0, /*!< Everything is OK */

    /** System and server-side errors.
     * This is never thrown by the server, it shouldn't be used other than
     * to indicate a range. Specifically error codes greater than this
     * value, but lesser than {@link #ZAPIERROR}, are system errors. */
    ZSYSTEMERROR = -1,
    ZRUNTIMEINCONSISTENCY = -2, /*!< A runtime inconsistency was found */
    ZDATAINCONSISTENCY = -3, /*!< A data inconsistency was found */
    ZCONNECTIONLOSS = -4, /*!< Connection to the server has been lost */
    ZMARSHALLINGERROR = -5, /*!< Error while marshalling or unmarshalling data */
    ZUNIMPLEMENTED = -6, /*!< Operation is unimplemented */
    ZOPERATIONTIMEOUT = -7, /*!< Operation timeout */
    ZBADARGUMENTS = -8, /*!< Invalid arguments */
    ZINVALIDSTATE = -9, /*!< Invliad zhandle state */

    /** API errors.
     * This is never thrown by the server, it shouldn't be used other than
     * to indicate a range. Specifically error codes greater than this
     * value are API errors (while values less than this indicate a
     * {@link #ZSYSTEMERROR}).
     */
    ZAPIERROR = -100,
    ZNONODE = -101, /*!< Node does not exist */
    ZNOAUTH = -102, /*!< Not authenticated */
    ZBADVERSION = -103, /*!< Version conflict */
    ZNOCHILDRENFOREPHEMERALS = -108, /*!< Ephemeral nodes may not have children */
    ZNODEEXISTS = -110, /*!< The node already exists */
    ZNOTEMPTY = -111, /*!< The node has children */
    ZSESSIONEXPIRED = -112, /*!< The session has been expired by the server */
    ZINVALIDCALLBACK = -113, /*!< Invalid callback specified */
    ZINVALIDACL = -114, /*!< Invalid ACL specified */
    ZAUTHFAILED = -115, /*!< Client authentication failed */
    ZCLOSING = -116, /*!< ZooKeeper is closing */
    ZNOTHING = -117, /*!< (not error) no server responses to process */
    ZSESSIONMOVED = -118, /*!<session moved to another server, so operation is ignored */
    // customize: raised when too many bytes to save.
    OVERFLOW = 1
  }//end enum

  /// connection state to the ZooKeeper server
  public enum ConnectionState {
  case CONNECTED, DISCONNECTED, EXPIRED
  }//end ConnectionState

  public enum NodeType {
  case PERSISTENT, EPHEMERAL, SEQUENTIAL, LEADERSHIP
  }//end enum

  public enum ACLTemplate {
  case OPEN, READ, CREATOR
  }//end enum

  /// zookeeper handle, could be nil if no connection available
  internal var handle: OpaquePointer? = nil

  /// connection time out value, in milliseconds
  internal var _timeout:Int32 = 0

  /// client id
  internal var id = clientid_t()

  /// connection event callback, default is `do nothing`
  public var onConnect: (ConnectionState)->Void = { _ in }

  public enum EventType {
  case DATA, CHILDREN, BOTH
  }//end WatchType

  public enum Event {
  case CONNECTED, DISCONNECTED, EXPIRED, CREATED, DELETED, DATA_CHANGED, CHILD_CHANGED, UNKNOWN, FAULT
  }

  /// change event callback, default is `do nothing`
  public var onChange: (EventType)->Void = { _ in  }

  /// log level
  public enum LogLevel: UInt32 {
  case ERROR = 1, WARN = 2, INFO = 3, DEBUG = 4
  }//end enum

  /// set debug level
  /// - parameters:
  ///   - level: LogLevel, i.e., ERROR, WARN, INFO or DEBUG, by default
  public static func debug(_ level: LogLevel = .DEBUG) {
    zoo_set_debug_level( ZooLogLevel(rawValue: level.rawValue))
  }//end func

  /// set log stream
  /// - parameters:
  ///   - stream: FILE *, the stream to write
  public static func log(_ to: UnsafeMutablePointer<FILE> = stderr) {
    zoo_set_log_stream(to)
  }//end func

  /// constructor
  /// - parameters:
  ///   - timeout: Int32, timeout in connection attampt, in milliseconds
  init(timeout: Int32 = 10000) {
    _timeout = timeout
  }//init

  /// connect to hosts
  /// - parameters:
  ///   - to: hosts, could be mutiple servers such as "server1:2181;server2:2181;server3:2181"
  ///   - completion: connection callback.
  /// - throws:
  ///   Exception
  public func connect(to: String = "localhost:2181", completion: @escaping (ConnectionState) -> Void ) throws {

    // close previous connection
    if handle != nil { zookeeper_close(handle!) }

    // set callback
    onConnect = completion

    // *BUG* zookeeper has some problems with function pointers, so using a inner pointer manager to deal with callbacks
    let ticket = Manager.push(mutable: self)

    // connect to the hosts with default watcher
    guard let _handle = zookeeper_init(to, globalDefaultWatcher, _timeout, &id, ticket, 0) else {
      throw Exception.ZCONNECTIONLOSS
    }//END guard

    // save the handle for future
    handle = _handle
  }//nect connect

  deinit {
    if handle != nil { zookeeper_close(handle!) }
  }//end destruction

  /// a watcher structure for passing context to C api.
  public struct Watcher {
    public var path = ""
    public var eventType = ZooKeeper.EventType.BOTH
    public var renew = true
    public var api: watcher_fn = globalNodeWatcher
    public var onChange: WatchCallback = { _ in }
  }//end ZooWatcher

  /// set a watcher on a specific node
  /// - parameters:
  ///   - path: String, the absolute full path of the node to watch
  ///   - eventType: watch for .DATA or .CHILDREN, or .BOTH
  ///   - renew: watch the event for once or for ever, false for once and true for ever.
  ///   - onChange: WatchCallback, callback once something changed
  /// - throws:
  ///   Exception
  public func watch(_ path: String, eventType: EventType = .BOTH, renew: Bool = true, onChange: @escaping WatchCallback) throws {

    // validate the handle first
    guard let h = handle else {
      throw Exception.ZCONNECTIONLOSS
    }//end guard

    // turn all parameters into a structure
    let watcher = ZooKeeper.Watcher(path: path, eventType: eventType, renew: renew, api:globalNodeWatcher, onChange: onChange)

    // deposit the structure to the pointer mananger and get the pointer
    let context = Manager.push(mutable: watcher)

    let watchForData = eventType == .BOTH || eventType == .DATA
    let watchForKids = eventType == .BOTH || eventType == .CHILDREN
    if watchForData {
      let r = zoo_awget(h, path, globalNodeWatcher, context,  { _, _, _, _, _ in }, nil)
      guard r == Exception.ZOK.rawValue else {
        throw Exception(rawValue: r)!
      }//end r
    }//end if
    if watchForKids {
      let r = zoo_awget_children(h, path, globalNodeWatcher, context, { _, _, _ in }, nil)
      guard r == Exception.ZOK.rawValue else {
        throw Exception(rawValue: r)!
      }//end r
    }//end if
  }//end watch

  /// load data from a node, synchronously
  /// - parameters:
  ///   - path: String, the absolute full path of the node to access
  /// - returns:
  ///   (value: String, stat: Stat), a tuple of data value and its directory status.
  /// - throws:
  ///   Exception
  @discardableResult
  public func load(_ path: String) throws -> (String, Stat) {

    // check the handle first
    guard let h = handle else {
      throw Exception.ZCONNECTIONLOSS
    }//end guard

    // prepare a buffer to receive the data
    var size = Int32(ZK_BUFSIZE)

    // status variable
    var stat = Stat()
    let buf = UnsafeMutablePointer<Int8>.allocate(capacity: ZK_BUFSIZE)
    let r = zoo_get(h, path, 0, buf, &size, &stat)

    // validate the result
    guard r == Exception.ZOK.rawValue else {
      buf.deallocate(capacity: ZK_BUFSIZE)
      throw Exception(rawValue: r)!
    }//end guard

    // save the pointer into string
    let data = String(cString: buf)
    buf.deallocate(capacity: ZK_BUFSIZE)
    return (data, stat)
  }//end read

  /// load data from a node, asynchronously
  /// - parameters:
  ///   - path: String, the absolute full path of the node to access
  ///   - completion: DataCallback, callback once data ready
  /// - throws:
  ///   Exception
  public func load(_ path: String, completion: @escaping DataCallback ) throws {

    // validate the handle first
    guard let h = handle else {
      throw Exception.ZCONNECTIONLOSS
    }//end guard

    // deposit the function pointer to the pointer mananger
    let key = Manager.push(immutable: completion)

    // asynchronously read data from server
    let r = zoo_aget(h, path, 0, { rc, value, len, pStat, data  in

      // get the callback function pointer
      guard let ptr = data else {
        return
      }//end guard
      let callback = Manager.immutables[ptr] as! DataCallback
      let err = Exception(rawValue: rc)!
      var st: Stat? = nil
      if let ptr = pStat {
        st = ptr.pointee
      }//end if

      // if data is ready, read it out
      if len > 0 && value != nil {
        let sz = Int(len)
        let val = UnsafeMutablePointer<CChar>.allocate(capacity: sz + 1)

        // fix the null-terminated c string
        memset(val, 0, sz + 1)
        memcpy(val, value!, sz)
        let str = String(cString: val)
        val.deallocate(capacity: sz)
        print(str)

        // call the completion function
        callback(err, str, st)
      } else {
        callback(err, "", st)
      }//end if
    }, key)

    guard r == Exception.ZOK.rawValue else {
      throw Exception(rawValue: r)!
    }//end r
  }//end load

  /// save data to path, synchronously
  /// - parameters:
  ///   - path: String, the absolute full path of the node to access
  ///   - data: String, the data to save
  ///   - version: version of data, default is -1 which indicates ignoring the version info
  /// - returns:
  ///   stat, the node status after saving
  /// - throws:
  ///   Exception
  @discardableResult
  public func save(_ path: String, data: String, version: Int = -1) throws -> Stat {
    guard let h = handle else {
      throw Exception.ZCONNECTIONLOSS
    }//end guard
    var stat = Stat()
    let r = zoo_set2(h, path, data, Int32(strlen(data)), Int32(version), &stat)
    guard r == Exception.ZOK.rawValue else {
      throw Exception(rawValue: r)!
    }//end guard
    return stat
  }//end save

  /// save data to path, asynchronously
  /// - parameters:
  ///   - path: String, the absolute full path of the node to access
  ///   - data: String, the data to save
  ///   - version: version of data, default is -1 which indicates ignoring the version info
  ///   - completion: StatusCallback once done.
  /// - throws:
  ///   Exception
  public func save(_ path: String, data: String, version: Int = -1, completion: @escaping StatusCallback ) throws {

    // validate the connection
    guard let h = handle else {
      throw Exception.ZCONNECTIONLOSS
    }//end guard

    // save the callback function pointer to pool
    let key = Manager.push(immutable: completion)

    // deposit data into node
    let r = zoo_aset(h, path, data, Int32(strlen(data)), Int32(version), { rc, pStat, data in

      // load the callback function pointer from the pool
      guard let ptr = data else {
        return
      }//end guard
      let callback = Manager.immutables[ptr] as! StatusCallback
      let err = Exception(rawValue: rc)!
      var st: Stat? = nil
      if let ptr = pStat {
        st = ptr.pointee
      }//end if
      callback (err, st)
    }, key)
    guard r == Exception.ZOK.rawValue else {
      throw Exception(rawValue: r)!
    }//end guard
  }//end save

  /// check the node existence
  /// - parameters:
  ///   - path: String, the absolute full path of the node to access
  /// - returns:
  ///   stat, the node status after saving
  /// - throws:
  ///   Exception
  @discardableResult
  public func exists(_ path: String) throws -> Stat {

    // validate the connection
    guard let h = handle else {
      throw Exception.ZCONNECTIONLOSS
    }//end guard
    var stat = Stat()
    let r = zoo_exists(h, path, 0, &stat)
    guard r == Exception.ZOK.rawValue else {
      throw Exception(rawValue: r)!
    }//end guard
    return stat
  }//end func

  /// list all children under a node
  /// - parameters:
  ///   - path: String, the absolute full path of the node to access
  /// - returns:
  ///   a string array with each element as a child
  /// - throws:
  ///   Exception
  @discardableResult
  public func children(_ path: String) throws -> [String] {

    // validate the connection
    guard let h = handle else {
      throw Exception.ZCONNECTIONLOSS
    }//end guard

    // prepare an empty string array
    var array = [String]()

    // prepare the pointer array to store the children
    var sv = String_vector()

    // preform calling
    let r = zoo_get_children(h, path, 0, &sv)
    guard r == Exception.ZOK.rawValue else {
      throw Exception(rawValue: r)!
    }//end guard
    if sv.count < 1 {
      return array
    }//end if

    // save the pointer into result set
    for i in 0 ... Int(sv.count - 1) {
      guard let cstr = sv.data.advanced(by: i).pointee else {
        continue
      }//end cstr
      guard let str = String(validatingUTF8: cstr) else {
        continue
      }//str
      array.append(str)
    }//next i
    return array
  }//end public

  /// make a node, synchronously
  /// - parameters:
  ///   - path: String, the absolute full path of the node to make
  ///   - value: String, the value to store into node
  ///   - type: NodeType, i.e., persistent, ephemeral, sequential, or leadership, which means ephemeral + sequential. Default is .PERSISTENT
  ///   - acl: ACLTemplate, i.e., open, read or creator. Default is .OPEN
  /// - returns:
  ///   a string array with each element as a child
  /// - throws:
  ///   Exception
  @discardableResult
  public func make(_ path: String, value: String = "", type: NodeType = .PERSISTENT, acl: ACLTemplate = .OPEN) throws -> String {

    // validate the connection
    guard let h = handle else {
      throw Exception.ZCONNECTIONLOSS
    }//end guard

    var flags: Int32 = 0
    switch type {
    case .EPHEMERAL : flags = ZOO_EPHEMERAL
    case .SEQUENTIAL: flags = ZOO_SEQUENCE
    case .LEADERSHIP: flags = ZOO_EPHEMERAL | ZOO_SEQUENCE
    default:
        flags = 0
    }//end switch

    var aclTemp : ACL_vector
    switch(acl) {
    case .READ: aclTemp = ZOO_READ_ACL_UNSAFE
    case .OPEN: aclTemp = ZOO_OPEN_ACL_UNSAFE
    default: aclTemp = ZOO_CREATOR_ALL_ACL
    }//end switch

    let sz = Int(strlen(path) * 2)
    let buf = UnsafeMutablePointer<CChar>.allocate(capacity: sz)
    memset(buf, 0, sz)

    let r = zoo_create(h, path, value, Int32(strlen(value)), &aclTemp, flags, buf, Int32(sz))

    guard r == Exception.ZOK.rawValue else {
      buf.deallocate(capacity: sz)
      throw Exception(rawValue: r)!
    }//end guard

    let retPath = String(cString: buf)
    buf.deallocate(capacity: sz)
    return retPath
  }//end make

  /// remove a node, synchronously
  /// - parameters:
  ///   - path: String, the absolute full path of the node to make
  ///   - version: Int32, the expected version of the node. The function will fail if the actual version of the node does not match the expected version. defaut -1 is used the version check will not take place.
  /// - returns:
  ///   a string array with each element as a child
  /// - throws:
  ///   Exception
  public func remove(_ path: String, version: Int32 = -1) throws {

    // validate the connection
    guard let h = handle else {
      throw Exception.ZCONNECTIONLOSS
    }//end guard

    let r = zoo_delete(h, path, version)

    guard r == Exception.ZOK.rawValue else {
      throw Exception(rawValue: r)!
    }//end guard
  }//end remove

}//end class
