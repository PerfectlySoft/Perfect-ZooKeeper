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

  /// zookeeper handle, could be nil if no connection available
  internal var handle: OpaquePointer? = nil

  /// connection time out value, in milliseconds
  internal var _timeout:Int32 = 0

  /// client id
  internal var id = clientid_t()

  /// connection event callback, default is `do nothing`
  public var onConnect: (ConnectionState)->Void = { _ in }

  ///
  public func touch(_ forData: Bool) { }

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

  /// load data from a node, synchronously
  /// - parameters:
  ///   - path: String, the absolute full path of the node to access
  /// - returns:
  ///   (value: String, stat: Stat), a tuple of data value and its directory status.
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
  public func load(_ path: String, completion: @escaping DataCallback ) {

    // validate the handle first
    guard let h = handle else {
      completion (Exception.ZCONNECTIONLOSS, "", Stat())
      return
    }//end guard

    // deposit the function pointer to the pointer mananger
    let key = Manager.push(immutable: completion)

    // asynchronously read data from server
    zoo_aget(h, path, 0, { rc, value, len, pStat, data  in

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
  }//end load

  /// save data to path, synchronously
  /// - parameters:
  ///   - path: String, the absolute full path of the node to access
  ///   - data: String, the data to save
  ///   - version: version of data, default is -1 which indicates ignoring the version info
  /// - returns:
  ///   stat, the node status after saving
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
  public func save(_ path: String, data: String, version: Int = -1, completion: @escaping StatusCallback ) {

    // validate the connection
    guard let h = handle else {
      completion (Exception.ZCONNECTIONLOSS, Stat())
      return
    }//end guard

    // save the callback function pointer to pool
    let key = Manager.push(immutable: completion)

    // deposit data into node
    zoo_aset(h, path, data, Int32(strlen(data)), Int32(version), { rc, pStat, data in

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
  }//end save

  /// check the node existence
  /// - parameters:
  ///   - path: String, the absolute full path of the node to access
  /// - returns:
  ///   stat, the node status after saving
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


/*
  internal func _createNode(flag: Int32, path: String, value: String, recursive: Bool) throws -> String {
    guard flag != ZOO_SEQUENCE else {
      throw Exception.INVALID_FLAG
    }//end guard
    var size = Int32(ZK_BUFSIZE)
    let buf = UnsafeMutablePointer<Int8>.allocate(capacity: ZK_BUFSIZE)
    var acl = ZOO_OPEN_ACL_UNSAFE
    let r = ZOO_ERRORS(zoo_create(h, path, value, value.utf8.count, &ac, flag, buf, size))
    guard r == ZOK else {
      throw Exception.FAULT(String(zkcode: r))
    }//end guard
    let parent = path.parentPath()
    if parent == "/" {
      return
    }
  }
  */
}//end class
