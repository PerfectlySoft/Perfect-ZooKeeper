#if os(Linux)
import SwiftGlibc
#else
import Darwin
#endif
import czookeeper

public typealias StatusCallback = (ZooKeeper.Exception, Stat?) -> Void
public typealias DataCallback =  (ZooKeeper.Exception, String, Stat?) -> Void

public class ZooKeeper {

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

  public enum ConnectionState {
  case CONNECTED, DISCONNECTED, EXPIRED
  }//end ConnectionState

  internal var handle: OpaquePointer? = nil
  internal var connectionString = ""
  internal var _timeout:Int32 = 0

  public var onConnection: (ConnectionState)->Void = { _ in }
  public func touch(_ forData: Bool) { }

  init(host: String = "localhost", port: Int = 2181, timeout: Int32 = 10000) {
    connectionString = "\(host):\(port)"
    _timeout = timeout
  }//init

  public func connect(completion: @escaping (ConnectionState) -> Void ) throws {
    if handle != nil { zookeeper_close(handle!) }
    onConnection = completion
    let ticket = Manager.push(mutable: self)
    var id = clientid_t()
    guard let _handle = zookeeper_init(connectionString, globalDefaultWatcher, _timeout, &id, ticket, 0) else {
      throw Exception.ZCONNECTIONLOSS
    }//END guard
    handle = _handle
  }//nect connect

  deinit {
    if handle != nil { zookeeper_close(handle!) }
  }//end destruction

  @discardableResult
  public func load(_ path: String) throws -> (String, Stat) {
    guard let h = handle else {
      throw Exception.ZCONNECTIONLOSS
    }//end guard
    var size = Int32(ZK_BUFSIZE)
    var stat = Stat()
    let buf = UnsafeMutablePointer<Int8>.allocate(capacity: ZK_BUFSIZE)
    let r = zoo_get(h, path, 0, buf, &size, &stat)
    guard r == Exception.ZOK.rawValue else {
      buf.deallocate(capacity: ZK_BUFSIZE)
      throw Exception(rawValue: r)!
    }//end guard
    let data = String(cString: buf)
    buf.deallocate(capacity: ZK_BUFSIZE)
    return (data, stat)
  }//end read

  public func load(_ path: String, completion: @escaping DataCallback ) {
    guard let h = handle else {
      completion (Exception.ZCONNECTIONLOSS, "", Stat())
      return
    }//end guard
    let key = Manager.push(immutable: completion)
    zoo_aget(h, path, 0, { rc, value, len, pStat, data  in
      guard let ptr = data else {
        return
      }//end guard
      let callback = Manager.immutables[ptr] as! DataCallback
      let err = Exception(rawValue: rc)!
      var st: Stat? = nil
      if let ptr = pStat {
        st = ptr.pointee
      }//end if
      if len > 0 && value != nil {
        let sz = Int(len)
        let val = UnsafeMutablePointer<CChar>.allocate(capacity: sz + 1)
        memset(val, 0, sz + 1)
        memcpy(val, value!, sz)
        let str = String(cString: val)
        val.deallocate(capacity: sz)
        print(str)
        callback(err, str, st)
      } else {
        callback(err, "", st)
      }//end if
    }, key)
  }//end load

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

  public func save(_ path: String, data: String, version: Int = -1, completion: @escaping StatusCallback ) {
    guard let h = handle else {
      completion (Exception.ZCONNECTIONLOSS, Stat())
      return
    }//end guard
    let key = Manager.push(immutable: completion)
    zoo_aset(h, path, data, Int32(strlen(data)), Int32(version), { rc, pStat, data in
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

  @discardableResult
  public func exists(_ path: String) throws -> Stat {
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

  @discardableResult
  public func children(_ path: String) throws -> [String] {
    guard let h = handle else {
      throw Exception.ZCONNECTIONLOSS
    }//end guard
    var array = [String]()
    var sv = String_vector()
    let r = zoo_get_children(h, path, 0, &sv)
    guard r == Exception.ZOK.rawValue else {
      throw Exception(rawValue: r)!
    }//end guard
    if sv.count < 1 {
      return array
    }//end if
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
