import LinuxBridge
import czookeeper

public class ZooKeeper {

  private let ZK_BUFSIZE = 10240

  public enum Exception: Error {
  case CONNECTION_LOSS, DATA_INCONSISTENT, NO_NODE, NO_AUTH, BAD_VERSION,
    NO_CHILDREN, NODE_EXISTS, INVALID_ACL, MARSHALLING, AUTH_FAILED,
    SESSION_EXPIRED, INVALID_CALLBACK, TIMEOUT, INTERRUPTED, UNKNOWN,
    INVALID_FLAG, FAULT(String)
  }//end Exception

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
    let ticket = Manager.push(self)
    var id = clientid_t()
    guard let _handle = zookeeper_init(connectionString, globalDefaultWatcher, _timeout, &id, ticket, 0) else {
      throw Exception.CONNECTION_LOSS
    }//END guard
    handle = _handle
  }//nect connect

  deinit {
    if handle != nil { zookeeper_close(handle!) }
  }//end destruction

  public func load(_ path: String) throws -> (String, Stat) {
    guard let h = handle else {
      throw Exception.CONNECTION_LOSS
    }//end guard
    var size = Int32(ZK_BUFSIZE)
    var stat = Stat()
    let buf = UnsafeMutablePointer<Int8>.allocate(capacity: ZK_BUFSIZE)
    let r = ZOO_ERRORS(zoo_get(h, path, 0, buf, &size, &stat))
    guard r == ZOK else {
      buf.deallocate(capacity: ZK_BUFSIZE)
      throw Exception.FAULT(String(zkcode: r))
    }//end guard
    let data = String(cString: buf)
    buf.deallocate(capacity: ZK_BUFSIZE)
    return (data, stat)
  }//end read

  public func save(_ path: String, data: String, version: Int = -1) throws {
    guard let h = handle else {
      throw Exception.CONNECTION_LOSS
    }//end guard
    let r = ZOO_ERRORS(zoo_set(h, path, data, Int32(strlen(data)), Int32(version)))
    guard r == ZOK else {
      throw Exception.FAULT(String(zkcode: r))
    }//end guard
  }

  public func exists(_ path: String) throws -> Bool {
    guard let h = handle else {
      throw Exception.CONNECTION_LOSS
    }//end guard
    let r = ZOO_ERRORS(zoo_exists(h, path, 0, nil))
    return r == ZOK
  }//end func

  public func children(_ path: String) throws -> [String] {
    guard let h = handle else {
      throw Exception.CONNECTION_LOSS
    }//end guard
    var array = [String]()
    let acl = ZOO_OPEN_ACL_UNSAFE
    print(acl.count )
    let a = acl.data.advanced(by: 0).pointee
    print(a)
    var sv = String_vector()
    let r = ZOO_ERRORS(zoo_get_children(h, path, 0, &sv))
    guard r == ZOK else {
      throw Exception.FAULT(String(zkcode: r))
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
