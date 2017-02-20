import LinuxBridge
import czookeeper

public class ZooKeeper {

  private let ZK_BUFSIZE = 16384

  public enum Exception: Error {
  case CONNECTION_LOSS, DATA_INCONSISTENT, NO_NODE, NO_AUTH, BAD_VERSION,
    NO_CHILDREN, NODE_EXISTS, INVALID_ACL, MARSHALLING, AUTH_FAILED,
    SESSION_EXPIRED, INVALID_CALLBACK, TIMEOUT, INTERRUPTED, UNKNOWN,
    FAULT(String)
  }//end Exception

  public enum ConnectionState {
  case CONNECTED, DISCONNECTED, EXPIRED
  }//end ConnectionState

  public class Ret {
    private var _code = ZOK
    init(_ code: ZOO_ERRORS = ZOK) { _code = code }
    public var ok: Bool { get { return _code == ZOK } }
    public var present: Bool { get { return _code == ZNODEEXISTS } }
    public var absent: Bool { get { return _code == ZNONODE } }
    public var info: String { get { return Ret.explain(_code) } }
    @discardableResult
    public static func explain(_ code: ZOO_ERRORS) -> String {
      switch(code) {
    	case ZOK: return "Everything is OK";
    	case ZSYSTEMERROR: return "System error";
    	case ZRUNTIMEINCONSISTENCY: return "A runtime inconsistency was found";
    	case ZDATAINCONSISTENCY: return "A data inconsistency was found";
    	case ZCONNECTIONLOSS: return "Connection to the server has been lost";
    	case ZMARSHALLINGERROR: return "Error while marshalling or unmarshalling data";
    	case ZUNIMPLEMENTED: return "Operation is unimplemented";
    	case ZOPERATIONTIMEOUT: return "Operation timeout";
    	case ZBADARGUMENTS: return "Invalid arguments";
    	case ZINVALIDSTATE: return "Invalid zhandle state";
    	case ZAPIERROR: return "Api error";
    	case ZNONODE: return "Node does not exist";
    	case ZNOAUTH: return "Not authenticated";
    	case ZBADVERSION: return "Version conflict";
    	case ZNOCHILDRENFOREPHEMERALS: return "Ephemeral nodes may not have children";
    	case ZNODEEXISTS: return "The node already exists";
    	case ZNOTEMPTY: return "The node has children";
    	case ZSESSIONEXPIRED: return "The session has been expired by the server";
    	case ZINVALIDCALLBACK: return "Invalid callback specified";
    	case ZINVALIDACL: return "Invalid ACL specified";
    	case ZAUTHFAILED: return "Client authentication failed";
    	case ZCLOSING: return "ZooKeeper is closing";
    	case ZNOTHING: return "(not error) no server responses to process";
    	case ZSESSIONMOVED: return "Session moved to another server, so operation is ignored";
    	default: return "unknown error";
    	}//end case
    }//end func
  }//end class

  internal var handle: OpaquePointer? = nil
  internal var connectionString = ""
  internal var _timeout:Int32 = 0

  internal var connected: (ConnectionState)->Void = { _ in }
  public func touch(_ forData: Bool) { }

  init(host: String = "localhost", port: Int = 2181, timeout: Int32 = 10000) {
    connectionString = "\(host):\(port)"
    _timeout = timeout
  }//init

  public func setConnection(_ state: ConnectionState) {
    connected(state)
  }//end func

  public func connect(completion: @escaping (ConnectionState) -> Void ) throws {
    if handle != nil { zookeeper_close(handle!) }
    connected = completion
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
      throw Exception.FAULT(Ret.explain(r))
    }//end guard
    let data = String(cString: buf)
    buf.deallocate(capacity: ZK_BUFSIZE)
    return (data, stat)
  }//end read
}//end class
