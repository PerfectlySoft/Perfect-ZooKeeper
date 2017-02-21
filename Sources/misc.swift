import czookeeper

extension String {
  // root's parent is always "/"
  @discardableResult
  public func parentPath() -> String {
    var nodes = self.characters.split(separator: "/").map { String($0) }
    if nodes.count < 1 {
      return "/"
    }//end if
    nodes.remove(at: nodes.count - 1)
    if nodes.count < 1 {
      return "/"
    } else {
      return nodes.reduce("") { $0 + "/" + $1 }
    }//end if
  }//end func

  init(zkcode: ZOO_ERRORS){
    switch(zkcode) {
    case ZOK: self = "Everything is OK";
    case ZSYSTEMERROR: self = "System error"
    case ZRUNTIMEINCONSISTENCY: self = "A runtime inconsistency was found"
    case ZDATAINCONSISTENCY: self = "A data inconsistency was found"
    case ZCONNECTIONLOSS: self = "Connection to the server has been lost"
    case ZMARSHALLINGERROR: self = "Error while marshalling or unmarshalling data"
    case ZUNIMPLEMENTED: self = "Operation is unimplemented"
    case ZOPERATIONTIMEOUT: self = "Operation timeout"
    case ZBADARGUMENTS: self = "Invalid arguments"
    case ZINVALIDSTATE: self = "Invalid zhandle state"
    case ZAPIERROR: self = "Api error"
    case ZNONODE: self = "Node does not exist"
    case ZNOAUTH: self = "Not authenticated"
    case ZBADVERSION: self = "Version conflict"
    case ZNOCHILDRENFOREPHEMERALS: self = "Ephemeral nodes may not have children"
    case ZNODEEXISTS: self = "The node already exists"
    case ZNOTEMPTY: self = "The node has children"
    case ZSESSIONEXPIRED: self = "The session has been expired by the server"
    case ZINVALIDCALLBACK: self = "Invalid callback specified"
    case ZINVALIDACL: self = "Invalid ACL specified"
    case ZAUTHFAILED: self = "Client authentication failed"
    case ZCLOSING: self = "ZooKeeper is closing"
    case ZNOTHING: self = "(not error) no server responses to process"
    case ZSESSIONMOVED: self = "Session moved to another server, so operation is ignored"
    default: self = "unknown error"
    }//end case
  }//end func
}//end class
