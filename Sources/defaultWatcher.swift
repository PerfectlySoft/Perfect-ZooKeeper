import LinuxBridge
import czookeeper

let globalDefaultWatcher: watcher_fn = { zooHandle, watcherType, state, watchPath, context in
  print("-------------------  watch now ----------------------")
  print("handle \(zooHandle)\ntype \(watcherType)")
  print("state \(state)\npath \(watchPath)\ncontext \(context)")
  guard let ptr = context else {
    print("something wrong, must log")
    return
  }//end guard
  guard let zk = Manager.pool[ptr] else {
    print("Unresolved keeper address.")
    return
  }//end guard

  switch (watcherType) {
  case ZOO_SESSION_EVENT:
    if(state == ZOO_CONNECTED_STATE) {
      zk.onConnection(.CONNECTED)
      print("connected")
  	}else if(state == ZOO_EXPIRED_SESSION_STATE) {
      zk.onConnection(.EXPIRED)
      print("session expired")
  	}else{
      zk.onConnection(.DISCONNECTED)
      print("connection loss")
  	}//end if
  case ZOO_CREATED_EVENT:
    guard let path = watchPath else {
      print("node created but path is missing???")
      return
    }//end guard
    let str = String(cString: path)
    print("node created: \(str)")
  case ZOO_DELETED_EVENT:
    guard let path = watchPath else {
      print("node deleted but path is missing???")
      return
    }//end guard
    let str = String(cString: path)
    print("node deleted: \(str)")
  case ZOO_CHANGED_EVENT:
    print("touch data")
    zk.touch(true)
  case ZOO_CHILD_EVENT:
    print("touch children")
    zk.touch(false)
  default:
    print("unexpected event???")
  }//end swtich
}//end defaultWatcher
