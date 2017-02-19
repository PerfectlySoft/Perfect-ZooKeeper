public struct Manager {
  public static var pool: [UnsafeMutableRawPointer: ZooKeeper] = [:]
  public static func push(_ me: ZooKeeper) -> UnsafeMutableRawPointer {
    var this = me
    return withUnsafeMutablePointer(to: &this) { ptr -> UnsafeMutableRawPointer in
      let p = unsafeBitCast(ptr, to: UnsafeMutableRawPointer.self)
      Manager.pool[p] = me
      return p
    }//end pointer
  }//end push
}//end Manager
