public struct Manager {
  public static var mutables: [UnsafeMutableRawPointer: Any] = [:]
  public static func push(mutable: Any) -> UnsafeMutableRawPointer {
    var this = mutable
    return withUnsafeMutablePointer(to: &this) { ptr -> UnsafeMutableRawPointer in
      let p = unsafeBitCast(ptr, to: UnsafeMutableRawPointer.self)
      Manager.mutables[p] = mutable
      return p
    }//end pointer
  }//end push
  public static var immutables: [UnsafeRawPointer: Any] = [:]
  public static func push(immutable: Any) -> UnsafeRawPointer {
    var this = immutable
    return withUnsafePointer(to: &this) { ptr -> UnsafeRawPointer in
      let p = unsafeBitCast(ptr, to: UnsafeRawPointer.self)
      Manager.immutables[p] = immutable
      return p
    }//end pointer
  }//end push
}//end Manager
