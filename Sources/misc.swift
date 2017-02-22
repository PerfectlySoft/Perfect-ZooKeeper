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
}//end class
