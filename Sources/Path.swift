extension String {
  @discardableResult
  public func parentPath() -> String {
    var nodes = self.characters.split(separator: "/").map { String($0) }
    nodes.remove(at: nodes.count - 1)
    if nodes.count < 1 {
      return "/"
    } else {
      return nodes.reduce("") { $0 + "/" + $1 }
    }//end if
  }//end func
}
