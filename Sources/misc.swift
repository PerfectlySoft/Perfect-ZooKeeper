//
//  misc.swift
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
import czookeeper

extension String {
  /// parse the path and extract the parent path
  /// - returns:
  ///   parent path, *NOTE* parent of root `/` is still root `/`
  @discardableResult
  public func parentPath() -> String {

    // splite the whole path and break it into nodes.
    var nodes = self.characters.split(separator: "/").map { String($0) }

    // check if root
    if nodes.count < 1 {
      return "/"
    }//end if

    // remove the current node and return the precessors.
    nodes.remove(at: nodes.count - 1)
    if nodes.count < 1 {
      return "/"
    } else {
      return nodes.reduce("") { $0 + "/" + $1 }
    }//end if
  }//end func
}//end class
