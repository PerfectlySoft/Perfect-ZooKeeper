//
//  Manager.swift
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

/// pointer manager for zooKeeper
/// directly using raw pointer in zookeeper's callbacks may cause segment fault
/// so this is a save and express way to replace the directly pointer casting.
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
