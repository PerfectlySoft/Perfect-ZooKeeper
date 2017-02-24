//
//  wachers.swift
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

/// for connection only, check ZooKeeper document for these parameters.
let globalDefaultWatcher: watcher_fn = { zooHandle, watcherType, state, watchPath, context in
  // print("-------------------  watch now ----------------------")
  // print("handle \(zooHandle)\ntype \(watcherType)")
  // print("state \(state)\npath \(watchPath)\ncontext \(context)")
  guard let ptr = context else {
    // print("something wrong, must log")
    return
  }//end guard
  let zk = Manager.mutables[ptr] as! ZooKeeper

  switch (watcherType) {
  case ZOO_SESSION_EVENT:
    if(state == ZOO_CONNECTED_STATE) {
      zk.onConnect(.CONNECTED)
      // print("connected")
  	}else if(state == ZOO_EXPIRED_SESSION_STATE) {
      zk.onConnect(.EXPIRED)
      // print("session expired")
  	}else{
      zk.onConnect(.DISCONNECTED)
      // print("connection loss")
  	}//end if
  case ZOO_CREATED_EVENT:
    // guard let path = watchPath else {
      // print("node created but path is missing???")
      // return
    // }//end guard
    //let str = String(cString: path)
    // print("node created: \(str)")
    ()
  case ZOO_DELETED_EVENT:
    // guard let path = watchPath else {
      // print("node deleted but path is missing???")
      // return
    // }//end guard
    //let str = String(cString: path)
    // print("node deleted: \(str)")
    ()
  case ZOO_CHANGED_EVENT:
    //print("touch data")
    zk.onChange(.DATA)
  case ZOO_CHILD_EVENT:
    // print("touch children")
    zk.onChange(.CHILDREN)
  default:
    //print("unexpected event???")
    ()
  }//end swtich
}//end defaultWatcher

let globalNodeWatcher: watcher_fn = { zooHandle, watcherType, state, watchPath, context in
  // get the callback function pointer
  guard let ptr = context else {
    return
  }//end guard
  let w = Manager.mutables[ptr] as! ZooKeeper.Watcher
  switch (watcherType) {
  case ZOO_SESSION_EVENT:
    if(state == ZOO_CONNECTED_STATE) {
      w.onChange(ZooKeeper.Event.CONNECTED)
    }else if(state == ZOO_EXPIRED_SESSION_STATE) {
      w.onChange(ZooKeeper.Event.EXPIRED)
      return
    }else{
      w.onChange(ZooKeeper.Event.DISCONNECTED)
      return
    }//end if
  case ZOO_CREATED_EVENT:
    w.onChange(ZooKeeper.Event.CREATED)
  case ZOO_DELETED_EVENT:
    w.onChange(ZooKeeper.Event.DELETED)
    return
  case ZOO_CHANGED_EVENT:
    w.onChange(ZooKeeper.Event.DATA_CHANGED)
  case ZOO_CHILD_EVENT:
    w.onChange(ZooKeeper.Event.CHILD_CHANGED)
  default:
    w.onChange(ZooKeeper.Event.UNKNOWN)
    return
  }//end swtich
  let watchForData = w.eventType == .BOTH || w.eventType == .DATA
  let watchForKids = w.eventType == .BOTH || w.eventType == .CHILDREN
  if watchForData && w.renew {
    let _ = zoo_awget(zooHandle, w.path, w.api, context,  { _, _, _, _, _ in }, nil)
  }//end if
  if watchForKids && w.renew {
    let _ = zoo_awget_children(zooHandle, w.path, w.api, context, { _, _, _ in }, nil)
  }//end if
}//end function
